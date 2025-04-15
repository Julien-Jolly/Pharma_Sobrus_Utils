import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging
import time
import re
import tempfile
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, WebDriverException, \
    NoSuchElementException, ElementClickInterceptedException
import shutil
from core.scraper import PharmaScraper
from database.db_manager import DBManager
from core.s3_utils import upload_to_s3, verify_s3_upload

# Configuration des logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("client_keys.log", mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
NUM_WORKERS = 3  # Nombre de workers (modifiable ici)
BASE_PORT = 9222  # Port de départ pour les instances Chrome

def create_scraper(login, password, port, download_dir):
    """Crée une instance PharmaScraper avec un port et un profil uniques."""
    process_name = multiprocessing.current_process().name
    unique_suffix = f"_port{port}_{int(time.time())}"
    user_data_dir = tempfile.mkdtemp(suffix=unique_suffix)
    try:
        options = [
            f"--remote-debugging-port={port}",
            f"--user-data-dir={user_data_dir}",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ]
        scraper = PharmaScraper(login=login, password=password, download_dir=download_dir)
        for opt in options:
            scraper.driver.command_executor._commands["send_command"] = (
                "POST",
                "/session/$sessionId/chromium/send_command"
            )
            scraper.driver.execute("send_command", {
                "cmd": "Page.addScriptToEvaluateOnNewDocument",
                "params": {"source": opt}
            })
        logger.info(f"[{process_name}] Scraper créé avec user_data_dir={user_data_dir}")
        return scraper, user_data_dir
    except Exception as e:
        logger.error(f"[{process_name}] Erreur création scraper port {port}: {str(e)}")
        shutil.rmtree(user_data_dir, ignore_errors=True)
        raise

def navigate_to_page(scraper, target_page, current_page=1, max_retries=3):
    """Navigue vers la page cible en cliquant sur 'Suivant' ou 'Précédent' avec vérification."""
    process_name = multiprocessing.current_process().name
    if target_page < 1:
        logger.error(f"[{process_name}] Page cible invalide : {target_page}")
        return False
    if target_page == current_page:
        logger.info(f"[{process_name}] Déjà sur la page {current_page}")
        return True

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"[{process_name}] Navigation vers page {target_page} depuis {current_page} (tentative {attempt})")

            # Charger la page des clients si nécessaire
            if current_page == 1 and "customers" not in scraper.driver.current_url:
                logger.info(f"[{process_name}] Chargement initial de la page clients")
                scraper.driver.get("https://app.pharma.sobrus.com/customers")
                WebDriverWait(scraper.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.sob-v2-table-pagination")),
                    message="Pagination non trouvée"
                )
                try:
                    page_element = scraper.driver.find_element(By.CSS_SELECTOR, "span.sob-v2-TablePage")
                    page_text = page_element.text.strip()
                    if page_text.isdigit():
                        current_page = int(page_text)
                        logger.info(f"[{process_name}] Page initiale confirmée : {current_page}")
                    else:
                        logger.warning(f"[{process_name}] Numéro de page initial non numérique: {page_text}")
                except (NoSuchElementException, ValueError):
                    logger.warning(f"[{process_name}] Impossible de vérifier la page initiale, assume {current_page}")

            # Boucle pour atteindre la page cible
            while current_page != target_page:
                logger.info(f"[{process_name}] Tentative d'atteindre page {target_page} depuis {current_page}")
                WebDriverWait(scraper.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.sob-v2-table-pagination")),
                    message="Pagination non trouvée"
                )

                # Avancer ou reculer selon la position
                if current_page < target_page:
                    if not scraper.go_to_next_page():
                        logger.info(f"[{process_name}] Impossible d'avancer à la page {current_page + 1}")
                        try:
                            next_button = scraper.driver.find_element(By.CSS_SELECTOR, "button.sob-v2-TablePage__btn:last-child")
                            if "sob-v2-TablePage__disabled" in next_button.get_attribute("class"):
                                logger.info(f"[{process_name}] Bouton 'Suivant' désactivé, dernière page atteinte")
                                return False
                        except NoSuchElementException:
                            logger.error(f"[{process_name}] Bouton 'Suivant' non trouvé")
                            return False
                        return False
                elif current_page > target_page:
                    logger.warning(f"[{process_name}] Overshoot : sur page {current_page}, cible {target_page}")
                    # Tenter de revenir en arrière
                    for retry in range(3):
                        try:
                            prev_button = scraper.driver.find_element(By.CSS_SELECTOR, "button.sob-v2-TablePage__btn:first-child")
                            if "sob-v2-TablePage__disabled" not in prev_button.get_attribute("class"):
                                logger.info(f"[{process_name}] Clic sur 'Précédent' pour corriger")
                                scraper.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", prev_button)
                                time.sleep(1)
                                prev_button.click()
                                WebDriverWait(scraper.driver, 15).until(
                                    EC.presence_of_element_located((By.CSS_SELECTOR, "table.sob-v2-table")),
                                    message="Tableau non chargé après clic 'Précédent'"
                                )
                                break
                            else:
                                logger.warning(f"[{process_name}] Bouton 'Précédent' désactivé")
                                return False
                        except (NoSuchElementException, TimeoutException, ElementClickInterceptedException):
                            logger.warning(f"[{process_name}] Échec clic 'Précédent', tentative {retry + 1}/3")
                            if retry == 2:
                                logger.error(f"[{process_name}] Échec correction overshoot")
                                return False
                            time.sleep(1)

                # Vérifier le numéro de page
                for retry in range(3):
                    try:
                        page_element = scraper.driver.find_element(By.CSS_SELECTOR, "span.sob-v2-TablePage")
                        new_page_text = page_element.text.strip()
                        logger.info(f"[{process_name}] Numéro de page lu : '{new_page_text}'")
                        if not new_page_text.isdigit():
                            logger.warning(f"[{process_name}] Numéro de page non numérique, tentative {retry + 1}/3")
                            time.sleep(1)
                            continue
                        current_page = int(new_page_text)
                        logger.info(f"[{process_name}] Atteint page {current_page}")
                        break
                    except (NoSuchElementException, StaleElementReferenceException):
                        logger.warning(f"[{process_name}] Échec lecture page, tentative {retry + 1}/3")
                        time.sleep(1)
                        if retry == 2:
                            logger.error(f"[{process_name}] Impossible de vérifier la page")
                            return False

            # Vérification finale
            try:
                page_element = scraper.driver.find_element(By.CSS_SELECTOR, "span.sob-v2-TablePage")
                final_page_text = page_element.text.strip()
                if not final_page_text.isdigit():
                    logger.warning(f"[{process_name}] Numéro de page final non numérique: {final_page_text}")
                    return False
                final_page = int(final_page_text)
                logger.info(f"[{process_name}] Vérification finale : page {final_page}")
                if final_page != target_page:
                    logger.error(f"[{process_name}] Page cible {target_page} non atteinte, sur page {final_page}")
                    return False
            except (NoSuchElementException, ValueError):
                logger.warning(f"[{process_name}] Impossible de vérifier la page finale")
                return False

            logger.info(f"[{process_name}] Navigation réussie vers page {target_page}")
            return True

        except (WebDriverException, TimeoutException) as e:
            logger.error(f"[{process_name}] Erreur navigation page {target_page}: {str(e)}")
            if attempt < max_retries:
                logger.info(f"[{process_name}] Nouvelle tentative après 2s")
                time.sleep(2)
            else:
                logger.error(f"[{process_name}] Échec définitif après {max_retries} tentatives")
                return False

    logger.error(f"[{process_name}] Échec navigation après toutes les tentatives")
    return False

def process_page(page_number, login, password, db_path, scraper, port, download_dir, lock, processed_client_keys):
    """Traite une page spécifique avec un navigateur existant."""
    process_name = multiprocessing.current_process().name
    logger.info(f"[{process_name}] Début traitement page {page_number}")
    try:
        scraper.ensure_session()

        for attempt in range(3):
            if navigate_to_page(scraper, page_number):
                break
            logger.warning(f"[{process_name}] Échec navigation page {page_number} (tentative {attempt + 1}/3)")
            if attempt == 2:
                logger.error(f"[{process_name}] Échec définitif navigation page {page_number}")
                return 0, False
            time.sleep(2)

        WebDriverWait(scraper.driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table.sob-v2-table")),
            message=f"Tableau des clients non chargé pour page {page_number}"
        )
        clients = scraper.get_clients_from_page()
        logger.info(f"[{process_name}] {len(clients)} clients extraits de la page {page_number}")

        db = DBManager(db_path)
        seen_client_keys = set()
        for client in clients:
            client_name = client["nom"]
            client_key = None
            for retry in range(3):
                try:
                    client_key = extract_client_key(scraper, client_name, page_number)
                    break
                except Exception as e:
                    logger.warning(f"[{process_name}] Échec tentative {retry + 1} pour {client_name}: {str(e)}")
                    if retry == 2:
                        logger.error(f"[{process_name}] Échec définitif pour {client_name}")
                        continue
                    time.sleep(2)
            if not client_key:
                continue

            with lock:
                if client_key in processed_client_keys:
                    logger.warning(f"[{process_name}] Clé {client_key} pour {client_name} déjà traitée globalement, ignoré")
                    continue
                processed_client_keys.append(client_key)
            if client_key in seen_client_keys:
                logger.warning(f"[{process_name}] Clé {client_key} pour {client_name} déjà traitée sur page {page_number}, ignoré")
                continue
            seen_client_keys.add(client_key)

            with db.connect() as conn:
                conn.execute("INSERT OR REPLACE INTO client_keys (nom, client_key) VALUES (?, ?)",
                             (client_name, client_key))
                conn.commit()
            logger.info(f"[{process_name}] Clé sauvegardée pour {client_name}: {client_key} (page {page_number})")

        # Vérifier si c'est la dernière page
        is_last_page = not scraper.go_to_next_page()
        if is_last_page:
            logger.info(f"[{process_name}] Dernière page détectée sur page {page_number}")
        return len(clients), is_last_page

    except WebDriverException as e:
        logger.error(f"[{process_name}] Erreur WebDriver page {page_number}: {str(e)}")
        return 0, False
    except Exception as e:
        logger.error(f"[{process_name}] Erreur page {page_number}: {str(e)}")
        return 0, False

def extract_client_key(scraper, client_name, expected_page, max_retries=3):
    """Extrait la clé d'un client en cliquant sur son lien."""
    process_name = multiprocessing.current_process().name
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"[{process_name}] Tentative {attempt} pour {client_name} (page {expected_page})")
            WebDriverWait(scraper.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.sob-v2-table")),
                message="Tableau des clients non chargé"
            )

            name_escaped = client_name.replace("'", "\\'").replace('"', '\\"')
            client_xpath = f'//table[contains(@class, "sob-v2-table")]//tbody/tr[th/span[normalize-space()="{name_escaped}"]]'
            client_row = WebDriverWait(scraper.driver, 15).until(
                EC.presence_of_element_located((By.XPATH, client_xpath)),
                message=f"Client {client_name} non trouvé"
            )
            scraper.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", client_row)
            time.sleep(0.5)
            WebDriverWait(scraper.driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, client_xpath))
            )

            try:
                client_row.click()
            except ElementClickInterceptedException:
                logger.warning(f"[{process_name}] Clic intercepté pour {client_name}, tentative avec JavaScript")
                scraper.driver.execute_script("arguments[0].click();", client_row)

            WebDriverWait(scraper.driver, 15).until(
                lambda d: "/customer/" in d.current_url,
                message="Redirection vers la page client échouée"
            )

            client_key_match = re.search(r"/customer/(\d+)/", scraper.driver.current_url)
            if not client_key_match:
                raise ValueError(f"Clé non trouvée pour {client_name}")
            client_key = client_key_match.group(1)
            logger.info(f"[{process_name}] Clé récupérée pour {client_name}: {client_key} (page {expected_page})")

            scraper.driver.back()
            WebDriverWait(scraper.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.sob-v2-table")),
                message="Retour à la page clients échoué"
            )
            return client_key

        except (StaleElementReferenceException, ElementClickInterceptedException, TimeoutException) as e:
            logger.warning(f"[{process_name}] Erreur pour {client_name} (tentative {attempt}/{max_retries}): {str(e)}")
            if attempt == max_retries:
                logger.error(f"[{process_name}] Échec définitif pour {client_name}")
                return None
            time.sleep(1)
        except WebDriverException as e:
            logger.error(f"[{process_name}] Erreur WebDriver pour {client_name}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"[{process_name}] Erreur inattendue pour {client_name}: {str(e)}")
            return None

def worker(port, login, password, db_path, lock, total_clients, download_dir, processed_client_keys, page_counter):
    """Travaille sur les pages assignées avec un seul scraper."""
    process_name = multiprocessing.current_process().name
    logger.info(f"[{process_name}] Démarrage du travailleur avec port {port}")
    scraper = None
    user_data_dir = None
    try:
        scraper, user_data_dir = create_scraper(login, password, port, download_dir)

        max_auth_retries = 3
        for attempt in range(1, max_auth_retries + 1):
            try:
                logger.info(f"[{process_name}] Authentification complète (tentative {attempt})")
                scraper.access_site("https://app.pharma.sobrus.com/", login, password, force_auth=True)
                scraper.get_cookies_for_requests()
                if scraper.is_session_active():
                    logger.info(f"[{process_name}] Authentification réussie")
                    break
                else:
                    logger.warning(f"[{process_name}] Session inactive, tentative {attempt}/{max_auth_retries}")
                    time.sleep(2)
            except Exception as e:
                logger.warning(
                    f"[{process_name}] Échec authentification (tentative {attempt}/{max_auth_retries}) : {str(e)}")
                if attempt == max_auth_retries:
                    logger.error(f"[{process_name}] Échec définitif de l'authentification")
                    return
                time.sleep(2)

        while True:
            # Obtenir la prochaine page à traiter
            with lock:
                page_number = page_counter.value
                page_counter.value += 1
            logger.info(f"[{process_name}] Tentative de traitement de la page {page_number}")

            num_clients, is_last_page = process_page(
                page_number, login, password, db_path, scraper, port, download_dir, lock, processed_client_keys
            )
            with lock:
                total_clients.value += num_clients

            if is_last_page or num_clients == 0:
                logger.info(f"[{process_name}] Arrêt sur page {page_number} : dernière page ou aucune donnée")
                break

    finally:
        if scraper:
            try:
                scraper.cleanup()
                logger.info(f"[{process_name}] Nettoyage terminé pour port {port}")
            except:
                logger.warning(f"[{process_name}] Erreur lors du nettoyage pour port {port}")
            if user_data_dir:
                shutil.rmtree(user_data_dir, ignore_errors=True)

def run_parallel(login, password, db_path, num_browsers=NUM_WORKERS):
    """Lance plusieurs navigateurs pour traiter les pages en parallèle."""
    logger.info(f"Démarrage de la récupération des clés clients avec {num_browsers} navigateurs")

    db = DBManager(db_path)
    with db.connect() as conn:
        conn.execute("DELETE FROM client_keys")
        conn.commit()

    download_dir = tempfile.mkdtemp()

    try:
        manager = multiprocessing.Manager()
        logger.info("Manager créé avec succès")
        lock = manager.Lock()
        total_clients = manager.Value('i', 0)
        processed_client_keys = manager.list()
        page_counter = manager.Value('i', 1)  # Compteur pour attribuer les pages
    except Exception as e:
        logger.error(f"Échec de l'initialisation du Manager: {str(e)}")
        raise

    ports = list(range(BASE_PORT, BASE_PORT + num_browsers))
    with ProcessPoolExecutor(max_workers=num_browsers) as executor:
        futures = []
        for port in ports:
            time.sleep(2)
            futures.append(executor.submit(
                worker, port, login, password, db_path, lock,
                total_clients, download_dir, processed_client_keys, page_counter
            ))
        for future in futures:
            try:
                future.result()
            except Exception as e:
                logger.error(f"Erreur dans un processus parallèle: {str(e)}")

    upload_to_s3(db_path)
    verify_s3_upload(s3_file=os.path.basename(db_path))
    logger.info(f"Processus terminé avec {total_clients.value} clients extraits")

def run(login, password, db_path, start_date=None, end_date=None, client_name=None, scraper=None):
    """Interface compatible avec main.py, appelle run_parallel."""
    logger.info(f"Appel de run avec login={login}, db_path={db_path}, client_name={client_name}")
    try:
        run_parallel(login, password, db_path, num_browsers=NUM_WORKERS)
    except Exception as e:
        logger.error(f"Erreur dans run_parallel: {str(e)}")
        raise

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python client_keys.py <login> <password> <db_path>")
        sys.exit(1)
    login, password, db_path = sys.argv[1:4]
    run(login, password, db_path)