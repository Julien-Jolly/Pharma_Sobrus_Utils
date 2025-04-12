import sys
import os
import logging
import time
import re
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, WebDriverException, StaleElementReferenceException, \
    ElementClickInterceptedException
from core.scraper import PharmaScraper
from database.db_manager import DBManager
from core.s3_utils import upload_to_s3, verify_s3_upload

# Configuration des logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("client_keys.log", mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_current_page_number(scraper, timeout=5, is_initial_page=False):
    """Récupère le numéro de la page actuelle avec un timeout réduit."""
    try:
        pagination = WebDriverWait(scraper.driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.sob-v2-table-pagination")),
            message="Pagination non trouvée"
        )
        current_page_element = pagination.find_element(By.CSS_SELECTOR, "span.sob-v2-TablePage")
        WebDriverWait(scraper.driver, timeout).until(
            lambda d: current_page_element.text.strip() != "",
            message="Texte de la page non chargé"
        )
        page_text = current_page_element.text.strip()
        logger.debug(f"Texte brut de la page : '{page_text}'")
        match = re.search(r'\d+', page_text)
        if match:
            page_number = int(match.group(0))
            logger.info(f"Page actuelle détectée : {page_number}")
            return page_number
        else:
            logger.warning(f"Texte de page non numérique : '{page_text}'")
            if is_initial_page:
                logger.info("Numéro de page non numérique, supposition page 1")
                return 1
            return None
    except (TimeoutException, StaleElementReferenceException, WebDriverException) as e:
        logger.warning(f"Impossible de récupérer le numéro de page : {str(e)}")
        try:
            dom_snippet = scraper.driver.execute_script(
                "return document.querySelector('div.sob-v2-table-pagination')?.outerHTML || 'Pagination absente';"
            )
            logger.debug(f"DOM de la pagination : {dom_snippet}")
        except:
            logger.debug("Impossible de récupérer le DOM de la pagination")
        if is_initial_page:
            logger.info("Échec de détection, supposition page 1")
            return 1
        return None

def extract_client_key(scraper, client_name, expected_page, cached_page=None, max_retries=3):
    """Extrait la clé d'un client en cliquant sur son lien."""
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Tentative {attempt} pour {client_name} (page {expected_page})")
            WebDriverWait(scraper.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.sob-v2-table")),
                message="Tableau des clients non chargé"
            )

            current_page = cached_page if cached_page is not None else get_current_page_number(scraper, timeout=5)
            if current_page is None and expected_page == 1:
                logger.info("Numéro de page non détecté pour page 1, continuation assumée")
            elif current_page != expected_page:
                logger.error(f"Page incorrecte : attendu {expected_page}, obtenu {current_page}")
                return None

            name_escaped = client_name.replace("'", "\\'").replace('"', '\\"')
            client_xpath = f'//table[contains(@class, "sob-v2-table")]//tbody/tr[th/span[normalize-space()="{name_escaped}"]]'
            client_row = WebDriverWait(scraper.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, client_xpath)),
                message=f"Client {client_name} non trouvé"
            )
            scraper.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", client_row)

            client_row.click()
            WebDriverWait(scraper.driver, 10).until(
                lambda d: "/customer/" in d.current_url,
                message="Redirection vers la page client échouée"
            )

            client_key_match = re.search(r"/customer/(\d+)/", scraper.driver.current_url)
            if not client_key_match:
                raise ValueError(f"Clé non trouvée pour {client_name}")
            client_key = client_key_match.group(1)
            logger.info(f"Clé récupérée pour {client_name}: {client_key} (page {expected_page})")

            scraper.driver.back()
            WebDriverWait(scraper.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.sob-v2-table")),
                message="Retour à la page clients échoué"
            )
            return client_key

        except (StaleElementReferenceException, ElementClickInterceptedException, TimeoutException) as e:
            logger.warning(f"Erreur pour {client_name} (tentative {attempt}/{max_retries}): {str(e)}")
            if attempt == max_retries:
                logger.error(f"Échec définitif pour {client_name} après {max_retries} tentatives")
                return None
            time.sleep(1)
        except Exception as e:
            logger.error(f"Erreur inattendue pour {client_name}: {str(e)}")
            return None

def go_to_next_page(scraper, current_page):
    """Passe à la page suivante et vérifie que la nouvelle page est chargée."""
    logger.info(f"Tentative de passage de la page {current_page} à {current_page + 1}")
    try:
        # Attendre la pagination
        pagination = WebDriverWait(scraper.driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.sob-v2-table-pagination")),
            message="Pagination non trouvée"
        )
        # Loguer tous les boutons pour diagnostic
        buttons = pagination.find_elements(By.TAG_NAME, "button")
        for i, btn in enumerate(buttons):
            btn_class = btn.get_attribute("class") or ""
            btn_text = btn.text.strip() or "Aucun texte"
            btn_disabled = btn.get_attribute("disabled") or "false"
            btn_html = scraper.driver.execute_script("return arguments[0].outerHTML;", btn)
            logger.debug(f"Bouton {i+1}: class='{btn_class}', texte='{btn_text}', disabled={btn_disabled}, html={btn_html}")

        # Trouver le numéro de page
        page_span = pagination.find_element(By.CSS_SELECTOR, "span.sob-v2-TablePage")
        # Sélectionner le bouton "suivant" (à droite du numéro de page)
        next_button = page_span.find_element(
            By.XPATH, "./following-sibling::button[1]"
        )
        btn_class = next_button.get_attribute("class") or ""
        btn_disabled = next_button.get_attribute("disabled")
        btn_html = scraper.driver.execute_script("return arguments[0].outerHTML;", next_button)
        logger.info(f"Bouton suivant détecté: class='{btn_class}', disabled={btn_disabled}, html={btn_html}")

        # Vérifier si le bouton est désactivé
        if btn_disabled == "true" or "sob-v2-TablePage__disabled" in btn_class:
            logger.info("Bouton suivant désactivé, dernière page atteinte")
            return False

        # Faire défiler jusqu'au bouton
        scraper.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)

        # Vérifier que le bouton est cliquable
        WebDriverWait(scraper.driver, 5).until(
            EC.element_to_be_clickable(next_button),
            message="Bouton suivant non cliquable"
        )

        # Cliquer via JavaScript
        scraper.driver.execute_script("arguments[0].click();", next_button)

        # Attendre que la nouvelle page soit chargée
        WebDriverWait(scraper.driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table.sob-v2-table")),
            message="Tableau des clients non chargé après changement de page"
        )
        new_page = get_current_page_number(scraper, timeout=5)
        if new_page != current_page + 1:
            logger.warning(f"Page incorrecte après clic : attendu {current_page + 1}, obtenu {new_page}")
            return False
        logger.info(f"Passage réussi à la page {current_page + 1}")
        return True

    except TimeoutException as e:
        logger.warning(f"Échec du passage à la page suivante (Timeout) : {str(e)}")
        return False
    except StaleElementReferenceException as e:
        logger.warning(f"Échec du passage à la page suivante (StaleElement) : {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Erreur inattendue lors du passage à la page suivante : {str(e)}")
        return False

def run(login, password, db_path, scraper=None):
    """Récupère les noms et clés des clients avec un seul navigateur."""
    if scraper is None:
        raise ValueError("Une instance de PharmaScraper doit être fournie")

    logger.info("Démarrage de la récupération des clés clients")
    scraper.access_site("https://app.pharma.sobrus.com/", login, password, force_auth=True)

    # Initialiser la base de données
    db = DBManager(db_path)
    with db.connect() as conn:
        conn.execute("DELETE FROM client_keys")
        conn.commit()

    # Parcourir les pages
    page = 1
    seen_clients = set()  # Pour éviter les duplications
    total_clients = 0  # Compteur pour suivi
    while True:
        logger.info(f"Collecte page {page}")
        try:
            # Charger la page customers
            scraper.driver.get("https://app.pharma.sobrus.com/customers")
            WebDriverWait(scraper.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.sob-v2-table")),
                message="Tableau des clients non chargé"
            )

            # Vérifier la page
            current_page = get_current_page_number(scraper, timeout=5, is_initial_page=(page == 1))
            if current_page is None and page == 1:
                logger.info("Numéro de page non détecté pour page 1, continuation assumée")
                current_page = 1
            elif current_page != page:
                logger.error(f"Page incorrecte après chargement : attendu {page}, obtenu {current_page}")
                break

            # Extraire les noms des clients
            clients = scraper.get_clients_from_page()
            logger.info(f"{len(clients)} clients extraits de la page {page}")
            total_clients += len(clients)
            logger.info(f"Total clients extraits jusqu'à présent : {total_clients}")

            # Récupérer les clés pour chaque client
            for client in clients:
                client_name = client["nom"]
                if client_name in seen_clients:
                    logger.warning(f"Client {client_name} déjà traité, ignoré")
                    continue
                seen_clients.add(client_name)
                client_key = extract_client_key(scraper, client_name, page, cached_page=current_page)
                if client_key:
                    with db.connect() as conn:
                        conn.execute("INSERT OR REPLACE INTO client_keys (nom, client_key) VALUES (?, ?)",
                                     (client_name, client_key))
                        conn.commit()
                    logger.info(f"Clé sauvegardée pour {client_name}: {client_key} (page {page})")

            # Passer à la page suivante
            scraper.ensure_session()
            if not go_to_next_page(scraper, page):
                logger.info("Dernière page atteinte")
                break
            page += 1

        except Exception as e:
            logger.error(f"Erreur lors du traitement de la page {page}: {str(e)}")
            break

    # Finalisation
    upload_to_s3(db_path)
    verify_s3_upload(s3_file=os.path.basename(db_path))
    scraper.cleanup()
    logger.info(f"Processus terminé avec {total_clients} clients extraits")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python client_keys.py <login> <password> <db_path>")
        sys.exit(1)
    login, password, db_path = sys.argv[1:4]
    scraper = PharmaScraper(login=login, password=password)
    run(login, password, db_path, scraper)