import os
import re
import time
import sys
import shutil
import requests
import logging
from requests.exceptions import RequestException
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, WebDriverException
from config.config import DOWNLOAD_DIR
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("scraper.log", mode='a'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class PharmaScraper:
    def __init__(self, download_dir=None, login=None, password=None):
        self.download_dir = download_dir or DOWNLOAD_DIR
        self.login = login
        self.password = password
        self.session = requests.Session()
        self.cookies_file = "cookies.json"
        if os.path.exists(self.cookies_file):
            logger.info("Chargement des cookies depuis cookies.json pour requests")
            with open(self.cookies_file, 'r') as f:
                cookies = json.load(f)
            self.session.cookies.update(cookies)
        self._setup_driver()

    def _setup_driver(self):
        if os.path.exists(self.download_dir):
            shutil.rmtree(self.download_dir)
        os.makedirs(self.download_dir)
        options = webdriver.ChromeOptions()
        #options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36")
        options.add_argument("--disable-blink-features=AutomationControlled")
        prefs = {
            "download.default_directory": self.download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
            "safebrowsing.enabled": True,
            "profile.managed_default_content_settings.images": 2,
        }
        options.add_experimental_option("prefs", prefs)
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        self.wait = WebDriverWait(self.driver, 30)

    def access_site(self, url, usern, password, force_auth=False):
        self.login = usern
        self.password = password

        if not force_auth and os.path.exists(self.cookies_file):
            logger.info("Test de validité des cookies chargés...")
            with open(self.cookies_file, 'r') as f:
                data = json.load(f)
                cookies_timestamp = data.get("timestamp", 0)
                cookies_age = time.time() - cookies_timestamp
                if cookies_age > 3600:
                    logger.info(f"Cookies trop vieux ({cookies_age:.0f}s), authentification requise")
                else:
                    cookie_dict = data.get("cookies", {})
                    if not isinstance(cookie_dict, dict):
                        logger.warning("Format des cookies invalide dans cookies.json, authentification requise")
                    else:
                        self.session.cookies.clear()
                        self.session.cookies.update(cookie_dict)
                        test_url = "https://api.pharma.sobrus.com/customers/export-customer-statement?type=advanced&start_date=2017-01-01&end_date=2025-04-10&customer_id=2211711"
                        try:
                            test_response = self.session.get(test_url, timeout=10)
                            if test_response.status_code == 200 and "Unauthorized" not in test_response.text:
                                logger.info("Cookies valides pour requests, application au driver")
                                self.driver.get("https://app.pharma.sobrus.com/")
                                for cookie in cookie_dict.items():
                                    self.driver.add_cookie({"name": cookie[0], "value": cookie[1], "domain": ".pharma.sobrus.com"})
                                return
                            else:
                                logger.info(f"Cookies invalides (code {test_response.status_code} ou contenu invalide), authentification requise")
                        except RequestException as e:
                            logger.warning(f"Erreur lors du test des cookies : {e}, authentification requise")

        logger.info(f"Accès à {url} pour authentification complète")
        self.driver.get(url)
        time.sleep(2)

        logger.info("Étape 1 : Recherche du bouton 'S’identifier'")
        button_login = self.wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-testid="s\'identifier"]')),
            message="Bouton 'S’identifier' non trouvé"
        )
        button_login.click()

        logger.info("Étape 2 : Saisie du login")
        login_input = self.wait.until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, "input[name='login']")),
            message="Champ login non trouvé"
        )
        login_input.clear()
        login_input.send_keys(usern)
        login_validation_button = self.wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn-signup")),
            message="Bouton de validation du login non trouvé"
        )
        login_validation_button.click()

        logger.info("Étape 3 : Saisie du mot de passe")
        password_input = self.wait.until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, "input[name='password']")),
            message="Champ mot de passe non trouvé"
        )
        password_input.clear()
        password_input.send_keys(password)
        login_validation_button = self.wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn-signup")),
            message="Bouton de validation du mot de passe non trouvé"
        )
        login_validation_button.click()

        logger.info("Étape 4 : Attente de redirection après authentification")
        self.wait.until(
            lambda driver: driver.current_url.startswith("https://app.pharma.sobrus.com/"),
            message="Redirection après authentification échouée"
        )
        logger.info(f"Authentification réussie, URL actuelle : {self.driver.current_url}")
        self.get_cookies_for_requests()

    def is_session_active(self):
        try:
            self.driver.get("https://app.pharma.sobrus.com/customers")
            if "login" in self.driver.current_url:
                logger.warning("Redirection détectée vers la page de login")
                return False
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.sob-v2-table")))
            logger.info("Session active vérifiée")
            return True
        except TimeoutException:
            logger.warning("Session inactive ou page non chargée")
            return False

    def ensure_session(self):
        logger.info("Vérification de la session...")
        if not self.is_session_active():
            logger.warning("Session invalide ou expirée, reconnexion...")
            if self.login and self.password:
                self.access_site("https://app.pharma.sobrus.com/", self.login, self.password, force_auth=True)
            else:
                logger.error("Aucune information d’authentification disponible pour restaurer la session")
                raise Exception("Aucune information d’authentification disponible")

    def get_clients_from_page(self):
        logger.info("Extraction des clients depuis la page")
        if "login" in self.driver.current_url:
            logger.error("Redirigé vers la page de login, session invalide")
            raise Exception("Session invalide, redirection vers login")
        self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.sob-v2-table tbody tr")))
        time.sleep(1)
        clients = []
        retries = 3
        while retries > 0:
            try:
                rows = self.driver.find_elements(By.CSS_SELECTOR, "table.sob-v2-table tbody tr")
                if not rows or not rows[0].is_displayed():
                    raise StaleElementReferenceException("Rows not displayed yet")
                for row in rows:
                    nom = row.find_element(By.TAG_NAME, "th").text.strip()
                    cells = row.find_elements(By.TAG_NAME, "td")
                    email = cells[0].text.strip()
                    telephone = cells[1].text.strip()
                    organisme = cells[2].text.strip()
                    immatriculation = cells[3].text.strip()
                    clients.append({
                        "nom": nom,
                        "email": email,
                        "telephone": telephone,
                        "organisme": organisme,
                        "immatriculation": immatriculation,
                        "lien": row
                    })
                    logger.debug(f"Client extrait: {nom}")
                break
            except StaleElementReferenceException as e:
                retries -= 1
                logger.warning(f"Stale element détecté, tentative restante : {retries}")
                time.sleep(2)
                if retries == 0:
                    logger.error(f"Échec après retries : {e}")
                    raise
            except Exception as e:
                logger.error(f"Erreur parsing ligne client: {e}")
                break
        logger.info(f"{len(clients)} clients extraits")
        return clients

    def retrieve_client_key(self, client):
        logger.info(f"Récupération clé pour {client['nom']}")
        client_xpath = f'//table[contains(@class, "sob-v2-table")]//tbody/tr[th/span[normalize-space()="{client["nom"]}"]]'
        client_row = self.wait.until(EC.element_to_be_clickable((By.XPATH, client_xpath)))
        self.driver.execute_script("arguments[0].scrollIntoView(true);", client_row)
        time.sleep(1)
        self.driver.execute_script("arguments[0].click();", client_row)
        self.wait.until(lambda d: "/customer/" in d.current_url)
        client_key = re.search(r"/customer/(\d+)/", self.driver.current_url).group(1)
        logger.info(f"Clé récupérée pour {client['nom']}: {client_key}")
        self.driver.get("https://app.pharma.sobrus.com/customers")
        self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.sob-v2-table")))
        time.sleep(1)
        return client_key

    def go_to_next_page(self):
        logger.info("Tentative passage page suivante")
        try:
            pagination = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.sob-v2-table-pagination")))
            current_page_element = pagination.find_element(By.CSS_SELECTOR, "span.sob-v2-TablePage")
            self.wait.until(lambda d: current_page_element.text.strip().isdigit(), "Numéro de page non chargé")
            current_page = int(current_page_element.text.strip())
            next_button = pagination.find_element(By.XPATH, ".//span[contains(@class, 'sob-v2-TablePage')]/following-sibling::button[1]")
            if "sob-v2-TablePage__disabled" in next_button.get_attribute("class"):
                logger.info("Bouton suivant désactivé, dernière page")
                return False
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.5)
            self.driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
            time.sleep(0.5)
            self.wait.until(EC.element_to_be_clickable(next_button))
            self.driver.execute_script("arguments[0].click();", next_button)
            self.wait.until(lambda d: d.find_element(By.CSS_SELECTOR, "span.sob-v2-TablePage").text.strip().isdigit() and
                                           int(d.find_element(By.CSS_SELECTOR, "span.sob-v2-TablePage").text.strip()) > current_page)
            logger.info(f"Passage à la page {current_page + 1}")
            return True
        except Exception as e:
            logger.warning(f"Échec passage page suivante: {e}")
            return False

    def get_cookies_for_requests(self):
        if self.driver:
            cookies = self.driver.get_cookies()
            self.session.cookies.clear()
            for cookie in cookies:
                self.session.cookies.set(cookie['name'], cookie['value'], domain=cookie['domain'])
            with open(self.cookies_file, 'w') as f:
                json.dump({"cookies": {c['name']: c['value'] for c in cookies}, "timestamp": time.time()}, f)
            logger.info("Cookies extraits et sauvegardés dans cookies.json")
        else:
            logger.warning("Aucun driver actif, utilisation des cookies précédemment chargés")

    def download_detailed_pdf_api_with_requests(self, client, start_date, end_date, timeout=30):
        url = f"https://api.pharma.sobrus.com/customers/export-customer-statement?type=advanced&start_date={start_date}&end_date={end_date}&customer_id={client['client_id']}"
        logger.info(f"Téléchargement du PDF détaillé pour {client['nom']} via l'URL: {url}")
        client_key = client['client_id']
        client_dir = os.path.join(self.download_dir, str(client_key))
        if not os.path.exists(client_dir):
            os.makedirs(client_dir)
        timestamp = str(int(time.time() * 1000))
        pdf_filename = f"{client_key}_{timestamp}.pdf"
        pdf_path = os.path.join(client_dir, pdf_filename)
        try:
            response = self.session.get(url, stream=True, timeout=30)
            if response.status_code != 200:
                raise Exception(f"Erreur HTTP {response.status_code}: {response.text}")
            with open(pdf_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            size = os.path.getsize(pdf_path)
            if size < 1000:
                raise Exception(f"Fichier {pdf_path} trop petit ({size} bytes)")
            logger.info(f"PDF détaillé téléchargé pour {client['nom']} : {pdf_path}")
            return pdf_path
        except Exception as e:
            logger.error(f"Erreur lors du téléchargement pour {client['nom']} : {str(e)}")
            if os.path.exists(pdf_path):
                os.remove(pdf_path)
            raise

    def cleanup(self):
        logger.info("Nettoyage scraper")
        self.driver.quit()
        if os.path.exists(self.download_dir):
            shutil.rmtree(self.download_dir)