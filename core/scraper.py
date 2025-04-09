import os
import re
import time
import sys
import shutil
import requests
from requests.exceptions import RequestException
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from config.config import DOWNLOAD_DIR

class PharmaScraper:
    def __init__(self, download_dir=None):
        self.download_dir = download_dir or DOWNLOAD_DIR
        self.login = None
        self.password = None
        self.session = requests.Session()  # Session requests pour gérer les cookies
        self._setup_driver()

    def _setup_driver(self):
        if os.path.exists(self.download_dir):
            shutil.rmtree(self.download_dir)
        os.makedirs(self.download_dir)
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")  # Activer le mode headless
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        prefs = {
            "download.default_directory": self.download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
            "safebrowsing.enabled": True,
            "profile.managed_default_content_settings.images": 2,
        }
        options.add_experimental_option("prefs", prefs)
        self.driver = webdriver.Chrome(options=options)
        self.wait = WebDriverWait(self.driver, 20)

    def access_site(self, url, usern, password):
        if self.login == usern and self.password == password and self.is_session_active():
            print("Session déjà active, réutilisation...")
            sys.stdout.flush()
            return
        print(f"Accès à {url}")
        sys.stdout.flush()
        self.driver.get(url)
        try:
            print("Étape 1 : Recherche du bouton 'S’identifier'")
            sys.stdout.flush()
            button_login = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-testid="s\'identifier"]')),
                message="Bouton 'S’identifier' non trouvé"
            )
            button_login.click()

            print("Étape 2 : Saisie du login")
            sys.stdout.flush()
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

            print("Étape 3 : Saisie du mot de passe")
            sys.stdout.flush()
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

            print("Étape 4 : Attente de redirection après authentification")
            sys.stdout.flush()
            self.wait.until(
                lambda driver: driver.current_url.startswith("https://app.pharma.sobrus.com/"),
                message="Redirection après authentification échouée"
            )
            self.login = usern
            self.password = password
            print(f"Authentification réussie, URL actuelle : {self.driver.current_url}")
            sys.stdout.flush()
        except TimeoutException as e:
            print(f"Erreur de timeout lors de l’authentification : {e}")
            sys.stdout.flush()
            raise
        except Exception as e:
            print(f"Erreur inattendue lors de l’authentification : {e}")
            sys.stdout.flush()
            raise

    def is_session_active(self):
        try:
            self.driver.get("https://app.pharma.sobrus.com/customers")
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.sob-v2-table")))
            return True
        except TimeoutException:
            return False

    def ensure_session(self):
        print("Vérification de la session...")
        sys.stdout.flush()
        if not self.is_session_active():
            print("Session invalide ou expirée, reconnexion...")
            sys.stdout.flush()
            if self.login and self.password:
                self.access_site("https://app.pharma.sobrus.com/", self.login, self.password)
            else:
                raise Exception("Aucune information d’authentification disponible pour restaurer la session")

    def get_clients_from_page(self):
        self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.sob-v2-table tbody tr")))
        self.wait.until(
            lambda driver: len(driver.find_elements(By.CSS_SELECTOR, "table.sob-v2-table tbody tr")) > 0
                           and driver.find_elements(By.CSS_SELECTOR, "table.sob-v2-table tbody tr")[0].is_displayed(),
            "Le tableau est présent mais pas encore complètement chargé"
        )
        clients = []
        rows = self.driver.find_elements(By.CSS_SELECTOR, "table.sob-v2-table tbody tr")
        for i in range(len(rows)):
            retries = 3
            while retries > 0:
                try:
                    rows = self.driver.find_elements(By.CSS_SELECTOR, "table.sob-v2-table tbody tr")
                    row = rows[i]
                    nom = row.find_element(By.TAG_NAME, "th").text.strip()
                    cells = row.find_elements(By.TAG_NAME, "td")
                    email = cells[0].text.strip()
                    telephone = cells[1].text.strip()
                    organisme = cells[2].text.strip()
                    immatriculation = cells[3].text.strip()
                    solde_text = cells[4].find_element(By.CSS_SELECTOR, "span.sob-v2-table-tag-text").text.strip()
                    solde = float(solde_text.replace(",", ".")) if "." in solde_text else float(solde_text.replace(",", ""))
                    clients.append({
                        "nom": nom,
                        "email": email,
                        "telephone": telephone,
                        "organisme": organisme,
                        "immatriculation": immatriculation,
                        "solde": solde,
                        "lien": row
                    })
                    break
                except StaleElementReferenceException as e:
                    retries -= 1
                    print(f"Stale element détecté, tentative restante : {retries}")
                    time.sleep(1)
                    if retries == 0:
                        print(f"Échec après retries pour la ligne {i} : {e}")
                        break
                except Exception as e:
                    print(f"Erreur parsing ligne client: {e}")
                    sys.stdout.flush()
                    break
        return clients

    def retrieve_client_key(self, client):
        client_xpath = f'//table[contains(@class, "sob-v2-table")]//tbody/tr[th/span[normalize-space()="{client["nom"]}"]]'
        client_row = self.wait.until(EC.element_to_be_clickable((By.XPATH, client_xpath)))
        self.driver.execute_script("arguments[0].scrollIntoView(true);", client_row)
        time.sleep(1)
        self.driver.execute_script("arguments[0].click();", client_row)
        self.wait.until(lambda d: "/customer/" in d.current_url)
        client_key = re.search(r"/customer/(\d+)/", self.driver.current_url).group(1)
        self.driver.get("https://app.pharma.sobrus.com/customers")
        self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.sob-v2-table")))
        time.sleep(1)
        return client_key

    def go_to_next_page(self):
        try:
            pagination = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.sob-v2-table-pagination")))
            current_page_element = pagination.find_element(By.CSS_SELECTOR, "span.sob-v2-TablePage")
            self.wait.until(lambda d: current_page_element.text.strip() != "")
            current_page = int(current_page_element.text.strip())
            next_button = pagination.find_element(By.XPATH, ".//span[contains(@class, 'sob-v2-TablePage')]/following-sibling::button[1]")
            if "sob-v2-TablePage__disabled" in next_button.get_attribute("class"):
                return False
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.5)
            self.driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
            time.sleep(0.5)
            self.wait.until(EC.visibility_of(next_button))
            self.driver.execute_script("arguments[0].click();", next_button)
            self.wait.until(lambda d: int(d.find_element(By.CSS_SELECTOR, "span.sob-v2-TablePage").text.strip()) > current_page)
            return True
        except Exception:
            return False

    def get_cookies_for_requests(self):
        """Extrait les cookies de Selenium pour les utiliser avec requests."""
        cookies = self.driver.get_cookies()
        for cookie in cookies:
            self.session.cookies.set(cookie['name'], cookie['value'], domain=cookie['domain'])
        print("Cookies extraits pour requests")
        sys.stdout.flush()

    def download_detailed_pdf_api_with_requests(self, client, start_date, end_date, timeout=30):
        """Télécharge un PDF détaillé via l'API en utilisant requests."""
        url = f"https://api.pharma.sobrus.com/customers/export-customer-statement?type=advanced&start_date={start_date}&end_date={end_date}&customer_id={client['client_id']}&details=details_with_discount"
        print(f"Téléchargement du PDF détaillé pour {client['nom']} via l'URL: {url}")
        sys.stdout.flush()

        # Utiliser la client_key comme nom de dossier
        client_key = client['client_id']
        client_dir = os.path.join(self.download_dir, str(client_key))
        if not os.path.exists(client_dir):
            os.makedirs(client_dir)

        # Ajouter un identifiant unique au nom du fichier pour éviter les conflits
        timestamp = str(int(time.time() * 1000))  # Timestamp en millisecondes
        pdf_filename = f"{client_key}_{timestamp}.pdf"
        pdf_path = os.path.join(client_dir, pdf_filename)

        max_attempts = 2
        for attempt in range(max_attempts):
            try:
                response = self.session.get(url, stream=True, timeout=timeout)
                if response.status_code != 200:
                    raise Exception(f"Erreur HTTP {response.status_code}: {response.text}")

                with open(pdf_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

                size = os.path.getsize(pdf_path)
                if size < 1000:
                    raise Exception(f"Fichier {pdf_path} trop petit ({size} bytes)")

                print(f"PDF détaillé téléchargé pour {client['nom']} : {pdf_path}")
                sys.stdout.flush()
                return pdf_path

            except RequestException as e:
                print(f"Échec téléchargement (tentative {attempt + 1}/{max_attempts}) pour {client['nom']} : {str(e)}")
                sys.stdout.flush()
                if attempt == max_attempts - 1:
                    raise
                time.sleep(1)
            except Exception as e:
                print(f"Erreur lors du téléchargement pour {client['nom']} : {str(e)}")
                sys.stdout.flush()
                if os.path.exists(pdf_path):
                    os.remove(pdf_path)
                raise

    def download_detailed_pdf_api(self, client, start_date, end_date, timeout=30):
        """Méthode originale utilisant Selenium (gardée pour compatibilité)."""
        url = f"https://api.pharma.sobrus.com/customers/export-customer-statement?type=advanced&start_date={start_date}&end_date={end_date}&customer_id={client['client_id']}&details=details_with_discount"
        print(f"Téléchargement du PDF détaillé pour {client['nom']} via l'URL: {url}")
        sys.stdout.flush()
        self.ensure_session()
        max_attempts = 2
        for attempt in range(max_attempts):
            try:
                self.driver.get(url)
                pdf_file = self.wait_for_download(timeout=timeout)
                print(f"PDF détaillé téléchargé pour {client['nom']} : {pdf_file}")
                sys.stdout.flush()
                return pdf_file
            except Exception as e:
                print(f"Échec téléchargement (tentative {attempt + 1}/{max_attempts}) pour {client['nom']} : {str(e)}")
                sys.stdout.flush()
                if attempt == max_attempts - 1:
                    raise
                time.sleep(1)

    def wait_for_download(self, timeout=30):
        print(f"Attente du téléchargement (timeout={timeout} secondes)...")
        sys.stdout.flush()
        end_time = time.time() + timeout
        while time.time() < end_time:
            files = [f for f in os.listdir(self.download_dir) if f.endswith(".pdf")]
            if files:
                pdf_path = os.path.join(self.download_dir, files[0])
                size = os.path.getsize(pdf_path)
                if size > 1000:
                    print(f"Fichier détecté : {pdf_path}, taille : {size} bytes")
                    sys.stdout.flush()
                    return pdf_path
                else:
                    print(f"Fichier {pdf_path} trop petit ({size} bytes), attente...")
                    sys.stdout.flush()
            time.sleep(1)
        raise Exception(f"Téléchargement PDF échoué après {timeout} secondes")

    def cleanup(self):
        self.driver.quit()
        if os.path.exists(self.download_dir):
            shutil.rmtree(self.download_dir)