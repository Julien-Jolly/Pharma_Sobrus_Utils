import os
import sys
from dotenv import load_dotenv
import time
import sqlite3
import re
import datetime
import shutil
import pdfplumber
import boto3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException

# --- Chargement des variables d'environnement ---
load_dotenv()
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
region = os.getenv("AWS_DEFAULT_REGION")
bucket_name = os.getenv("AWS_BUCKET")

# --- Constantes globales pour les dates ---
START_DATE = "2017-01-01"
END_DATE = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

# Vérification des identifiants AWS
if not aws_access_key_id or not aws_secret_access_key:
    raise ValueError("Les clés AWS_ACCESS_KEY_ID et AWS_SECRET_ACCESS_KEY doivent être définies dans l’environnement ou un fichier .env")
print(f"Clés chargées : {aws_access_key_id[:8]}... (region: {region})")
sys.stdout.flush()

# --- Configuration S3 ---
s3_client = boto3.client("s3", region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

def upload_to_s3(local_file, bucket_name, s3_file):
    try:
        print(f"Paramètres : local_file={repr(local_file)}, bucket_name={repr(bucket_name)}, s3_file={repr(s3_file)}")
        sys.stdout.flush()
        if not os.path.exists(local_file):
            raise FileNotFoundError(f"Le fichier {local_file} n'existe pas")
        print(f"Fichier {local_file} trouvé, taille : {os.path.getsize(local_file)} bytes")
        sys.stdout.flush()
        s3_client.upload_file(local_file, bucket_name, s3_file)
        print(f"Base de données {local_file} uploadée avec succès vers S3://{bucket_name}/{s3_file}")
        sys.stdout.flush()
    except Exception as e:
        print(f"Erreur lors de l'upload vers S3 : {e}")
        sys.stdout.flush()
        raise

def verify_s3_upload(bucket_name, s3_file):
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_file)
        print(f"Vérification réussie : le fichier {s3_file} existe sur S3://{bucket_name}.")
        sys.stdout.flush()
        return True
    except Exception as e:
        print(f"Erreur lors de la vérification de l'upload sur S3 : {e}")
        sys.stdout.flush()
        raise Exception(f"Le fichier {s3_file} n'a pas été trouvé sur S3://{bucket_name} après l'upload.")

class PharmaScraper:
    def __init__(self, driver, wait, download_dir):
        self.driver = driver
        self.wait = wait
        self.download_dir = download_dir

    def access_site(self, url, usern, password):
        print(f"Tentative d'accès au site : {url}")
        sys.stdout.flush()
        self.driver.get(url)
        try:
            button_login = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-testid="s\'identifier"]')), "Bouton 'S'identifier' non trouvé")
            print("Bouton 'S'identifier' trouvé, clic en cours...")
            sys.stdout.flush()
            button_login.click()
        except Exception as e:
            print(f"Erreur lors de la recherche du bouton de login : {e}")
            sys.stdout.flush()
            raise

        try:
            login_input = self.wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "input[name='login']")), "Champ login non trouvé")
            print("Champ login trouvé, saisie de l'utilisateur...")
            sys.stdout.flush()
            login_input.send_keys(usern)
            login_validation_button = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn-signup")), "Bouton de validation du login non trouvé")
            login_validation_button.click()
        except Exception as e:
            print(f"Erreur lors de la saisie du login : {e}")
            sys.stdout.flush()
            raise

        try:
            password_input = WebDriverWait(self.driver, 30).until(
                lambda driver: driver.find_element(By.CSS_SELECTOR, "input[name='password']") or
                               driver.find_element(By.CSS_SELECTOR, "input[type='password']"),
                "Champ mot de passe non trouvé"
            )
            print("Champ mot de passe trouvé, saisie du mot de passe...")
            sys.stdout.flush()
            password_input.send_keys(password)
            login_validation_button = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn-signup")), "Bouton de validation du mot de passe non trouvé")
            login_validation_button.click()
            print("Authentification soumise, attente de redirection...")
            sys.stdout.flush()
        except Exception as e:
            print(f"Erreur lors de la saisie du mot de passe : {e}")
            sys.stdout.flush()
            raise

    def get_clients_from_page(self):
        self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.sob-v2-table tbody tr")))
        self.wait.until(
            lambda driver: len(driver.find_elements(By.CSS_SELECTOR, "table.sob-v2-table tbody tr")) > 0
                           and driver.find_elements(By.CSS_SELECTOR, "table.sob-v2-table tbody tr")[0].is_displayed(),
            "Le tableau est présent mais pas encore complètement chargé"
        )

        clients = []
        row_count = len(self.driver.find_elements(By.CSS_SELECTOR, "table.sob-v2-table tbody tr"))
        for i in range(row_count):
            retries = 3
            while retries > 0:
                try:
                    rows = self.driver.find_elements(By.CSS_SELECTOR, "table.sob-v2-table tbody tr")
                    row = rows[i]
                    try:
                        nom_cell = row.find_element(By.TAG_NAME, "th")
                        nom = nom_cell.text.strip()
                    except Exception:
                        nom = ""
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 5:
                        email = cells[0].text.strip()
                        telephone = cells[1].text.strip()
                        organisme = cells[2].text.strip()
                        immatriculation = cells[3].text.strip()
                        try:
                            solde_span = cells[4].find_element(By.CSS_SELECTOR, "span.sob-v2-table-tag-text")
                            solde_text = solde_span.text.strip()
                        except Exception:
                            solde_text = cells[4].text.strip()
                        if "." in solde_text:
                            solde_text = solde_text.replace(",", "")
                        else:
                            solde_text = solde_text.replace(",", ".")
                        try:
                            solde = float(solde_text)
                        except Exception:
                            solde = None
                        clients.append({
                            "nom": nom,
                            "email": email,
                            "telephone": telephone,
                            "organisme": organisme,
                            "immatriculation": immatriculation,
                            "solde": solde,
                            "lien": row,
                        })
                    break
                except StaleElementReferenceException as e:
                    retries -= 1
                    print(f"Stale element détecté, tentative restante : {retries}")
                    sys.stdout.flush()
                    time.sleep(1)
                    if retries == 0:
                        print(f"Échec après retries pour la ligne {i} : {e}")
                        sys.stdout.flush()
                        break
                except Exception as e:
                    print(f"Erreur inattendue pour la ligne {i} : {e}")
                    sys.stdout.flush()
                    break
        return clients

    def access_site_and_get_clients(self, url, usern, password):
        self.access_site(url, usern, password)
        print("Authentification réussie, passage à la page des clients...")
        sys.stdout.flush()
        time.sleep(2)
        self.driver.get("https://app.pharma.sobrus.com/customers")
        print("Page des clients chargée.")
        sys.stdout.flush()
        return self.get_clients_from_page()

    def wait_for_download(self, timeout=30):
        end_time = time.time() + timeout
        while time.time() < end_time:
            files = [f for f in os.listdir(self.download_dir) if f.endswith(".pdf")]
            if files:
                pdf_path = os.path.join(self.download_dir, files[0])
                if os.path.getsize(pdf_path) > 1000:
                    print(f"Fichier détecté : {pdf_path}, taille : {os.path.getsize(pdf_path)} bytes")
                    sys.stdout.flush()
                    return pdf_path
                else:
                    print(f"Fichier {pdf_path} trop petit, attente...")
                    sys.stdout.flush()
            time.sleep(1)
        raise Exception(f"Téléchargement du PDF non terminé après {timeout} secondes.")

    def merge_thousands(self, ligne):
        def repl(match):
            group1 = match.group(1)
            group2 = match.group(2)
            if len(group1) > 1:
                return group1 + group2
            else:
                return group1 + " " + group2
        return re.sub(r"(\d)\s+(\d{3},\d+)", repl, ligne)

    def extract_data_from_pdf(self, pdf_file, client):
        print(f"Début de l'extraction pour {client['nom']} avec le fichier {pdf_file}")
        sys.stdout.flush()
        records = []
        solde_final = None
        try:
            with pdfplumber.open(pdf_file) as pdf:
                print(f"PDF ouvert : {len(pdf.pages)} pages détectées")
                sys.stdout.flush()
                for page_num, page in enumerate(pdf.pages, 1):
                    print(f"Traitement de la page {page_num}/{len(pdf.pages)}")
                    sys.stdout.flush()
                    try:
                        text = page.extract_text(timeout=10)  # Timeout conservé
                        if not text:
                            print(f"Page {page_num} vide ou non extraite")
                            sys.stdout.flush()
                            continue
                    except Exception as e:
                        print(f"Erreur lors de l'extraction du texte de la page {page_num} : {e}")
                        sys.stdout.flush()
                        continue
                    lines = text.split("\n")
                    print(f"Page {page_num} - Lignes brutes : {lines}")
                    sys.stdout.flush()

                    # Fusion intelligente des lignes (restaurée)
                    processed_lines = []
                    buffer_line = ""
                    for line in lines:
                        line = line.strip()
                        if not line:
                            continue
                        line = self.merge_thousands(line)
                        if re.match(r"^\d{4}-\d{2}-\d{2}", line):
                            if buffer_line:
                                processed_lines.append(buffer_line)
                            buffer_line = line
                        elif line.lower().startswith("solde final"):
                            if buffer_line:
                                processed_lines.append(buffer_line)
                            buffer_line = line
                        else:
                            if re.match(r"^-?\d+(?:\s+-?\d+(?:[.,]\d+)?){1,2}$", line) and buffer_line and not re.match(
                                    r"^\d{4}-\d{2}-\d{2}", buffer_line):
                                buffer_line += " " + line
                            elif buffer_line and re.match(r"^\d{4}-\d{2}-\d{2}", buffer_line):
                                processed_lines.append(buffer_line)
                                buffer_line = line
                            else:
                                buffer_line += " " + line
                    if buffer_line:
                        processed_lines.append(buffer_line)
                    print(f"Page {page_num} - Lignes après fusion : {processed_lines}")
                    sys.stdout.flush()

                    # Parsing des lignes (restauré avec logs)
                    for line in processed_lines:
                        print(f"Parsing de la ligne : {line}")
                        sys.stdout.flush()
                        tokens = line.split()
                        if line.lower().startswith("solde final"):
                            try:
                                num_part = "".join(tokens[2:]).replace(" ", "")
                                solde_final = float(num_part.replace(",", "."))
                                print(f"Solde final extrait : {solde_final}")
                                sys.stdout.flush()
                            except Exception as e:
                                print(f"Erreur d'extraction du solde final : {e}")
                                sys.stdout.flush()
                            continue
                        if not re.match(r"^\d{4}-\d{2}-\d{2}", line):
                            print(f"Ligne ignorée (pas de date valide) : {line}")
                            sys.stdout.flush()
                            continue
                        try:
                            date_obj = datetime.datetime.strptime(tokens[0], "%Y-%m-%d")
                            print(f"Date extraite : {date_obj}")
                            sys.stdout.flush()
                        except Exception as e:
                            print(f"Erreur de conversion de date : {e}")
                            sys.stdout.flush()
                            continue

                        i = len(tokens) - 1
                        numeric_tokens = []
                        temp_number = ""
                        while i >= 0:
                            if re.match(r"^-?\d+(?:[.,]\d+)?$", tokens[i]):
                                if len(tokens[i]) > 5 and tokens[i].isdigit():
                                    break
                                if "paiement" in line.lower() and temp_number:
                                    temp_number = tokens[i] + " " + temp_number
                                else:
                                    if temp_number:
                                        numeric_tokens.append(temp_number)
                                    temp_number = tokens[i]
                            elif tokens[i] == "-" and i > 0 and "paiement" in line.lower() and re.match(
                                    r"^-?\d+(?:[.,]\d+)?$", tokens[i - 1]):
                                temp_number = "-" + " " + temp_number
                            else:
                                if temp_number:
                                    numeric_tokens.append(temp_number)
                                    temp_number = ""
                                break
                            i -= 1
                        if temp_number:
                            numeric_tokens.append(temp_number)
                        numeric_tokens = list(reversed(numeric_tokens))
                        if "paiement" not in line.lower() and len(numeric_tokens) > 3:
                            numeric_tokens = numeric_tokens[-3:]
                        print(f"Numeric tokens détectés : {numeric_tokens}")
                        sys.stdout.flush()

                        if len(numeric_tokens) == 3:  # Achat ou retour
                            try:
                                quantite = float(numeric_tokens[0].replace(",", "."))
                                prix = float(numeric_tokens[1].replace(",", "."))
                                total_val = float(numeric_tokens[2].replace(",", "."))
                                produit = " ".join(tokens[1:len(tokens) - len(numeric_tokens)])
                                debit = -abs(total_val) if quantite > 0 else abs(total_val)
                                credit = None
                                print(
                                    f"Achat/Retour - Produit: {produit}, Quantité: {quantite}, Débit: {debit}, Crédit: {credit}")
                                sys.stdout.flush()
                            except Exception as e:
                                print(f"Erreur de parsing des nombres : {e}")
                                sys.stdout.flush()
                                continue
                        elif len(numeric_tokens) == 1:  # Paiement ou avoir
                            try:
                                total_val = float(numeric_tokens[0].replace(" ", "").replace(",", "."))
                                produit = " ".join(tokens[1:len(tokens) - len(numeric_tokens[0].split())]).replace(
                                    " - -",
                                    "").strip()
                                if "paiement" in produit.lower():
                                    credit = abs(total_val)
                                    debit = None
                                elif "avoir" in produit.lower() and total_val > 0:
                                    credit = None
                                    debit = -total_val
                                else:
                                    credit = None
                                    debit = None
                                print(
                                    f"Paiement/Avoir - Produit: {produit}, Total: {total_val}, Débit: {debit}, Crédit: {credit}")
                                sys.stdout.flush()
                                quantite = None
                            except Exception as e:
                                print(f"Erreur de parsing du total : {e}")
                                sys.stdout.flush()
                                continue
                        else:
                            print(f"Format inattendu dans la ligne : {line}")
                            sys.stdout.flush()
                            continue

                        record = {
                            "nom": client["nom"],
                            "date": date_obj,
                            "produit": produit,
                            "quantite": quantite,
                            "debit": debit,
                            "credit": credit,
                        }
                        print(f"Enregistrement créé : {record}")
                        sys.stdout.flush()
                        records.append(record)

        except Exception as e:
            print(f"Erreur globale dans extract_data_from_pdf : {e}")
            sys.stdout.flush()
            raise
        print(f"Extraction terminée pour {client['nom']} : {len(records)} records, solde final = {solde_final}")
        sys.stdout.flush()
        return records, solde_final

    def run_pdf_extraction(login, password, db_path, client_name=None):
        download_dir = os.path.join(os.getcwd(), "downloads")
        if os.path.exists(download_dir):
            try:
                shutil.rmtree(download_dir)
                print(f"Dossier {download_dir} supprimé avec succès.")
                sys.stdout.flush()
            except Exception as e:
                print(f"Erreur lors de la suppression initiale du dossier downloads : {e}")
                sys.stdout.flush()
        os.makedirs(download_dir)
        print(f"Dossier {download_dir} créé.")
        sys.stdout.flush()

        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        prefs = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
            "safebrowsing.enabled": True,
        }
        options.add_experimental_option("prefs", prefs)
        driver = webdriver.Chrome(options=options)
        wait = WebDriverWait(driver, 20)
        scraper = PharmaScraper(driver, wait, download_dir)

        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            if client_name:
                cursor.execute("DELETE FROM num_transactions WHERE nom = ?", (client_name,))
                cursor.execute("DELETE FROM solde_final WHERE nom = ?", (client_name,))
            else:
                cursor.execute("DROP TABLE IF EXISTS num_transactions")
                cursor.execute("DROP VIEW IF EXISTS vue_debiteurs")
                cursor.execute("DROP TABLE IF EXISTS solde_final")
                cursor.execute("""
                    CREATE TABLE num_transactions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        nom TEXT,
                        date DATE,
                        produit TEXT,
                        quantite INTEGER,
                        debit REAL,
                        credit REAL
                    )
                """)
            conn.commit()
            conn.close()

            _ = scraper.access_site_and_get_clients("https://app.pharma.sobrus.com/", login, password)

            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            if client_name:
                cursor.execute("SELECT nom, client_key FROM client_keys WHERE nom = ?", (client_name,))
            else:
                cursor.execute("SELECT nom, client_key FROM client_keys")
            client_keys_list = cursor.fetchall()
            conn.close()

            if not client_keys_list:
                print(
                    f"Aucune clé client trouvée en base pour {client_name if client_name else 'tous les clients'}. Veuillez exécuter le choix 1 au préalable.")
                sys.stdout.flush()
                return

            all_records = []
            solde_final_dict = {}
            for nom, client_key in client_keys_list:
                client = {"nom": nom, "client_id": client_key}
                try:
                    pdf_file = scraper.download_pdf_api(client)
                    data, solde_final = scraper.extract_data_from_pdf(pdf_file, client)
                    all_records.extend(data)
                    if solde_final is not None:
                        solde_final_dict[client["nom"]] = solde_final
                    try:
                        os.remove(pdf_file)
                        print(f"PDF {pdf_file} supprimé.")
                        sys.stdout.flush()
                    except Exception as e:
                        print(f"Erreur lors de la suppression du fichier PDF {pdf_file} : {e}")
                        sys.stdout.flush()
                except Exception as e:
                    print(f"Erreur lors du traitement du PDF pour {nom} : {e}")
                    sys.stdout.flush()

            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            if not client_name:
                cursor.execute("""
                    CREATE VIEW vue_debiteurs AS
                    SELECT nom,
                           strftime('%Y-%m', date) as mois,
                           ROUND(SUM(COALESCE(debit, 0)), 2) as total_debit,
                           ROUND(SUM(COALESCE(credit, 0)), 2) as total_credit,
                           ROUND(SUM(COALESCE(credit, 0)) + SUM(COALESCE(debit, 0)), 2) as solde
                    FROM num_transactions
                    GROUP BY nom, mois
                """)
                cursor.execute("CREATE TABLE IF NOT EXISTS solde_final (nom TEXT, solde_final REAL)")
            for record in all_records:
                cursor.execute("""
                    INSERT INTO num_transactions (nom, date, produit, quantite, debit, credit)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    record.get("nom"),
                    record["date"].strftime("%Y-%m-%d"),
                    record.get("produit"),
                    record.get("quantite"),
                    record.get("debit"),
                    record.get("credit"),
                ))
                print(f"Inserted into num_transactions: {record}")
                sys.stdout.flush()
            for nom, solde in solde_final_dict.items():
                cursor.execute("INSERT OR REPLACE INTO solde_final (nom, solde_final) VALUES (?, ?)", (nom, solde))
                print(f"Inserted into solde_final: {nom}, {solde}")
                sys.stdout.flush()
            conn.commit()
            conn.close()

            print("Upload de la base de données vers S3 après l'extraction des données PDF...")
            sys.stdout.flush()
            upload_to_s3(db_path, bucket_name, os.path.basename(db_path))
            verify_s3_upload(bucket_name, os.path.basename(db_path))

            print("Script terminé avec succès!")
            sys.stdout.flush()
        finally:
            driver.quit()
            print("WebDriver fermé.")
            sys.stdout.flush()
            if os.path.exists(download_dir):
                try:
                    shutil.rmtree(download_dir)
                    print(f"Dossier {download_dir} supprimé après exécution.")
                    sys.stdout.flush()
                except Exception as e:
                    print(f"Erreur lors de la suppression finale du dossier downloads : {e}")
                    sys.stdout.flush()

    def retrieve_client_key(self, client):
        time.sleep(2)
        client_xpath = f'//table[contains(@class, "sob-v2-table")]//tbody/tr[th/span[normalize-space()="{client["nom"]}"]]'
        client_row = self.wait.until(EC.element_to_be_clickable((By.XPATH, client_xpath)))
        self.driver.execute_script("arguments[0].scrollIntoView(true);", client_row)
        time.sleep(1)
        try:
            client_row.click()
        except Exception:
            self.driver.execute_script("arguments[0].click();", client_row)
        self.wait.until(lambda d: "/customer/" in d.current_url)
        current_url = self.driver.current_url
        m = re.search(r"/customer/(\d+)/", current_url)
        client_key = m.group(1) if m else None
        print(f"Client {client['nom']} a la clé: {client_key}")
        sys.stdout.flush()
        self.driver.get("https://app.pharma.sobrus.com/customers")
        self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.sob-v2-table")))
        time.sleep(2)
        return client_key

    def download_pdf_api(self, client):
        url = f"https://api.pharma.sobrus.com/customers/export-customer-statement?type=simple&start_date={START_DATE}&end_date={END_DATE}&customer_id={client['client_id']}"
        print(f"Téléchargement du PDF pour {client['nom']} via l'URL: {url}")
        sys.stdout.flush()
        self.driver.get(url)
        pdf_file = self.wait_for_download()
        print(f"PDF téléchargé : {pdf_file}, taille : {os.path.getsize(pdf_file)} bytes")
        sys.stdout.flush()
        return pdf_file

    def go_to_next_page(self):
        try:
            pagination = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.sob-v2-table-pagination")))
            current_page_element = pagination.find_element(By.CSS_SELECTOR, "span.sob-v2-TablePage")
            self.wait.until(lambda d: current_page_element.text.strip() != "")
            current_page = int(current_page_element.text.strip() or "0")
            next_button = pagination.find_element(
                By.XPATH,
                ".//span[contains(@class, 'sob-v2-TablePage')]/following-sibling::button[1]"
            )
            if "sob-v2-TablePage__disabled" in next_button.get_attribute("class"):
                print("Le bouton 'Suivant' est désactivé.")
                sys.stdout.flush()
                return False
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.5)
            self.driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
            time.sleep(0.5)
            self.wait.until(EC.visibility_of(next_button))
            try:
                next_button.click()
            except Exception as e:
                print("Clic standard échoué, tentative via JavaScript:", e)
                sys.stdout.flush()
                self.driver.execute_script("arguments[0].click();", next_button)
            self.wait.until(
                lambda d: int(
                    d.find_element(By.CSS_SELECTOR, "div.sob-v2-table-pagination span.sob-v2-TablePage").text.strip() or "0"
                ) > current_page
            )
            return True
        except Exception as e:
            print("Erreur lors de la navigation vers la page suivante :", e)
            sys.stdout.flush()
            return False

def run_client_keys_scraping(login, password, db_path):
    download_dir = os.path.join(os.getcwd(), "downloads")
    if os.path.exists(download_dir):
        shutil.rmtree(download_dir)
    os.makedirs(download_dir)

    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
    }
    options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 20)

    scraper = PharmaScraper(driver, wait, download_dir)
    clients = scraper.access_site_and_get_clients("https://app.pharma.sobrus.com/", login, password)
    client_keys_list = []
    count = 0

    while True:
        print(f"Clients sur la page actuelle : {[c['nom'] for c in clients]}")
        sys.stdout.flush()
        total_clients_on_page = len(clients)
        print(f"Nombre total de clients sur cette page : {total_clients_on_page}")
        sys.stdout.flush()
        for i, client in enumerate(clients, 1):
            count += 1
            print(f"Traitement du client {count} (page client {i}/{total_clients_on_page}) : {client['nom']}")
            sys.stdout.flush()
            client_key = scraper.retrieve_client_key(client)
            if client_key and (client["nom"], client_key) not in client_keys_list:
                client_keys_list.append((client["nom"], client_key))
            print(f"Clé récupérée pour {client['nom']} : {client_key}")
            sys.stdout.flush()
        if not scraper.go_to_next_page():
            print("Fin de la pagination, plus de pages à scraper.")
            sys.stdout.flush()
            break
        print("Passage à la page suivante...")
        sys.stdout.flush()
        clients = scraper.get_clients_from_page()

    print(f"Total de clients traités : {count}")
    sys.stdout.flush()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS client_keys")
    cursor.execute("""
        CREATE TABLE client_keys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nom TEXT,
            client_key TEXT
        )
    """)
    for nom, client_key in client_keys_list:
        cursor.execute("INSERT INTO client_keys (nom, client_key) VALUES (?, ?)", (nom, client_key))
    conn.commit()
    print("Contenu de la table client_keys :")
    sys.stdout.flush()
    cursor.execute("SELECT * FROM client_keys")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
        sys.stdout.flush()
    conn.close()
    driver.quit()

    print("Upload de la base de données vers S3 après la recherche des clés clients...")
    sys.stdout.flush()
    upload_to_s3(db_path, bucket_name, os.path.basename(db_path))
    verify_s3_upload(bucket_name, os.path.basename(db_path))

def run_pdf_extraction(login, password, db_path, client_name=None):
    download_dir = os.path.join(os.getcwd(), "downloads")
    if os.path.exists(download_dir):
        try:
            shutil.rmtree(download_dir)
            print(f"Dossier {download_dir} supprimé avec succès.")
            sys.stdout.flush()
        except Exception as e:
            print(f"Erreur lors de la suppression initiale du dossier downloads : {e}")
            sys.stdout.flush()
    os.makedirs(download_dir)
    print(f"Dossier {download_dir} créé.")
    sys.stdout.flush()

    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 20)
    scraper = PharmaScraper(driver, wait, download_dir)

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        if client_name:
            cursor.execute("DELETE FROM num_transactions WHERE nom = ?", (client_name,))
            cursor.execute("DELETE FROM solde_final WHERE nom = ?", (client_name,))
        else:
            cursor.execute("DROP TABLE IF EXISTS num_transactions")
            cursor.execute("DROP VIEW IF EXISTS vue_debiteurs")
            cursor.execute("DROP TABLE IF EXISTS solde_final")
            cursor.execute("""
                CREATE TABLE num_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nom TEXT,
                    date DATE,
                    produit TEXT,
                    quantite INTEGER,
                    debit REAL,
                    credit REAL
                )
            """)
        conn.commit()
        conn.close()

        _ = scraper.access_site_and_get_clients("https://app.pharma.sobrus.com/", login, password)

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        if client_name:
            cursor.execute("SELECT nom, client_key FROM client_keys WHERE nom = ?", (client_name,))
        else:
            cursor.execute("SELECT nom, client_key FROM client_keys")
        client_keys_list = cursor.fetchall()
        conn.close()

        if not client_keys_list:
            print(f"Aucune clé client trouvée en base pour {client_name if client_name else 'tous les clients'}. Veuillez exécuter le choix 1 au préalable.")
            sys.stdout.flush()
            return

        all_records = []
        solde_final_dict = {}
        for nom, client_key in client_keys_list:
            client = {"nom": nom, "client_id": client_key}
            try:
                pdf_file = scraper.download_pdf_api(client)
                data, solde_final = scraper.extract_data_from_pdf(pdf_file, client)
                all_records.extend(data)
                if solde_final is not None:
                    solde_final_dict[client["nom"]] = solde_final
                try:
                    os.remove(pdf_file)
                    print(f"PDF {pdf_file} supprimé.")
                    sys.stdout.flush()
                except Exception as e:
                    print(f"Erreur lors de la suppression du fichier PDF {pdf_file} : {e}")
                    sys.stdout.flush()
            except Exception as e:
                print(f"Erreur lors du traitement du PDF pour {nom} : {e}")
                sys.stdout.flush()

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        if not client_name:
            cursor.execute("""
                CREATE VIEW vue_debiteurs AS
                SELECT nom,
                       strftime('%Y-%m', date) as mois,
                       ROUND(SUM(COALESCE(debit, 0)), 2) as total_debit,
                       ROUND(SUM(COALESCE(credit, 0)), 2) as total_credit,
                       ROUND(SUM(COALESCE(credit, 0)) + SUM(COALESCE(debit, 0)), 2) as solde
                FROM num_transactions
                GROUP BY nom, mois
            """)
            cursor.execute("CREATE TABLE IF NOT EXISTS solde_final (nom TEXT, solde_final REAL)")
        for record in all_records:
            cursor.execute("""
                INSERT INTO num_transactions (nom, date, produit, quantite, debit, credit)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                record.get("nom"),
                record["date"].strftime("%Y-%m-%d"),
                record.get("produit"),
                record.get("quantite"),
                record.get("debit"),
                record.get("credit"),
            ))
            print(f"Inserted into num_transactions: {record}")
            sys.stdout.flush()
        for nom, solde in solde_final_dict.items():
            cursor.execute("INSERT OR REPLACE INTO solde_final (nom, solde_final) VALUES (?, ?)", (nom, solde))
        conn.commit()
        conn.close()

        print("Upload de la base de données vers S3 après l'extraction des données PDF...")
        sys.stdout.flush()
        upload_to_s3(db_path, bucket_name, os.path.basename(db_path))
        verify_s3_upload(bucket_name, os.path.basename(db_path))

        print("Script terminé avec succès!")
        sys.stdout.flush()
    finally:
        driver.quit()  # Toujours fermer le driver, même en cas d’erreur
        print("WebDriver fermé.")
        sys.stdout.flush()
        if os.path.exists(download_dir):
            try:
                shutil.rmtree(download_dir)
                print(f"Dossier {download_dir} supprimé après exécution.")
                sys.stdout.flush()
            except Exception as e:
                print(f"Erreur lors de la suppression finale du dossier downloads : {e}")
                sys.stdout.flush()
    driver.quit()

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python main.py <choice> <login> <password> [<client_name>]")
        sys.stdout.flush()
        sys.exit(1)

    choice = sys.argv[1]
    login = sys.argv[2]
    password = sys.argv[3]
    db_path = f"pharmacie_{login.replace('@', '_at_').replace('.', '_')}.db"

    download_dir = os.path.join(os.getcwd(), "downloads")

    if choice == "1":
        run_client_keys_scraping(login, password, db_path)
    elif choice == "2":
        run_pdf_extraction(login, password, db_path)
    elif choice == "3":
        if len(sys.argv) != 5:
            print("Pour choice=3, spécifiez un client : python main.py 3 <login> <password> <client_name>")
            sys.stdout.flush()
            sys.exit(1)
        client_name = sys.argv[4]
        run_pdf_extraction(login, password, db_path, client_name)
    else:
        print("Option invalide. Veuillez choisir 1, 2 ou 3.")
        sys.stdout.flush()

    if os.path.exists(download_dir):
        try:
            shutil.rmtree(download_dir)
            print(f"Dossier {download_dir} supprimé après exécution du processus.")
            sys.stdout.flush()
        except Exception as e:
            print(f"Erreur lors de la suppression finale du dossier downloads : {e}")
            sys.stdout.flush()