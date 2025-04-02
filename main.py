import os
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
region = os.getenv("AWS_DEFAULT_REGION") or "eu-north-1"

# Vérification des identifiants AWS
if not aws_access_key_id or not aws_secret_access_key:
    raise ValueError(
        "Les clés AWS_ACCESS_KEY_ID et AWS_SECRET_ACCESS_KEY doivent être définies dans l’environnement ou un fichier .env"
    )
print(f"Clés chargées : {aws_access_key_id[:8]}... (region: {region})")

# --- Configuration S3 ---
s3_client = boto3.client(
    "s3",
    region_name=region,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)
bucket_name = "jujul"  # Remplacez par le nom de votre bucket S3

# Fonction pour uploader vers S3 (déplacée ici pour être globale)
def upload_to_s3(local_file, bucket_name, s3_file):
    try:
        print(
            f"Paramètres : local_file={repr(local_file)}, bucket_name={repr(bucket_name)}, s3_file={repr(s3_file)}"
        )
        if not os.path.exists(local_file):
            raise FileNotFoundError(f"Le fichier {local_file} n'existe pas")
        print(
            f"Fichier {local_file} trouvé, taille : {os.path.getsize(local_file)} bytes"
        )
        s3_client.upload_file(local_file, bucket_name, s3_file)
        print(
            f"Base de données {local_file} uploadée avec succès vers S3://{bucket_name}/{s3_file}"
        )
    except Exception as e:
        print(f"Erreur lors de l'upload vers S3 : {e}")
        raise


class PharmaScraper:
    def __init__(self, driver, wait, download_dir):
        self.driver = driver
        self.wait = wait
        self.download_dir = download_dir

    def access_site(self, url, usern, password):
        """Accède au site et réalise l'authentification."""
        print(f"Tentative d'accès au site : {url}")
        self.driver.get(url)
        try:
            button_login = self.wait.until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, 'button[data-testid="s\'identifier"]')
                ),
                "Bouton 'S'identifier' non trouvé"
            )
            print("Bouton 'S'identifier' trouvé, clic en cours...")
            button_login.click()
        except Exception as e:
            print(f"Erreur lors de la recherche du bouton de login : {e}")
            raise

        try:
            login_input = self.wait.until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "input[name='login']")),
                "Champ login non trouvé"
            )
            print("Champ login trouvé, saisie de l'utilisateur...")
            login_input.send_keys(usern)
            login_validation_button = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn-signup")),
                "Bouton de validation du login non trouvé"
            )
            login_validation_button.click()
        except Exception as e:
            print(f"Erreur lors de la saisie du login : {e}")
            raise

        try:
            # Augmenter le timeout à 30 secondes et tester un fallback
            password_input = WebDriverWait(self.driver, 30).until(
                lambda driver: driver.find_element(By.CSS_SELECTOR, "input[name='password']") or
                               driver.find_element(By.CSS_SELECTOR, "input[type='password']"),
                "Champ mot de passe non trouvé"
            )
            print("Champ mot de passe trouvé, saisie du mot de passe...")
            password_input.send_keys(password)
            login_validation_button = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn-signup")),
                "Bouton de validation du mot de passe non trouvé"
            )
            login_validation_button.click()
            print("Authentification soumise, attente de redirection...")
        except Exception as e:
            print(f"Erreur lors de la saisie du mot de passe : {e}")
            raise

    def get_clients_from_page(self):
        """Récupère la liste des clients depuis le tableau de la page."""
        # Attendre que le tableau soit visible et stable
        self.wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table.sob-v2-table tbody tr"))
        )
        # Ajouter une attente supplémentaire pour s'assurer que le contenu est chargé
        self.wait.until(
            lambda driver: len(driver.find_elements(By.CSS_SELECTOR, "table.sob-v2-table tbody tr")) > 0
                           and driver.find_elements(By.CSS_SELECTOR, "table.sob-v2-table tbody tr")[0].is_displayed(),
            "Le tableau est présent mais pas encore complètement chargé"
        )

        clients = []
        # Ne pas stocker une liste statique, mais compter les lignes dynamiquement
        row_count = len(self.driver.find_elements(By.CSS_SELECTOR, "table.sob-v2-table tbody tr"))
        for i in range(row_count):
            retries = 3  # Nombre de tentatives en cas d'erreur
            while retries > 0:
                try:
                    # Relocaliser les lignes à chaque itération
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
                    break  # Sortir de la boucle de retries si succès
                except StaleElementReferenceException as e:
                    retries -= 1
                    print(f"Stale element détecté, tentative restante : {retries}")
                    time.sleep(1)  # Petite pause avant de retenter
                    if retries == 0:
                        print(f"Échec après retries pour la ligne {i} : {e}")
                        break
                except Exception as e:
                    print(f"Erreur inattendue pour la ligne {i} : {e}")
                    break
        return clients

    def access_site_and_get_clients(self, url, usern, password):
        """Authentification puis navigation vers la page des clients."""
        self.access_site(url, usern, password)
        print("Authentification réussie, passage à la page des clients...")
        time.sleep(2)
        self.driver.get("https://app.pharma.sobrus.com/customers")
        print("Page des clients chargée.")
        return self.get_clients_from_page()

    def wait_for_download(self, timeout=30):
        """Attend qu'un fichier PDF apparaisse dans le dossier de téléchargement."""
        end_time = time.time() + timeout
        while time.time() < end_time:
            files = [f for f in os.listdir(self.download_dir) if f.endswith(".pdf")]
            if files:
                return os.path.join(self.download_dir, files[0])
            time.sleep(1)
        raise Exception("Téléchargement du PDF non terminé.")

    def merge_thousands(self, ligne):
        """Fusionne les séparateurs de milliers dans une ligne."""
        def repl(match):
            group1 = match.group(1)
            group2 = match.group(2)
            if len(group1) > 1:
                return group1 + group2
            else:
                return group1 + " " + group2
        return re.sub(r"(\d)\s+(\d{3},\d+)", repl, ligne)

    def extract_data_from_pdf(self, pdf_file, client):
        records = []
        solde_final = None
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue
                lines = text.split("\n")
                print(f"Lignes brutes extraites du PDF : {lines}")
                processed_lines = []
                buffer_line = ""

                # Étape 1 : Fusion intelligente des lignes
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    line = self.merge_thousands(line)
                    # Nouvelle entrée si date détectée
                    if re.match(r"^\d{4}-\d{2}-\d{2}", line):
                        if buffer_line:
                            processed_lines.append(buffer_line)
                        buffer_line = line
                    # Solde final
                    elif line.lower().startswith("solde final"):
                        if buffer_line:
                            processed_lines.append(buffer_line)
                        buffer_line = line
                    # Continuation d'une ligne
                    else:
                        # Si la ligne contient seulement des nombres et suit une description, la rattacher
                        if re.match(r"^-?\d+(?:\s+-?\d+(?:[.,]\d+)?){1,2}$", line) and buffer_line and not re.match(
                                r"^\d{4}-\d{2}-\d{2}", buffer_line):
                            buffer_line += " " + line
                        # Sinon, traiter comme continuation ou nouvelle ligne si elle a une date précédente
                        elif buffer_line and re.match(r"^\d{4}-\d{2}-\d{2}", buffer_line):
                            processed_lines.append(buffer_line)
                            buffer_line = line
                        else:
                            buffer_line += " " + line
                if buffer_line:
                    processed_lines.append(buffer_line)
                print(f"Lignes traitées après fusion : {processed_lines}")

                # Étape 2 : Parsing des lignes (inchangé sauf logs)
                for line in processed_lines:
                    print(f"Traitement de la ligne : {line}")
                    tokens = line.split()
                    if line.lower().startswith("solde final"):
                        try:
                            num_part = "".join(tokens[2:]).replace(" ", "")
                            solde_final = float(num_part.replace(",", "."))
                            print(f"Solde final extrait : {solde_final}")
                        except Exception as e:
                            print(f"Erreur d'extraction du solde final : {e}")
                        continue
                    if not re.match(r"^\d{4}-\d{2}-\d{2}", line):
                        print(f"Ligne ignorée (pas de date valide) : {line}")
                        continue
                    try:
                        date_obj = datetime.datetime.strptime(tokens[0], "%Y-%m-%d")
                    except Exception as e:
                        print(f"Erreur de conversion de date : {e}")
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
                        except Exception as e:
                            print(f"Erreur de parsing des nombres : {e}")
                            continue
                    elif len(numeric_tokens) == 1:  # Paiement ou autre
                        try:
                            total_val = float(numeric_tokens[0].replace(" ", "").replace(",", "."))
                            produit = " ".join(tokens[1:len(tokens) - len(numeric_tokens[0].split())]).replace(" - -",
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
                                f"Paiement - Produit: {produit}, Total: {total_val}, Débit: {debit}, Créd/servlet: {credit}")
                            quantite = None
                            prix = None
                        except Exception as e:
                            print(f"Erreur de parsing du total : {e}")
                            continue
                    else:
                        print(f"Format inattendu dans la ligne: {line}")
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
                    records.append(record)
        return records, solde_final

    def retrieve_client_key(self, client):
        time.sleep(2)
        client_xpath = f'//table[contains(@class, "sob-v2-table")]//tbody/tr[th/span[normalize-space()="{client["nom"]}"]]'
        client_row = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, client_xpath))
        )
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
        self.driver.get("https://app.pharma.sobrus.com/customers")
        self.wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table.sob-v2-table"))
        )
        time.sleep(2)
        return client_key

    def download_pdf_api(self, client):
        end_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        #end_date="2022-03-31"
        url = f"https://api.pharma.sobrus.com/customers/export-customer-statement?type=simple&start_date=2020-01-01&end_date={end_date}&customer_id={client['client_id']}"
        print(f"Téléchargement du PDF pour {client['nom']} via l'URL: {url}")
        self.driver.get(url)
        pdf_file = self.wait_for_download()
        return pdf_file

    def go_to_next_page(self):
        try:
            pagination = self.wait.until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "div.sob-v2-table-pagination")
                )
            )
            current_page_element = pagination.find_element(
                By.CSS_SELECTOR, "span.sob-v2-TablePage"
            )
            self.wait.until(lambda d: current_page_element.text.strip() != "")
            current_page = int(current_page_element.text.strip() or "0")
            next_button = pagination.find_element(
                By.XPATH,
                ".//span[contains(@class, 'sob-v2-TablePage')]/following-sibling::button[1]",
            )
            if "sob-v2-TablePage__disabled" in next_button.get_attribute("class"):
                print("Le bouton 'Suivant' est désactivé.")
                return False
            self.driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);"
            )
            time.sleep(0.5)
            self.driver.execute_script(
                "arguments[0].scrollIntoView(true);", next_button
            )
            time.sleep(0.5)
            self.wait.until(EC.visibility_of(next_button))
            try:
                next_button.click()
            except Exception as e:
                print("Clic standard échoué, tentative via JavaScript:", e)
                self.driver.execute_script("arguments[0].click();", next_button)
            self.wait.until(
                lambda d: int(
                    d.find_element(
                        By.CSS_SELECTOR,
                        "div.sob-v2-table-pagination span.sob-v2-TablePage",
                    ).text.strip()
                    or "0"
                )
                > current_page
            )
            return True
        except Exception as e:
            print("Erreur lors de la navigation vers la page suivante :", e)
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
        "safebrowsing.enabled": True,
        "profile.managed_default_content_settings.images": 2,
    }
    options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 20)

    scraper = PharmaScraper(driver, wait, download_dir)
    clients = scraper.access_site_and_get_clients(
        "https://app.pharma.sobrus.com/", login, password
    )
    print("Nombre de clients récupérés sur la première page :", len(clients))
    client_keys_list = []
    while True:
        print(f"Clients sur la page actuelle : {[c['nom'] for c in clients]}")
        for client in clients:
            client_key = scraper.retrieve_client_key(client)
            if client_key and (client["nom"], client_key) not in client_keys_list:
                client_keys_list.append((client["nom"], client_key))
        if scraper.go_to_next_page():
            time.sleep(2)
            clients = scraper.get_clients_from_page()
            print("Nombre de clients récupérés sur la nouvelle page :", len(clients))
        else:
            break
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS client_keys")
    conn.commit()
    cursor.execute(
        """
    CREATE TABLE client_keys (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nom TEXT,
        client_key TEXT
    )
    """
    )
    for nom, client_key in client_keys_list:
        cursor.execute(
            "INSERT INTO client_keys (nom, client_key) VALUES (?, ?)", (nom, client_key)
        )
    conn.commit()
    print("Contenu de la table client_keys :")
    cursor.execute("SELECT * FROM client_keys")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    conn.close()
    driver.quit()


def run_pdf_extraction(login, password, db_path):
    download_dir = os.path.join(os.getcwd(), "downloads")
    if os.path.exists(download_dir):
        try:
            shutil.rmtree(download_dir)
            print(f"Dossier {download_dir} supprimé avec succès.")
        except Exception as e:
            print(f"Erreur lors de la suppression initiale du dossier downloads : {e}")
    os.makedirs(download_dir)
    print(f"Dossier {download_dir} créé.")

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
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS num_transactions")
    cursor.execute("DROP VIEW IF EXISTS vue_debiteurs")
    cursor.execute("DROP TABLE IF EXISTS solde_final")
    conn.commit()
    conn.close()

    try:
        _ = scraper.access_site_and_get_clients(
            "https://app.pharma.sobrus.com/", login, password
        )
    except Exception as e:
        print(f"Erreur lors de l’authentification ou de l’accès aux clients : {e}")
        driver.quit()
        raise

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT nom, client_key FROM client_keys")
    client_keys_list = cursor.fetchall()
    conn.close()

    if not client_keys_list:
        print(
            "Aucune clé client trouvée en base. Veuillez exécuter le choix 1 au préalable."
        )
        driver.quit()
        exit()

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
            except Exception as e:
                print(f"Erreur lors de la suppression du fichier PDF {pdf_file} : {e}")
        except Exception as e:
            print(f"Erreur lors du téléchargement du PDF pour {nom} : {e}")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
    CREATE TABLE num_transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nom TEXT,
        date DATE,
        produit TEXT,
        quantite INTEGER,
        debit REAL,
        credit REAL
    )
    """
    )
    conn.commit()

    for record in all_records:
        cursor.execute(
            """
        INSERT INTO num_transactions (nom, date, produit, quantite, debit, credit)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
            (
                record.get("nom"),
                record["date"].strftime("%Y-%m-%d"),
                record.get("produit"),
                record.get("quantite"),
                record.get("debit"),
                record.get("credit"),
            ),
        )
        print(f"Inserted into num_transactions: {record}")

    cursor.execute(
        """
    CREATE VIEW vue_debiteurs AS
    SELECT nom,
           strftime('%Y-%m', date) as mois,
           ROUND(SUM(COALESCE(debit, 0)), 2) as total_debit,
           ROUND(SUM(COALESCE(credit, 0)), 2) as total_credit,
           ROUND(SUM(COALESCE(credit, 0)) + SUM(COALESCE(debit, 0)), 2) as solde
    FROM num_transactions
    GROUP BY nom, mois
    """
    )
    conn.commit()

    cursor.execute("DROP TABLE IF EXISTS solde_final")
    cursor.execute("CREATE TABLE solde_final (nom TEXT, solde_final REAL)")
    for nom, solde in solde_final_dict.items():
        cursor.execute(
            "INSERT INTO solde_final (nom, solde_final) VALUES (?, ?)", (nom, solde)
        )
    conn.commit()
    conn.close()

    upload_to_s3(db_path, bucket_name, os.path.basename(db_path))
    print("Script terminé avec succès!")
    driver.quit()


def run_single_client_pdf_extraction(login, password, db_path, client_name):
    print(f"Début de run_single_client_pdf_extraction pour {client_name}")
    download_dir = os.path.join(os.getcwd(), "downloads")
    print(f"Vérification du dossier downloads : {download_dir}")
    if os.path.exists(download_dir):
        try:
            shutil.rmtree(download_dir)
            print(f"Dossier {download_dir} supprimé avec succès.")
        except Exception as e:
            print(f"Erreur lors de la suppression du dossier downloads : {e}")
    os.makedirs(download_dir)
    print(f"Dossier {download_dir} créé.")

    print("Configuration des options de Chrome...")
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

    print("Initialisation du driver Chrome...")
    try:
        driver = webdriver.Chrome(options=options)
    except Exception as e:
        print(f"Erreur lors de l’initialisation du driver Chrome : {e}")
        return
    wait = WebDriverWait(driver, 20)
    scraper = PharmaScraper(driver, wait, download_dir)

    print(f"Connexion à la base de données : {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT client_key FROM client_keys WHERE nom = ?", (client_name,))
    result = cursor.fetchone()
    if not result:
        print(f"Aucun client trouvé avec le nom {client_name} dans client_keys.")
        conn.close()
        driver.quit()
        return
    client_key = result[0]
    conn.close()
    print(f"Clé client trouvée pour {client_name} : {client_key}")

    # Authentification avec gestion d’erreur
    print("Tentative d’authentification et accès aux clients...")
    try:
        _ = scraper.access_site_and_get_clients("https://app.pharma.sobrus.com/", login, password)
        print("Authentification réussie et liste des clients récupérée.")
    except Exception as e:
        print(f"Erreur lors de l’authentification ou de l’accès aux clients : {e}")
        driver.quit()
        raise

    client = {"nom": client_name, "client_id": client_key}
    print(f"Téléchargement du PDF pour {client_name}...")
    try:
        pdf_file = scraper.download_pdf_api(client)
        print(f"PDF téléchargé avec succès : {pdf_file}")
    except Exception as e:
        print(f"Échec du téléchargement : {e}")
        driver.quit()
        raise

    print(f"Extraction des données du PDF pour {client_name}...")
    try:
        data, solde_final = scraper.extract_data_from_pdf(pdf_file, client)
        print(f"Extraction réussie : {len(data)} lignes, solde final = {solde_final}")
    except Exception as e:
        print(f"Échec de l’extraction : {e}")
        driver.quit()
        raise

    try:
        os.remove(pdf_file)
        print(f"PDF {pdf_file} supprimé.")
    except Exception as e:
        print(f"Erreur lors de la suppression du fichier PDF {pdf_file} : {e}")

    print(f"Mise à jour de la base de données pour {client_name}...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM num_transactions WHERE nom = ?", (client_name,))
    cursor.execute("DELETE FROM solde_final WHERE nom = ?", (client_name,))

    for record in data:
        cursor.execute(
            """
            INSERT INTO num_transactions (nom, date, produit, quantite, debit, credit)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                record.get("nom"),
                record["date"].strftime("%Y-%m-%d"),
                record.get("produit"),
                record.get("quantite"),
                record.get("debit"),
                record.get("credit"),
            ),
        )
        print(f"Insertion BDD : {record}")

    if solde_final is not None:
        cursor.execute(
            "INSERT INTO solde_final (nom, solde_final) VALUES (?, ?)",
            (client_name, solde_final),
        )
        print(f"Inserted into solde_final: nom={client_name}, solde_final={solde_final}")

    conn.commit()
    conn.close()
    print("Base de données mise à jour et commit effectué.")

    print(f"Upload de la base vers S3 : {db_path}")
    upload_to_s3(db_path, bucket_name, os.path.basename(db_path))
    print(f"Mise à jour réussie pour le client {client_name}")
    driver.quit()
    print("Driver Chrome fermé.")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 4:
        print("Usage: python main.py <choice> <login> <password> [<client_name>]")
        sys.exit(1)

    choice = sys.argv[1]
    login = sys.argv[2]
    password = sys.argv[3]
    db_path = f"pharmacie_{login.replace('@', '_at_').replace('.', '_')}.db"  # Nom unique basé sur le login

    if choice == "1":
        run_client_keys_scraping(login, password, db_path)
    elif choice == "2":
        run_pdf_extraction(login, password, db_path)
    elif choice == "3":  # Nouvelle option pour un seul client
        if len(sys.argv) != 5:
            print("Pour choice=3, spécifiez un client : python main.py 3 <login> <password> <client_name>")
            sys.exit(1)
        client_name = sys.argv[4]
        run_single_client_pdf_extraction(login, password, db_path, client_name)
    else:
        print("Option invalide. Veuillez choisir 1, 2 ou 3.")