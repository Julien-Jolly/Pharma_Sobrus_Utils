import os
from dotenv import load_dotenv
import streamlit as st
import sqlite3
import pandas as pd
import boto3
import subprocess
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# Charger les variables d’environnement depuis .env
load_dotenv()

# Récupération des identifiants AWS depuis st.secrets ou os.getenv
if "aws" in st.secrets:
    aws_access_key_id = st.secrets["aws"]["aws_access_key_id"]
    aws_secret_access_key = st.secrets["aws"]["aws_secret_access_key"]
    region = st.secrets["aws"]["aws_default_region"]
else:
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_DEFAULT_REGION") or "eu-north-1"

if not aws_access_key_id or not aws_secret_access_key:
    st.error("Les identifiants AWS ne sont pas configurés correctement.")
    st.stop()

# Client S3 global
s3_client = boto3.client(
    "s3",
    region_name=region,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)
bucket_name = "jujul"


def download_from_s3(bucket_name, s3_file, local_file):
    try:
        s3_client.download_file(bucket_name, s3_file, local_file)
        return True
    except Exception as e:
        st.write(
            f"Aucune base existante trouvée sur S3 pour {s3_file}, une nouvelle sera créée."
        )
        return False


def verify_credentials(login, password):
    """Tente une authentification sur le site pour valider les identifiants, optimisée pour la vitesse."""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option(
        "prefs", {"profile.managed_default_content_settings.images": 2}
    )

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 10)

    try:
        print("Étape 1 : Accès à la page de login")
        driver.get("https://app.pharma.sobrus.com/")
        button_login = wait.until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, 'button[data-testid="s\'identifier"]')
            )
        )
        button_login.click()

        print("Étape 2 : Saisie du login")
        login_input = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "input[name='login']"))
        )
        login_input.send_keys(login)
        login_validation_button = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn-signup"))
        )
        login_validation_button.click()

        print("Étape 3 : Saisie du mot de passe")
        password_input = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "input[name='password']"))
        )
        password_input.send_keys(password)
        login_validation_button = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn-signup"))
        )
        login_validation_button.click()

        print("Étape 4 : Vérification de l’authentification")
        wait.until(
            lambda driver: driver.current_url.startswith(
                "https://app.pharma.sobrus.com/"
            )
        )
        print(f"Authentification réussie, URL actuelle : {driver.current_url}")
        driver.quit()
        return True

    except TimeoutException as e:
        try:
            password_input = driver.find_element(
                By.CSS_SELECTOR, "input[name='password']"
            )
            if password_input.is_displayed():
                print(
                    "Échec de l’authentification : champ mot de passe toujours visible"
                )
            else:
                print(f"Timeout mais URL actuelle : {driver.current_url}")
        except:
            print(f"Timeout inattendu : {e}")
        driver.quit()
        return False
    except Exception as e:
        print(f"Erreur inattendue lors de la vérification : {e}")
        driver.quit()
        return False


# Fonction pour afficher l’interface de travail
def display_work_interface(login, password, db_path, s3_db_name):
    local_db = db_path

    # Téléchargement de la base S3 au premier accès
    if "s3_downloaded" not in st.session_state:
        with st.spinner("Chargement des données depuis S3..."):
            download_from_s3(bucket_name, s3_db_name, db_path)
            st.session_state.s3_downloaded = True

    # Transformation pour la vue détaillée (avec solde cumulé)
    def add_totals_and_spacing(df):
        df["solde"] = pd.to_numeric(df["solde"], errors="coerce").fillna(0)
        df["total_debit"] = pd.to_numeric(df["total_debit"], errors="coerce").fillna(0)
        df["total_credit"] = pd.to_numeric(df["total_credit"], errors="coerce").fillna(
            0
        )
        df = df.sort_values(["nom", "mois"])
        new_rows = []
        for client, group in df.groupby("nom", sort=False):
            group = group.copy()
            group["solde_cumule"] = group["solde"].cumsum()
            new_rows.append(group)
            total_debit = group["total_debit"].sum()
            total_credit = group["total_credit"].sum()
            solde_global = total_credit + total_debit
            solde_cumule_final = (
                group["solde_cumule"].iloc[-1] if not group.empty else 0
            )
            total_row = pd.DataFrame(
                {
                    "nom": [client],
                    "mois": ["Total"],
                    "total_debit": [total_debit],
                    "total_credit": [total_credit],
                    "solde": [solde_global],
                    "solde_cumule": [solde_cumule_final],
                }
            )
            new_rows.append(total_row)
            empty_row = pd.DataFrame(
                {
                    "nom": [""],
                    "mois": [""],
                    "total_debit": [None],
                    "total_credit": [None],
                    "solde": [None],
                    "solde_cumule": [None],
                }
            )
            new_rows.append(empty_row)
        return pd.concat(new_rows, ignore_index=True)

    def group_by_client(df):
        df["total_debit"] = pd.to_numeric(df["total_debit"], errors="coerce").fillna(0)
        df["total_credit"] = pd.to_numeric(df["total_credit"], errors="coerce").fillna(
            0
        )
        df["solde"] = pd.to_numeric(df["solde"], errors="coerce").fillna(0)
        df_grouped = df.groupby("nom", as_index=False).agg(
            {"total_debit": "sum", "total_credit": "sum", "solde": "sum"}
        )
        return df_grouped

    def highlight_totals(row):
        if row["mois"] == "Total":
            return ["font-weight: bold"] * len(row)
        else:
            return [""] * len(row)

    # Menu avec déconnexion dans la barre latérale
    menu_option = st.sidebar.radio(
        "Menu", ("Recherche des clients", "Débit/Credit par mois des clients")
    )
    if st.sidebar.button("Déconnexion"):
        st.session_state.clear()
        st.rerun()

    def run_process(option, login, password, db_path):
        progress_bar = st.progress(0)
        progress_text = st.empty()
        with st.spinner("Processus en cours, veuillez patienter..."):
            for percent in range (0, 101, 10):
                progress_bar.progress(percent)
                progress_text.text(f"Avancement : {percent}%")
                time.sleep(0.3)
            env = {
                **os.environ,
                "AWS_ACCESS_KEY_ID": aws_access_key_id,
                "AWS_SECRET_ACCESS_KEY": aws_secret_access_key,
                "AWS_DEFAULT_REGION": region,
            }
            result = subprocess.run(
                ["python", "main.py", option, login, password],
                text=True,
                capture_output=True,
                env=env,
            )
        if result.returncode == 0:
            st.success("Processus terminé.")
        else:
            st.error(f"Processus échoué avec code {result.returncode}")
        st.text(result.stdout)
        if result.stderr:
            st.error(f"Erreurs détaillées : {result.stderr}")

    if menu_option == "Recherche des clients":
        st.header("Recherche des clients")
        if st.button("Mettre à jour la liste des clients"):
            run_process("1", login, password, db_path)
        try:
            conn = sqlite3.connect(local_db)
            df_keys = pd.read_sql_query("SELECT * FROM client_keys", conn)
            conn.close()
        except Exception as e:
            st.error(f"Erreur lors de la récupération des clés clients : {e}")
            df_keys = pd.DataFrame()
        if df_keys.empty:
            st.warning(
                "La table des clés clients est vide. Lancement automatique du processus de récupération..."
            )
            run_process("1", login, password, db_path)
            conn = sqlite3.connect(local_db)
            df_keys = pd.read_sql_query("SELECT * FROM client_keys", conn)
            conn.close()
        st.subheader("Liste actuelle des clés clients")
        st.dataframe(
            df_keys.style.set_properties(**{"text-align": "right"}),
            use_container_width=True,
        )

    elif menu_option == "Débit/Credit par mois des clients":
        st.header("Débit/Credit par mois des clients")
        if st.button("Mettre à jour les données de débit/credit"):
            run_process("2", login, password, db_path)
        try:
            conn = sqlite3.connect(local_db)
            df_debiteurs = pd.read_sql_query("SELECT * FROM vue_debiteurs", conn)
            conn.close()
        except Exception as e:
            st.error(
                f"Erreur lors de la récupération des données de débit/credit : {e}"
            )
            df_debiteurs = pd.DataFrame()
        expected_cols = {"total_debit", "total_credit", "solde"}
        if df_debiteurs.empty or not expected_cols.issubset(set(df_debiteurs.columns)):
            st.warning(
                "Aucune donnée de débit/credit n'a été trouvée. Lancement automatique du processus de récupération..."
            )
            run_process("2", login, password, db_path)
            conn = sqlite3.connect(local_db)
            df_debiteurs = pd.read_sql_query("SELECT * FROM vue_debiteurs", conn)
            conn.close()
        if not expected_cols.issubset(set(df_debiteurs.columns)):
            st.error(
                "Les colonnes attendues (total_debit, total_credit, solde) sont introuvables dans les données récupérées."
            )
        else:
            tab1, tab2 = st.tabs(
                ["Vue globale par client", "Vue détaillée par client et par mois"]
            )

            with tab1:
                st.subheader("Vue globale par client")
                df_grouped = group_by_client(df_debiteurs)
                styled_grouped = df_grouped.style.format(
                    {
                        "total_debit": "{:.2f}",
                        "total_credit": "{:.2f}",
                        "solde": "{:.2f}",
                    }
                ).set_properties(**{"text-align": "right"})
                st.dataframe(styled_grouped, use_container_width=True)

            with tab2:
                st.subheader("Vue détaillée par client et par mois (avec solde cumulé)")
                df_detailed = add_totals_and_spacing(df_debiteurs)
                styled_detailed = df_detailed.style.format(
                    {
                        "total_debit": "{:.2f}",
                        "total_credit": "{:.2f}",
                        "solde": "{:.2f}",
                        "solde_cumule": "{:.2f}",
                    }
                ).set_properties(**{"text-align": "right"})
                st.dataframe(styled_detailed, use_container_width=True)

            st.markdown("---")
            st.subheader("Filtrer par client")
            try:
                conn = sqlite3.connect(local_db)
                df_clients = pd.read_sql_query(
                    "SELECT DISTINCT nom FROM vue_debiteurs", conn
                )
                conn.close()
                client_list = df_clients["nom"].tolist()
            except Exception as e:
                st.error(
                    f"Erreur lors de la récupération de la liste des clients : {e}"
                )
                client_list = []
            if client_list:
                selected_client = st.selectbox(
                    "Sélectionnez un client", ["Tous"] + client_list
                )
                if selected_client != "Tous":
                    filtered_df = df_debiteurs[df_debiteurs["nom"] == selected_client]
                    try:
                        total_debit = filtered_df["total_debit"].astype(float).sum()
                        total_credit = filtered_df["total_credit"].astype(float).sum()
                    except Exception:
                        total_debit = 0
                        total_credit = 0
                    solde_global = total_credit + total_debit
                    filtered_df = filtered_df.sort_values("mois")
                    filtered_df["solde_cumule"] = pd.to_numeric(
                        filtered_df["solde"], errors="coerce"
                    ).cumsum()
                    total_row = pd.DataFrame(
                        {
                            "nom": [selected_client],
                            "mois": ["Total"],
                            "total_debit": [total_debit],
                            "total_credit": [total_credit],
                            "solde": [solde_global],
                            "solde_cumule": [filtered_df["solde_cumule"].iloc[-1]],
                        }
                    )
                    filtered_df = pd.concat([filtered_df, total_row], ignore_index=True)
                    styled_filtered = filtered_df.style.format(
                        {
                            "total_debit": "{:.2f}",
                            "total_credit": "{:.2f}",
                            "solde": "{:.2f}",
                            "solde_cumule": "{:.2f}",
                        }
                    ).set_properties(**{"text-align": "right"})
                    st.dataframe(styled_filtered, use_container_width=True)

                    try:
                        conn = sqlite3.connect(local_db)
                        df_solde = pd.read_sql_query(
                            "SELECT solde_final FROM solde_final WHERE nom=?",
                            conn,
                            params=(selected_client,),
                        )
                        conn.close()
                        if not df_solde.empty:
                            final_solde = float(df_solde.iloc[0]["solde_final"])
                            st.write(
                                f"Solde final pour {selected_client} : {final_solde:.2f}"
                            )
                        else:
                            st.write("Aucun solde final extrait pour ce client.")
                    except Exception as e:
                        st.error(f"Erreur lors de la récupération du solde final : {e}")

                    # Ajout du bouton "Mettre à jour ce client"
                    if st.button(f"Mettre à jour ce client ({selected_client})"):
                        with st.spinner(f"Mise à jour des données pour {selected_client} en cours..."):
                            env = {
                                **os.environ,
                                "AWS_ACCESS_KEY_ID": aws_access_key_id,
                                "AWS_SECRET_ACCESS_KEY": aws_secret_access_key,
                                "AWS_DEFAULT_REGION": region,
                            }
                            process = subprocess.Popen(
                                ["python", "main.py", "3", login, password, selected_client],
                                text=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                env=env,
                                bufsize=1,  # Ligne par ligne
                                universal_newlines=True
                            )
                            stdout_output = []
                            stderr_output = []
                            output_container = st.empty()  # Conteneur pour mettre à jour la sortie

                            # Lire la sortie en temps réel
                            while process.poll() is None:
                                stdout_line = process.stdout.readline()
                                if stdout_line:
                                    stdout_output.append(stdout_line.strip())
                                    output_container.text("\n".join(stdout_output))  # Mettre à jour la sortie
                                stderr_line = process.stderr.readline()
                                if stderr_line:
                                    stderr_output.append(stderr_line.strip())
                                    st.error(stderr_line.strip())  # Afficher les erreurs immédiatement

                            # Capturer le reste après la fin du processus
                            stdout_remainder, stderr_remainder = process.communicate()
                            if stdout_remainder:
                                stdout_output.append(stdout_remainder.strip())
                            if stderr_remainder:
                                stderr_output.append(stderr_remainder.strip())

                            # Mettre à jour l’affichage final
                            output_container.text("\n".join(stdout_output))
                            if stderr_output:
                                st.error(f"Erreurs détaillées : {'\n'.join(stderr_output)}")

                            if process.returncode == 0:
                                st.success(f"Mise à jour terminée pour {selected_client}.")
                                download_from_s3(bucket_name, s3_db_name, local_db)
                                st.rerun()
                            else:
                                st.error(f"Échec de la mise à jour pour {selected_client} : {'\n'.join(stderr_output)}")
                            if stderr_output:
                                st.error(f"Erreurs détaillées : {' '.join(stderr_output)}")
                else:
                    st.write("Affichage de tous les clients.")
            else:
                st.warning(
                    "La liste des clients est vide. Veuillez d'abord mettre à jour la liste via le menu 'Recherche des clients'."
                )


# Logique principale
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

# Afficher la page de login uniquement si non authentifié
if not st.session_state.authenticated:
    st.title("Bienvenue")
    login = st.text_input("Login")
    password = st.text_input("Mot de passe", type="password")
    if st.button("Se connecter"):
        if login and password:
            with st.spinner("Vérification des identifiants..."):
                if verify_credentials(login, password):
                    st.session_state.authenticated = True
                    st.session_state.login = login
                    st.session_state.password = password
                    db_filename = (
                        f"pharmacie_{login.replace('@', '_at_').replace('.', '_')}.db"
                    )
                    st.session_state.db_path = db_filename
                    st.session_state.s3_db_name = db_filename
                    st.success("Connecté avec succès!")
                    st.rerun()  # Forcer un rechargement pour passer à l’interface
                else:
                    st.error(
                        "Identifiants incorrects. Veuillez vérifier votre login ou mot de passe."
                    )
        else:
            st.error("Veuillez saisir les deux champs.")
# Si authentifié, afficher uniquement l’interface de travail
else:
    display_work_interface(
        st.session_state.login,
        st.session_state.password,
        st.session_state.db_path,
        st.session_state.s3_db_name,
    )