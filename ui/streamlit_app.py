import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import streamlit as st
import pandas as pd
import sqlite3
import subprocess
import time
import datetime
from config.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_BUCKET
from core.s3_utils import download_from_s3
from core.scraper import PharmaScraper

def verify_credentials(login, password):
    scraper = PharmaScraper()
    try:
        scraper.access_site("https://app.pharma.sobrus.com/", login, password)
        return True
    except Exception as e:
        print(f"Erreur lors de la vérification : {e}")
        return False
    finally:
        scraper.cleanup()

def run_process(option, login, password, db_path, start_date, end_date, client_name=None):
    progress_bar = st.progress(0)
    progress_text = st.empty()
    cmd = ["python", "main.py", option, login, password]
    if client_name:
        cmd.append(client_name)
    cmd.extend([start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")])
    env = {
        **os.environ, "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID, "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
        "AWS_DEFAULT_REGION": AWS_REGION, "AWS_BUCKET": AWS_BUCKET, "PYTHONUNBUFFERED": "1"
    }
    process = subprocess.Popen(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, bufsize=1,
                               universal_newlines=True)
    stdout_output, stderr_output = [], []
    output_container = st.empty()
    for i in range(0, 101, 10):
        progress_bar.progress(i)
        progress_text.text(f"Avancement : {i}%")
        time.sleep(0.3)
    try:
        stdout, stderr = process.communicate(timeout=120)
        if stdout: stdout_output.append(stdout.strip())
        if stderr: stderr_output.append(stderr.strip())
    except subprocess.TimeoutExpired:
        process.kill()
        stdout_output.append("Processus tué après 120 secondes.")
        stderr_output.append("Timeout.")
        process.communicate()
    output_container.text("\n".join(stdout_output))
    if "process_logs" not in st.session_state:
        st.session_state.process_logs = []
    st.session_state.process_logs.append("\n".join(stdout_output))
    if stderr_output:
        st.session_state.process_logs.append("\n".join(stderr_output))
    return process.returncode == 0, "\n".join(stdout_output), "\n".join(stderr_output) if stderr_output else ""

def display_work_interface(login, password, db_path, s3_db_name):
    if "s3_downloaded" not in st.session_state:
        with st.spinner("Chargement depuis S3..."):
            download_from_s3(AWS_BUCKET, s3_db_name, db_path)
            st.session_state.s3_downloaded = True

    menu_option = st.sidebar.radio("Menu", ("Recherche des clients", "Ventes détaillées par client"))
    if st.sidebar.button("Déconnexion"):
        st.session_state.clear()
        st.rerun()

    if "process_logs" in st.session_state and st.session_state.process_logs:
        st.subheader("Logs du processus")
        for log in st.session_state.process_logs:
            st.text(log)

    conn = sqlite3.connect(db_path)
    if menu_option == "Recherche des clients":
        st.header("Recherche des clients")
        if st.button("Mettre à jour la liste des clients"):
            conn.close()
            success, stdout, stderr = run_process("1", login, password, db_path, datetime.date(2017, 1, 1),
                                                  datetime.date.today())
            if success:
                st.success("Mise à jour terminée.")
            else:
                st.error(f"Échec: {stderr}")

        df_keys = pd.read_sql_query("SELECT * FROM client_keys", conn)
        if df_keys.empty:
            st.warning("Table vide, lancement automatique...")
            conn.close()
            run_process("1", login, password, db_path, datetime.date(2017, 1, 1), datetime.date.today())
            conn = sqlite3.connect(db_path)
            df_keys = pd.read_sql_query("SELECT * FROM client_keys", conn)
        st.subheader("Liste des clés clients")
        st.dataframe(df_keys.style.set_properties(**{"text-align": "right"}), use_container_width=True)

    elif menu_option == "Ventes détaillées par client":
        st.header("Ventes détaillées par client")
        default_start = datetime.date(2017, 1, 1)
        default_end = datetime.date.today() - datetime.timedelta(days=1)
        start_date = st.date_input("Date de début", default_start, min_value=default_start,
                                   max_value=datetime.date.today(), key="detailed_start")
        end_date = st.date_input("Date de fin", default_end, min_value=start_date, max_value=datetime.date.today(),
                                 key="detailed_end")
        if start_date > end_date:
            st.error("La date de début doit être antérieure ou égale à la date de fin.")
            conn.close()
            return
        client_list = pd.read_sql_query("SELECT DISTINCT nom FROM client_keys", conn)["nom"].tolist()
        if client_list:
            selected_client = st.selectbox("Sélectionnez un client", client_list, key="detailed_client")

            if st.button(f"Mettre à jour les ventes détaillées pour {selected_client}"):
                conn.close()
                success, stdout, stderr = run_process("4", login, password, db_path, start_date, end_date,
                                                      selected_client)
                if success:
                    st.success(f"Mise à jour terminée pour {selected_client}.")
                    download_from_s3(AWS_BUCKET, s3_db_name, db_path)
                    st.rerun()
                else:
                    st.error(f"Échec: {stderr}")

            if st.button("Mettre à jour tous les clients"):
                with st.spinner("Mise à jour des ventes détaillées pour tous les clients en cours..."):
                    conn.close()
                    success, stdout, stderr = run_process("4", login, password, db_path, start_date, end_date,
                                                          client_name=None)
                    download_from_s3(AWS_BUCKET, s3_db_name, db_path)
                    conn = sqlite3.connect(db_path)  # ← Reconnexion nécessaire ici
                    if success:
                        st.success("Mise à jour terminée pour tous les clients.")
                        st.rerun()
                    else:
                        st.error(f"Échec: {stderr}")

            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='simple_transactions'")
            if cursor.fetchone():
                df_simple = pd.read_sql_query("SELECT * FROM simple_transactions WHERE nom = ?", conn,
                                              params=(selected_client,))
                if not df_simple.empty:
                    st.subheader(f"Mouvements pour {selected_client}")

                    solde_initial = df_simple.iloc[0]["solde"] - df_simple.iloc[0]["total"]
                    st.write(f"🔹 Solde initial (recalculé) : **{solde_initial:.2f}**")

                    libelles = df_simple["libelle"].str.lower()
                    total_ventes = df_simple[libelles.str.contains("vente") & ~libelles.str.contains("paiement") & ~libelles.str.contains("retour")]["total"].sum()
                    total_paiements = df_simple[libelles.str.contains("paiement")]["total"].sum()
                    total_avoirs = df_simple[libelles.str.contains("avoir")]["total"].sum()
                    total_retours = df_simple[libelles.str.contains("retour")]["total"].sum()

                    st.markdown(f"""
                    - 💰 **Total ventes :** {total_ventes:.2f}  
                    - 🔻 **Total paiements :** {total_paiements:.2f}  
                    - 🟢 **Total avoirs :** {total_avoirs:.2f}  
                    - 🔁 **Total retours :** {total_retours:.2f}
                    """)

                    st.dataframe(df_simple[["date", "reference", "libelle", "total", "solde"]].style.format({
                        "total": "{:.2f}", "solde": "{:.2f}"
                    }).set_properties(**{"text-align": "right"}), use_container_width=True)

                    df_solde = pd.read_sql_query("SELECT solde FROM solde_final WHERE nom=?", conn,
                                                 params=(selected_client,))
                    solde_final_calcule = df_simple.iloc[-1]["solde"]
                    if not df_solde.empty:
                        solde_final_pdf = float(df_solde.iloc[0]["solde"])
                        st.markdown(f"""✅ **Solde final (calculé)** : {solde_final_calcule:.2f}  
                        📄 **Solde final (PDF)** : {solde_final_pdf:.2f}""")

                    # Export CSV
                    st.download_button(
                        label="📁 Exporter en CSV",
                        data=df_simple.to_csv(index=False).encode("utf-8"),
                        file_name=f"{selected_client}_ventes.csv",
                        mime="text/csv"
                    )
        else:
            st.warning("Aucune liste de clients disponible. Mettez à jour via 'Recherche des clients'.")
    conn.close()


if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.title("Bienvenue")
    login = st.text_input("Login")
    password = st.text_input("Mot de passe", type="password")
    if st.button("Se connecter"):
        if login and password:
            with st.spinner("Vérification..."):
                print(f"Tentative d'authentification avec login: {login}")
                if verify_credentials(login, password):
                    print("Authentification réussie, mise à jour de l'état")
                    st.session_state.authenticated = True
                    st.session_state.login = login
                    st.session_state.password = password
                    st.session_state.db_path = f"pharmacie_{login.replace('@', '_at_').replace('.', '_')}.db"
                    st.session_state.s3_db_name = st.session_state.db_path
                    st.success("Connecté!")
                    print("Appel de st.rerun()")
                    st.rerun()
                else:
                    st.error("Identifiants incorrects.")
        else:
            st.error("Veuillez remplir les champs.")
else:
    print("Utilisateur authentifié, affichage de l'interface")
    display_work_interface(st.session_state.login, st.session_state.password, st.session_state.db_path,
                          st.session_state.s3_db_name)
