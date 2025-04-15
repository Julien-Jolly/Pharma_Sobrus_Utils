import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import streamlit as st
import pandas as pd
import sqlite3
import subprocess
import time
import datetime
import queue
import threading
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

import os
import subprocess
import time
import streamlit as st

def run_process(option, login, password, db_path, start_date, end_date, client_name=None):
    progress_bar = st.progress(0)
    progress_text = st.empty()
    # Calculer la racine du projet
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, ".."))
    if not os.path.exists(os.path.join(project_root, "main.py")):
        project_root = os.path.dirname(os.path.dirname(sys.executable))
    # Chemins
    python_exe = os.path.join(project_root, ".venv", "Scripts", "python.exe")
    main_py = os.path.join(project_root, "main.py")
    # Diagnostics
    st.write(f"DEBUG: script_dir = {script_dir}")
    st.write(f"DEBUG: project_root = {project_root}")
    st.write(f"DEBUG: python_exe = {python_exe}")
    st.write(f"DEBUG: main_py = {main_py}")
    st.write(f"DEBUG: sys.executable = {sys.executable}")
    if not os.path.exists(python_exe):
        python_exe = sys.executable
        st.warning(f"Chemin .venv non trouvé, utilisation de sys.executable : {python_exe}")
    if not os.path.exists(python_exe):
        st.error(f"Interpréteur Python non trouvé : {python_exe}")
        return False, "", "Erreur : Interpréteur Python manquant"
    if not os.path.exists(main_py):
        st.error(f"Fichier main.py non trouvé : {main_py}")
        return False, "", "Erreur : main.py manquant"
    cmd = [python_exe, main_py, option, login, password, db_path]
    if client_name:
        cmd.append(client_name)
    cmd.extend([start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")])
    st.write(f"DEBUG: cmd = {cmd}")
    # Environnement
    env = os.environ.copy()
    env.update({
        "PYTHONPATH": project_root,
        "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        "AWS_DEFAULT_REGION": os.environ.get("AWS_DEFAULT_REGION", ""),
        "AWS_BUCKET": os.environ.get("AWS_BUCKET", ""),
        "PYTHONUNBUFFERED": "1",
        "ComSpec": os.environ.get("ComSpec", "C:\\Windows\\System32\\cmd.exe"),
        "SystemRoot": os.environ.get("SystemRoot", "C:\\Windows")
    })
    try:
        process = subprocess.Popen(
            cmd,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            bufsize=1,
            universal_newlines=True,
            cwd=project_root
        )
    except FileNotFoundError as e:
        st.error(f"Erreur lancement processus : {str(e)}")
        return False, "", str(e)
    stdout_output, stderr_output = [], []
    output_container = st.empty()
    # File d'attente pour la sortie
    stdout_queue = queue.Queue()
    stderr_queue = queue.Queue()
    def read_stream(stream, q):
        try:
            while True:
                line = stream.readline()
                if not line:
                    break
                q.put(line.strip())
        except Exception as e:
            q.put(f"Erreur lecture stream: {str(e)}")
    # Lancer les threads de lecture
    threading.Thread(target=read_stream, args=(process.stdout, stdout_queue), daemon=True).start()
    threading.Thread(target=read_stream, args=(process.stderr, stderr_queue), daemon=True).start()
    # Boucle principale
    start_time = time.time()
    timeout = 300  # Temps pour scraping
    while process.poll() is None and (time.time() - start_time) < timeout:
        try:
            while True:
                line = stdout_queue.get_nowait()
                stdout_output.append(line)
                output_container.text("\n".join(stdout_output[-10:]))
        except queue.Empty:
            pass
        try:
            while True:
                line = stderr_queue.get_nowait()
                stderr_output.append(line)
                output_container.text("\n".join(stderr_output[-10:]))
        except queue.Empty:
            pass
        # Mise à jour progression
        elapsed = time.time() - start_time
        progress = min(int(elapsed / timeout * 100), 100)
        progress_bar.progress(progress)
        progress_text.text(f"Avancement : {progress}% (Temps écoulé : {int(elapsed)}s)")
        time.sleep(0.1)
    # Gestion de la fin
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout_output.append("Processus tué après timeout.")
            stderr_output.append("Timeout.")
    # Capturer les sorties restantes
    try:
        while True:
            line = stdout_queue.get_nowait()
            stdout_output.append(line)
    except queue.Empty:
        pass
    try:
        while True:
            line = stderr_queue.get_nowait()
            stderr_output.append(line)
    except queue.Empty:
        pass
    output_container.text("\n".join(stdout_output[-10:]))
    if stderr_output:
        output_container.text("\n".join(stderr_output[-10:]))
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

    if menu_option == "Recherche des clients":
        st.header("Recherche des clients")
        if st.button("Mettre à jour la liste des clients"):
            success, stdout, stderr = run_process("1", login, password, db_path, datetime.date(2017, 1, 1),
                                                  datetime.date.today())
            if success:
                st.success("Mise à jour terminée.")
            else:
                st.error(f"Échec: {stderr}")

        # Ouvrir une connexion pour la requête
        with sqlite3.connect(db_path) as conn:
            df_keys = pd.read_sql_query("SELECT * FROM client_keys", conn)
            if df_keys.empty:
                st.warning("Table vide, lancement automatique...")
                success, stdout, stderr = run_process("1", login, password, db_path, datetime.date(2017, 1, 1),
                                                      datetime.date.today())
                # Nouvelle connexion après run_process
                with sqlite3.connect(db_path) as conn:
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
            return

        # Ouvrir une connexion pour la liste des clients
        with sqlite3.connect(db_path) as conn:
            client_list = pd.read_sql_query("SELECT DISTINCT nom FROM client_keys", conn)["nom"].tolist()

        if client_list:
            selected_client = st.selectbox("Sélectionnez un client", client_list, key="detailed_client")

            if st.button(f"Mettre à jour les ventes détaillées pour {selected_client}"):
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
                    success, stdout, stderr = run_process("4", login, password, db_path, start_date, end_date,
                                                          client_name=None)
                    download_from_s3(AWS_BUCKET, s3_db_name, db_path)
                    if success:
                        st.success("Mise à jour terminée pour tous les clients.")
                        st.rerun()
                    else:
                        st.error(f"Échec: {stderr}")

            # Ouvrir une connexion pour les transactions
            with sqlite3.connect(db_path) as conn:
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
                        total_ventes = df_simple[libelles.str.contains("vente") & ~libelles.str.contains(
                            "paiement") & ~libelles.str.contains("retour")]["total"].sum()
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
                    st.warning("Aucune donnée de transactions disponible. Mettez à jour via 'Ventes détaillées'.")
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