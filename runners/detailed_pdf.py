import logging
import time
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from concurrent.futures import ThreadPoolExecutor, as_completed

# Imports relatifs corrigés
from core.scraper import PharmaScraper
from core.pdf_processor import PDFProcessor
from database.db_manager import DBManager
from core.s3_utils import upload_to_s3


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("detailed_pdf.log", mode='a'),
        logging.StreamHandler()  # Ajout pour voir les logs en temps réel
    ]
)
logger = logging.getLogger(__name__)


def download_pdf(scraper, client, start_date, end_date):
    """Fonction pour télécharger un PDF (utilisée dans un thread)."""
    try:
        pdf_file = scraper.download_detailed_pdf_api_with_requests(client, start_date, end_date)
        return client, pdf_file, None
    except Exception as e:
        return client, None, str(e)


def process_pdf(client, pdf_file, processor, db):
    """Traite un PDF après téléchargement (utilisée dans un thread)."""
    try:
        if not os.path.exists(pdf_file):
            raise Exception("Fichier PDF non trouvé")

        data, solde_final = processor.extract_detailed_data(pdf_file, client)
        print(f"Données extraites pour {client['nom']}: {len(data)} enregistrements")
        if data:
            print(f"Exemple de données: {data[0]}")
        db.init_detailed_transactions(client["nom"])
        db.save_detailed_transactions(data, solde_final, client)
        print(f"Sauvegarde terminée pour {client['nom']}")
        sys.stdout.flush()

    finally:
        if os.path.exists(pdf_file):
            os.remove(pdf_file)
            print(f"PDF supprimé: {pdf_file}")
            sys.stdout.flush()


def run(login, password, db_path, start_date, end_date, client_name, scraper=None):
    try:
        if scraper is None:
            scraper = PharmaScraper()
        processor = PDFProcessor()  # Plus besoin de use_pymupdf
        db = DBManager(db_path)

        logger.info(f"Début - login: {login}, db_path: {db_path}, client_name: {client_name}")
        print(f"Début - login: {login}, db_path: {db_path}, client_name: {client_name}")
        sys.stdout.flush()

        # Authentification avec Selenium pour récupérer les cookies
        scraper.access_site("https://app.pharma.sobrus.com/", login, password)
        scraper.get_cookies_for_requests()  # Extraire les cookies pour requests

        client_keys = db.get_client_keys() if not client_name else db.get_client_keys(client_name)

        logger.info(f"Nombre total de clients : {len(client_keys)}")
        print(f"Nombre total de clients : {len(client_keys)}")
        sys.stdout.flush()

        # Étape 1 : Paralléliser les téléchargements
        max_workers_download = 6  # Limiter à 6 téléchargements simultanés
        downloaded_pdfs = []
        failed_downloads = []

        # Première tentative de téléchargement
        with ThreadPoolExecutor(max_workers=max_workers_download) as executor:
            futures = [
                executor.submit(download_pdf, scraper, {"nom": client_name, "client_id": client_key}, start_date,
                                end_date)
                for client_name, client_key in client_keys
            ]
            for idx, future in enumerate(as_completed(futures), 1):
                client, pdf_file, error = future.result()
                if error:
                    logger.error(f"Erreur téléchargement pour {client['nom']} : {error}")
                    print(f"[{idx}/{len(client_keys)}] Erreur téléchargement pour {client['nom']} : {error}")
                    failed_downloads.append(client)  # Stocker uniquement client
                else:
                    logger.info(f"[{idx}/{len(client_keys)}] Téléchargé: {client['nom']}")
                    print(f"[{idx}/{len(client_keys)}] Téléchargé: {client['nom']}")
                    downloaded_pdfs.append((client, pdf_file))
                sys.stdout.flush()

        # Réessayer les téléchargements échoués (max 3 tentatives)
        max_retries = 3
        retry_count = 0
        while failed_downloads and retry_count < max_retries:
            retry_count += 1
            print(
                f"\n--- Tentative de réessai {retry_count}/{max_retries} pour {len(failed_downloads)} téléchargements échoués ---")
            logger.info(
                f"Tentative de réessai {retry_count}/{max_retries} pour {len(failed_downloads)} téléchargements échoués")

            # Réinitialiser la liste des échecs pour cette tentative
            current_failed = failed_downloads
            failed_downloads = []

            with ThreadPoolExecutor(max_workers=max_workers_download) as executor:
                futures = [
                    executor.submit(download_pdf, scraper, client, start_date, end_date)
                    for client in current_failed
                ]
                for idx, future in enumerate(as_completed(futures), 1):
                    client, pdf_file, error = future.result()
                    if error:
                        logger.error(f"Échec réessai {retry_count} pour {client['nom']} : {error}")
                        print(
                            f"[{idx}/{len(current_failed)}] Échec réessai {retry_count} pour {client['nom']} : {error}")
                        failed_downloads.append(client)  # Stocker uniquement client
                    else:
                        logger.info(f"[{idx}/{len(current_failed)}] Réussite réessai {retry_count}: {client['nom']}")
                        print(f"[{idx}/{len(current_failed)}] Réussite réessai {retry_count}: {client['nom']}")
                        downloaded_pdfs.append((client, pdf_file))
                    sys.stdout.flush()

            # Backoff exponentiel : 5s, 10s, 20s
            if failed_downloads and retry_count < max_retries:
                delay = 5 * (2 ** (retry_count - 1))  # 5s, 10s, 20s
                print(f"Attente de {delay} secondes avant la prochaine tentative...")
                logger.info(f"Attente de {delay} secondes avant la prochaine tentative")
                time.sleep(delay)

        # Loguer les échecs définitifs après toutes les tentatives
        if failed_downloads:
            print(f"\n--- {len(failed_downloads)} téléchargements ont échoué après {max_retries} tentatives ---")
            logger.error(f"{len(failed_downloads)} téléchargements ont échoué après {max_retries} tentatives")
            for client in failed_downloads:
                print(f"Échec définitif pour {client['nom']}")
                logger.error(f"Échec définitif pour {client['nom']}")

        # Étape 2 : Paralléliser les traitements
        max_workers_process = 8  # Limiter à 8 traitements simultanés
        with ThreadPoolExecutor(max_workers=max_workers_process) as executor:
            futures = [
                executor.submit(process_pdf, client, pdf_file, processor, db)
                for client, pdf_file in downloaded_pdfs
            ]
            for idx, future in enumerate(as_completed(futures), 1):
                future.result()  # Attendre la fin de chaque traitement
                logger.info(f"[{idx}/{len(downloaded_pdfs)}] Traitement terminé: {downloaded_pdfs[idx - 1][0]['nom']}")
                print(f"[{idx}/{len(downloaded_pdfs)}] Traitement terminé: {downloaded_pdfs[idx - 1][0]['nom']}")
                sys.stdout.flush()

        # Upload sur S3
        logger.info(f"Upload: {db_path} -> S3://jujul/{os.path.basename(db_path)}")
        upload_to_s3(db_path, "jujul", os.path.basename(db_path))
        print(f"Upload réussi")
        sys.stdout.flush()

    finally:
        if scraper:
            scraper.cleanup()
        logger.info("Fin du traitement")
        print("Fin du traitement")
        sys.stdout.flush()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 4:
        print("Usage: python detailed_pdf.py <login> <password> <db_path> [<client_name>] [<start_date>] [<end_date>]")
        sys.exit(1)
    login, password, db_path = sys.argv[1:4]
    client_name = sys.argv[4] if len(sys.argv) > 4 else None
    start_date = sys.argv[5] if len(sys.argv) > 5 else "2017-01-01"
    end_date = sys.argv[6] if len(sys.argv) > 6 else "2025-04-07"
    scraper = PharmaScraper()
    run(login, password, db_path, start_date, end_date, client_name, scraper)