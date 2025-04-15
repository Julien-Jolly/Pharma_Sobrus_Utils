import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging
import time
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from core.scraper import PharmaScraper
from core.pdf_processor import PDFProcessor
from database.db_manager import DBManager
from core.s3_utils import upload_to_s3

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("detailed_pdf.log", mode='a'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def download_pdf(scraper, client, start_date, end_date):
    try:
        pdf_file = scraper.download_detailed_pdf_api_with_requests(client, start_date, end_date)
        return client, pdf_file, None
    except Exception as e:
        return client, None, str(e)

def process_pdf(client, pdf_file, processor, db):
    try:
        if not os.path.exists(pdf_file):
            raise Exception("Fichier PDF non trouvé")
        data, solde_final = processor.extract_detailed_data(pdf_file, client)
        print(f"Client {client['nom']} - Données extraites : {len(data)} lignes")
        if data:
            print(f"Client {client['nom']} - Exemple première ligne : {data[0]}")
        else:
            print(f"Client {client['nom']} - Aucune donnée extraite !")
        db.save_simple_transactions(data, solde_final, client)
        print(f"Client {client['nom']} - Sauvegarde terminée, lignes insérées : {len(data)}")
    finally:
        if os.path.exists(pdf_file):
            os.remove(pdf_file)
            print(f"PDF supprimé: {pdf_file}")


def process_client(client, scraper, processor, db, start_date, end_date):
    """Télécharge et traite le PDF pour un client."""
    try:
        # Téléchargement
        logger.info(f"Téléchargement pour {client['nom']}")
        print(f"Téléchargement pour {client['nom']}")
        pdf_file = scraper.download_detailed_pdf_api_with_requests(client, start_date, end_date)

        if not os.path.exists(pdf_file):
            raise Exception("Fichier PDF non trouvé")

        # Traitement immédiat
        logger.info(f"Traitement pour {client['nom']}")
        print(f"Traitement pour {client['nom']}")
        data, solde_final = processor.extract_detailed_data(pdf_file, client)
        print(f"Client {client['nom']} - Données extraites : {len(data)} lignes")
        if data:
            print(f"Client {client['nom']} - Exemple première ligne : {data[0]}")
        else:
            print(f"Client {client['nom']} - Aucune donnée extraite !")
        db.save_simple_transactions(data, solde_final, client)
        print(f"Client {client['nom']} - Sauvegarde terminée, lignes insérées : {len(data)}")

        return client, pdf_file, None

    except Exception as e:
        return client, None, str(e)

    finally:
        if pdf_file and os.path.exists(pdf_file):
            os.remove(pdf_file)
            print(f"PDF supprimé: {pdf_file}")


def run(login, password, db_path, start_date, end_date, client_name=None, scraper=None):
    try:
        if scraper is None:
            scraper = PharmaScraper()
        processor = PDFProcessor()
        db = DBManager(db_path)

        logger.info(f"Début - login: {login}, db_path: {db_path}, client_name: {client_name}")
        print(f"Début - login: {login}, db_path: {db_path}, client_name: {client_name}")
        sys.stdout.flush()

        scraper.access_site("https://app.pharma.sobrus.com/", login, password)

        client_keys = db.get_client_keys(client_name) if client_name else db.get_client_keys()
        logger.info(f"Nombre total de clients : {len(client_keys)}")
        print(f"Nombre total de clients : {len(client_keys)}")
        sys.stdout.flush()

        max_workers = 6
        failed_downloads = []
        processed_count = 0

        # Téléchargement et traitement parallèles
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_client = {
                executor.submit(process_client, {"nom": name, "client_id": key}, scraper, processor, db, start_date,
                                end_date): name
                for name, key in client_keys
            }

            for future in as_completed(future_to_client):
                client, pdf_file, error = future.result()
                if error:
                    logger.error(f"Erreur pour {client['nom']} : {error}")
                    print(f"Erreur pour {client['nom']} : {error}")
                    failed_downloads.append(client)
                else:
                    processed_count += 1
                    logger.info(f"[{processed_count}] Traitement terminé: {client['nom']}")
                    print(f"[{processed_count}] Traitement terminé: {client['nom']}")
                sys.stdout.flush()

        # Retries pour les échecs
        max_retries = 3
        retry_count = 0
        while failed_downloads and retry_count < max_retries:
            retry_count += 1
            print(f"\n--- Réessai {retry_count}/{max_retries} pour {len(failed_downloads)} clients échoués ---")
            logger.info(f"Réessai {retry_count}/{max_retries}")
            current_failed = failed_downloads
            failed_downloads = []

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_client = {
                    executor.submit(process_client, client, scraper, processor, db, start_date, end_date): client["nom"]
                    for client in current_failed
                }

                for future in as_completed(future_to_client):
                    client, pdf_file, error = future.result()
                    if error:
                        logger.error(f"Échec réessai {retry_count} : {client['nom']} : {error}")
                        print(f"Échec réessai {retry_count} : {client['nom']} : {error}")
                        failed_downloads.append(client)
                    else:
                        processed_count += 1
                        logger.info(f"[{processed_count}] Réussite réessai {retry_count} : {client['nom']}")
                        print(f"[{processed_count}] Réussite réessai {retry_count} : {client['nom']}")
                    sys.stdout.flush()

            if failed_downloads and retry_count < max_retries:
                delay = 5 * (2 ** (retry_count - 1))
                print(f"Attente de {delay}s avant prochain essai...")
                time.sleep(delay)

        if failed_downloads:
            print(f"\n--- {len(failed_downloads)} échecs définitifs ---")
            for client in failed_downloads:
                print(f"Échec pour {client['nom']}")

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
    if len(sys.argv) < 4:
        print("Usage: python detailed_pdf.py <login> <password> <db_path> [<client_name>] [<start_date>] [<end_date>]")
        sys.exit(1)
    login, password, db_path = sys.argv[1:4]
    client_name = sys.argv[4] if len(sys.argv) > 4 else None
    start_date = sys.argv[5] if len(sys.argv) > 5 else "2017-01-01"
    end_date = sys.argv[6] if len(sys.argv) > 6 else "2025-04-10"
    scraper = PharmaScraper()
    run(login, password, db_path, start_date, end_date, client_name, scraper)