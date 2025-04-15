import sys
import os
import logging
import threading
import time
import signal

# Configurer le logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config.config import START_DATE, END_DATE
from runners.client_keys import run as run_client_keys
from runners.detailed_pdf import run as run_detailed_pdf
from core.scraper import PharmaScraper

def timeout_handler(timeout_event):
    logger.error("Timeout atteint lors de l'exécution de main.py")
    timeout_event.set()  # Signale que le timeout est atteint
    raise TimeoutError("Exécution trop longue")

if __name__ == "__main__":
    logger.info("Démarrage de main.py avec args: %s", sys.argv)
    if len(sys.argv) < 4:
        logger.error("Usage: python main.py <choice> <login> <password> [<client_name>] [<start_date>] [<end_date>]")
        sys.exit(1)

    choice, login, password = sys.argv[1:4]
    db_path = f"pharmacie_{login.replace('@', '_at_').replace('.', '_')}.db"

    # Gestion des arguments optionnels
    if len(sys.argv) > 4:
        if len(sys.argv) == 7:
            client_name = sys.argv[4]
            start_date = sys.argv[5]
            end_date = sys.argv[6]
        elif len(sys.argv) == 6:
            client_name = None
            start_date = sys.argv[4]
            end_date = sys.argv[5]
        else:
            logger.error("Nombre d'arguments incorrect")
            sys.exit(1)
    else:
        client_name = None
        start_date = START_DATE
        end_date = END_DATE

    logger.info(
        f"Arguments reçus : choice={choice}, login={login}, password=****, client_name={client_name}, "
        f"start_date={start_date}, end_date={end_date}, db_path={db_path}"
    )

    logger.info("Initialisation de PharmaScraper")
    scraper = PharmaScraper()
    timeout_event = threading.Event()  # Événement pour suivre le timeout
    try:
        if choice == "1":
            logger.info("Lancement de run_client_keys")
            start_time = time.time()
            # Configurer le timer pour 15 minutes (900 secondes)
            timer = threading.Timer(900, timeout_handler, args=(timeout_event,))
            timer.start()
            try:
                run_client_keys(login, password, db_path, scraper=scraper)
                logger.info("Fin de run_client_keys en %.2f secondes", time.time() - start_time)
            finally:
                timer.cancel()  # Annuler le timer si l'exécution se termine
        elif choice == "4":
            logger.info("Lancement de run_detailed_pdf")
            timer = threading.Timer(900, timeout_handler, args=(timeout_event,))
            timer.start()
            try:
                run_detailed_pdf(login, password, db_path, start_date, end_date, client_name, scraper=scraper)
                logger.info("Fin de run_detailed_pdf")
            finally:
                timer.cancel()
        else:
            logger.error("Option invalide: 1 ou 4")
            sys.exit(1)
    except TimeoutError as e:
        logger.error("Arrêt forcé: %s", str(e))
        sys.exit(1)
    except Exception as e:
        logger.error("Erreur lors de l'exécution: %s", str(e))
        raise
    finally:
        if timeout_event.is_set():
            logger.error("Programme arrêté à cause du timeout")
        logger.info("Appel de scraper.cleanup")
        scraper.cleanup()
        logger.info("Fin de main.py")