import sys
from config.config import START_DATE, END_DATE
from runners.client_keys import run as run_client_keys
from runners.detailed_pdf import run as run_detailed_pdf
from core.scraper import PharmaScraper

import sys
from config.config import START_DATE, END_DATE
from runners.client_keys import run as run_client_keys
from runners.detailed_pdf import run as run_detailed_pdf
from core.scraper import PharmaScraper

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python main.py <choice> <login> <password> [<client_name>] [<start_date>] [<end_date>]")
        sys.exit(1)

    choice, login, password = sys.argv[1:4]
    db_path = f"pharmacie_{login.replace('@', '_at_').replace('.', '_')}.db"

    # Gestion des arguments optionnels
    if len(sys.argv) > 4:
        if len(sys.argv) == 7:  # Avec client_name
            client_name = sys.argv[4]
            start_date = sys.argv[5]
            end_date = sys.argv[6]
        elif len(sys.argv) == 6:  # Sans client_name
            client_name = None
            start_date = sys.argv[4]
            end_date = sys.argv[5]
        else:
            print("Nombre d'arguments incorrect")
            sys.exit(1)
    else:
        client_name = None
        start_date = START_DATE
        end_date = END_DATE

    print(f"Arguments reçus : choice={choice}, login={login}, password=****, client_name={client_name}, "
          f"start_date={start_date}, end_date={end_date}, db_path={db_path}")

    scraper = PharmaScraper()
    try:
        if choice == "1":
            run_client_keys(login, password, db_path, scraper=scraper)
        elif choice == "4":
            run_detailed_pdf(login, password, db_path, start_date, end_date, client_name, scraper=scraper)
        else:
            print("Option invalide: 1 ou 4")
            sys.exit(1)
    finally:
        scraper.cleanup()