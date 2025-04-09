from core.scraper import PharmaScraper
from database.db_manager import DBManager
from core.s3_utils import upload_to_s3, verify_s3_upload
import os

def run(login, password, db_path, scraper=None, manage_cleanup=True):
    if scraper is None:
        raise ValueError("Une instance de PharmaScraper doit être fournie")

    scraper.access_site("https://app.pharma.sobrus.com/", login, password)
    scraper.driver.get("https://app.pharma.sobrus.com/customers")

    db = DBManager(db_path)
    with db.connect() as conn:
        conn.execute("DELETE FROM client_keys")
        conn.commit()

    all_clients = []
    while True:
        clients = scraper.get_clients_from_page()
        all_clients.extend(clients)
        if not scraper.go_to_next_page():
            break

    for client in all_clients:
        client_key = scraper.retrieve_client_key(client)
        with db.connect() as conn:
            conn.execute("INSERT OR REPLACE INTO client_keys (nom, client_key) VALUES (?, ?)",
                         (client["nom"], client_key))
            conn.commit()

    upload_to_s3(db_path)
    verify_s3_upload(s3_file=os.path.basename(db_path))

    if manage_cleanup:
        scraper.cleanup()

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 4:
        print("Usage: python client_keys.py <login> <password> <db_path>")
        sys.exit(1)
    login, password, db_path = sys.argv[1:4]
    scraper = PharmaScraper()
    run(login, password, db_path, scraper)