import sqlite3
import os

class DBManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.init_db()

    def connect(self):
        return sqlite3.connect(self.db_path)

    def init_db(self):
        with self.connect() as conn:
            # Table des clés clients
            conn.execute("""
                CREATE TABLE IF NOT EXISTS client_keys (
                    nom TEXT PRIMARY KEY,
                    client_key TEXT
                )
            """)
            # Table originale détaillée
            conn.execute("""
                CREATE TABLE IF NOT EXISTS detailed_transactions (
                    nom TEXT,
                    date TEXT,
                    reference TEXT,
                    produit TEXT,
                    quantite REAL,
                    prix_unitaire REAL,
                    remise REAL,
                    prix_unitaire_remise REAL,
                    total REAL,
                    solde REAL
                )
            """)
            # Ajout colonne manquante si nécessaire
            cursor = conn.execute("PRAGMA table_info(detailed_transactions)")
            columns = [col[1] for col in cursor.fetchall()]
            if 'reference' not in columns:
                conn.execute("ALTER TABLE detailed_transactions ADD COLUMN reference TEXT")
            # Table des soldes finaux
            conn.execute("""
                CREATE TABLE IF NOT EXISTS solde_final (
                    nom TEXT PRIMARY KEY,
                    solde REAL
                )
            """)
            # Nouvelle table simplifiée
            conn.execute("""
                CREATE TABLE IF NOT EXISTS simple_transactions (
                    nom TEXT,
                    date TEXT,
                    reference TEXT,
                    libelle TEXT,
                    total REAL,
                    solde REAL
                )
            """)
            conn.commit()

    def init_detailed_transactions(self, client_name):
        with self.connect() as conn:
            conn.execute("DELETE FROM detailed_transactions WHERE nom = ?", (client_name,))
            conn.commit()

    def get_client_keys(self, client_name=None):
        with self.connect() as conn:
            if client_name:
                cursor = conn.execute("SELECT nom, client_key FROM client_keys WHERE nom = ?", (client_name,))
            else:
                cursor = conn.execute("SELECT nom, client_key FROM client_keys")
            return cursor.fetchall()

    def save_detailed_transactions(self, data, solde_final, client):
        with self.connect() as conn:
            conn.executemany("""
                INSERT INTO detailed_transactions (nom, date, reference, produit, quantite, prix_unitaire, remise, prix_unitaire_remise, total, solde)
                VALUES (:nom, :date, :reference, :produit, :quantite, :prix_unitaire, :remise, :prix_unitaire_remise, :total, :solde)
            """, data)
            if solde_final is not None:
                conn.execute("INSERT OR REPLACE INTO solde_final (nom, solde) VALUES (?, ?)", (client['nom'], solde_final))
            conn.commit()

    def save_simple_transactions(self, data, solde_final, client):
        with self.connect() as conn:
            conn.execute("DELETE FROM simple_transactions WHERE nom = ?", (client['nom'],))
            conn.executemany("""
                INSERT INTO simple_transactions (nom, date, reference, libelle, total, solde)
                VALUES (:nom, :date, :reference, :libelle, :total, :solde)
            """, data)
            if solde_final is not None:
                conn.execute("INSERT OR REPLACE INTO solde_final (nom, solde) VALUES (?, ?)",
                             (client['nom'], solde_final))
            conn.commit()
