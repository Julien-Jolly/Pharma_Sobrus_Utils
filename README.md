Pharma Sobrus Utils
Pharma_Sobrus_Utils est une application Python automatisant l'extraction de données clients et de relevés financiers depuis la plateforme Pharma Sobrus. Elle utilise Selenium pour la navigation web, multiprocessing pour le traitement parallèle, Streamlit pour une interface utilisateur intuitive, et AWS S3 pour le stockage des données. Le projet permet de récupérer les clés clients, télécharger des PDFs de ventes détaillées, traiter leur contenu et afficher les résultats dans une base de données SQLite.
Fonctionnalités principales
Authentification sécurisée : Connexion automatique à Pharma Sobrus avec gestion des sessions via cookies.

Extraction des clés clients : Récupération des identifiants uniques des clients (nom, clé) à partir des pages de la plateforme.

Téléchargement de PDFs : Génération et récupération de relevés détaillés au format PDF via l’API Pharma Sobrus pour une période définie.

Traitement des PDFs : Extraction des données transactionnelles (date, libellé, total, solde) avec calcul du solde initial et final.

Stockage des données : Sauvegarde des clés clients et des transactions dans une base SQLite spécifique à l’utilisateur (pharmacie_<email>.db).

Synchronisation AWS S3 : Téléversement automatique des bases SQLite vers S3 et récupération si nécessaire.

Interface utilisateur : Application Streamlit permettant de lancer les tâches, visualiser les données et exporter les résultats en CSV.

Traitement parallèle : Utilisation de plusieurs workers (par défaut 3) pour accélérer l’extraction des clés clients.

Gestion robuste des erreurs : Réessais automatiques pour les échecs de téléchargement, navigation ou authentification.

Nettoyage automatique : Suppression des fichiers temporaires (PDFs, cookies, dossiers de téléchargement) après exécution.

Architecture du projet

Pharma_Sobrus_Utils/
├── /config/
│   ├── __init__.py
│   └── config.py           # Configuration AWS, dates par défaut, chemins
├── /core/
│   ├── __init__.py
│   ├── s3_utils.py         # Gestion des opérations S3 (upload/download)
│   ├── scraper.py          # Logique de scraping avec Selenium et requests
│   └── pdf_processor.py    # Extraction des données des PDFs
├── /database/
│   ├── __init__.py
│   └── db_manager.py       # Gestion de la base SQLite
├── /runners/
│   ├── __init__.py
│   ├── client_keys.py      # Extraction parallèle des clés clients
│   └── detailed_pdf.py     # Téléchargement et traitement des PDFs
├── /ui/
│   ├── __init__.py
│   └── streamlit_app.py    # Interface Streamlit
├── main.py                 # Point d’entrée CLI
├── requirements.txt        # Dépendances Python
└── README.md               # Documentation

Prérequis
Système d’exploitation : Windows (testé sur Windows 10), Linux ou macOS.

Python : Version 3.8+.

Navigateur : Google Chrome (version compatible avec ChromeDriver).

Connexion Internet : Nécessaire pour accéder à https://app.pharma.sobrus.com et AWS S3.

Compte AWS : Clés d’accès AWS configurées pour S3.

Installation
Cloner le dépôt :
bash

git clone https://github.com/<votre-utilisateur>/Pharma_Sobrus_Utils.git
cd Pharma_Sobrus_Utils

Créer un environnement virtuel :
bash

python -m venv .venv

Activer l’environnement :
Windows :
powershell

.venv\Scripts\Activate.ps1

Linux/macOS :
bash

source .venv/bin/activate

Installer les dépendances :
bash

pip install -r requirements.txt

Exemple de requirements.txt :

streamlit==1.38.0
selenium==4.25.0
webdriver-manager==4.0.2
requests==2.32.3
pandas==2.2.3
pdfplumber==0.11.4
boto3==1.35.24
python-dotenv==1.0.1

Configurer AWS :
Créez un fichier .env à la racine du projet avec :

AWS_ACCESS_KEY_ID=<votre-clé>
AWS_SECRET_ACCESS_KEY=<votre-clé-secrète>
AWS_DEFAULT_REGION=<votre-région>
AWS_BUCKET=<votre-bucket>

Utilisation
Via l’interface Streamlit
Lancer l’application :
bash

streamlit run ui/streamlit_app.py

L’interface s’ouvre sur http://localhost:8501.

Se connecter :
<<<<<<< HEAD
Entrez l’email et le mot de passe (ex. : email@gmail.com, mot de passe fourni séparément).
=======
Entrez l’email et le mot de passe (ex. : ma@gmail.com, mot de passe fourni séparément).
>>>>>>> 603158ca85eada80d37c81c1a0fb5c1f3e762f70

L’application vérifie les identifiants en se connectant au site cible.

Options disponibles :
Recherche des clients :
Cliquez sur "Mettre à jour la liste des clients".

Les clés clients sont extraites (ex. : 259 clients sur ~13 pages) et stockées dans pharmacie_<email>.db.

Affiche la liste des clients dans un tableau.

Ventes détaillées par client :
Sélectionnez une période (par défaut : 2017-01-01 à hier).

Choisissez un client ou mettez à jour tous les clients.

Les PDFs sont téléchargés, traités, et les transactions affichées (ventes, paiements, avoirs, retours).

Exportez les données en CSV.

Logs : Consultez les logs dans l’interface pour suivre la progression ou diagnostiquer les erreurs.

Via la ligne de commande
bash

python main.py <choice> <login> <password> [<client_name>] [<start_date>] [<end_date>]

<choice> : 1 (extraire les clés clients) ou 4 (télécharger et traiter les PDFs).

Exemple :
bash

<<<<<<< HEAD
python main.py 1 email@gmail.com mpd
python main.py 4 email@gmail.com mpd "Client X" 2024-01-01 2024-12-31

Base de données
Nom : pharmacie_<email>.db (ex. : pharmacie_email.db).
=======
python main.py 1 mar@gmail.com JU
python main.py 4 mr@gmail.com JU "Client X" 2024-01-01 2024-12-31

Base de données
Nom : pharmacie_<email>.db (ex. : pharmacie_ma_at_gmail_com.db).
>>>>>>> 603158ca85eada80d37c81c1a0fb5c1f3e762f70

Tables :
client_keys : Nom et clé client.

simple_transactions : Transactions simplifiées (date, libellé, total, solde).

detailed_transactions : Détails des transactions (produit, quantité, prix, etc.).

solde_final : Solde final par client.

Synchronisation : Téléversée sur S3 après chaque mise à jour, téléchargée si disponible.

Dépannage
Problèmes courants
Erreur "Overshoot" dans client_keys.py :
Cause : Conflits entre workers sur la navigation des pages.

Solution :
Réduisez NUM_WORKERS à 1 dans client_keys.py pour tester.

Vérifiez les fichiers cookies_<port>.json (un par worker).

Supprimez les cookies : rm cookies_*.json.

Moins de clients extraits :
Cause : Échec de navigation ou déconnexion.

Solution :
Consultez client_keys.log pour identifier la page en échec.

Testez en mode non-headless (décommentez --headless dans scraper.py).

Vérifiez la stabilité de la connexion Internet.

Erreur ChromeDriver :
Cause : Incompatibilité Chrome/ChromeDriver.

Solution :
bash

pip install --upgrade webdriver-manager
rm -rf ~/.wdm

Session invalide :
Cause : Cookies expirés ou blocage par le site.

Solution :
Supprimez cookies_*.json.

Forcez l’authentification dans Streamlit.

Erreur S3 :
Cause : Clés AWS incorrectes ou bucket inaccessible.

Solution :
Vérifiez .env.

Testez avec aws s3 ls s3://<votre-bucket>.

Nettoyage manuel
bash

taskkill /F /IM chrome.exe
taskkill /F /IM chromedriver.exe
rm -rf downloads cookies_*.json pharmacie_*.db
rm -rf __pycache__ core/__pycache__ runners/__pycache__ ui/__pycache__

Performances
Extraction des clés : ~300s pour 259 clients (13 pages) avec 3 workers.

Téléchargement PDFs : Dépend du nombre de clients et de la période (parallélisé avec 6 workers).

Optimisations possibles :
Réduire les time.sleep dans scraper.py (ex. : 1s → 0.7s).

Augmenter NUM_WORKERS ou max_workers si CPU le permet.

Implémenter une navigation directe par URL si disponible.

Sécurité
Les identifiants sont saisis via Streamlit ou CLI, non stockés localement.

Les cookies sont sauvegardés temporairement (cookies_<port>.json) et supprimés à la fin.

Les bases SQLite sont spécifiques à l’utilisateur et synchronisées sur S3.

Contributeurs
Julien : Développeur principal, architecture et maintenance.

Licence
Sous licence MIT (voir LICENSE si disponible).
Support
Ouvre une issue sur GitHub.

Contact : <votre-email> (optionnel).

Dernière mise à jour : 15 avril 2025

