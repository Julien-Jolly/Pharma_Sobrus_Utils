import os
from dotenv import load_dotenv
import datetime

load_dotenv()

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION")
AWS_BUCKET = os.getenv("AWS_BUCKET")

# Validation des identifiants AWS
if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    raise ValueError("Les clés AWS_ACCESS_KEY_ID et AWS_SECRET_ACCESS_KEY doivent être définies dans .env")

# Constantes globales
START_DATE = "2017-01-01"
END_DATE = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

# Chemins
DOWNLOAD_DIR = os.path.join(os.getcwd(), "downloads")