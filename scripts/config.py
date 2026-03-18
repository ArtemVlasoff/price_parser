import os
from dotenv import load_dotenv

load_dotenv()  # загружаем переменные из .env

# База данных
NEON_DB_URL = os.getenv('NEON_DB_URL')

# # Backblaze
# BACKBLAZE_KEY_ID = os.getenv('BACKBLAZE_KEY_ID')
# BACKBLAZE_APP_KEY = os.getenv('BACKBLAZE_APP_KEY')
# BACKBLAZE_BUCKET = os.getenv('BACKBLAZE_BUCKET')
# BACKBLAZE_ENDPOINT = os.getenv('BACKBLAZE_ENDPOINT')

# Пути (относительно корня проекта, а не папки scripts)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PRICE_FILES_DIR = os.path.join(PROJECT_ROOT, "price_files")  # папка с прайсами
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")            # папка для отчётов