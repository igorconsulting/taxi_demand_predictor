import dotenv
import os
from src.paths import PARENT_DIR

dotenv.load_dotenv(PARENT_DIR / '.env')

PROJECT_NAME = os.getenv('HOPSWORKS_PROJECT_NAME')
API_KEY = os.getenv('HOPSWORKS_API_KEY')