import dotenv
import os
from src.paths import PARENT_DIR

dotenv.load_dotenv(PARENT_DIR / '.env')

try:
    PROJECT_NAME = os.getenv('HOPSWORKS_PROJECT_NAME')
    API_KEY = os.getenv('HOPSWORKS_API_KEY')
    FEATURE_GROUP_NAME = os.getenv('FEATURE_GROUP_NAME')
    FEATURE_GROUP_VERSION = os.getenv('FEATURE_GROUP_VERSION')
except:
    raise Exception("Create an .env file with HOPSWORKS_PROJECT_NAME and HOPSWORKS_API_KEY")
