from dotenv import load_dotenv
import os
from pathlib import Path
import logging
import sys

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)])

env_path = Path(__file__).resolve().parent.parent / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
    logging.info(f"Loaded environment variables from {env_path}")
else:
    logging.info(f"No .env file found at {env_path}, using system environment variables.")        
        
def get_db_config():
    return {
        "DB_HOST":os.getenv("PG_HOST", "db"),
        "DB_PORT":os.getenv("PG_PORT", "5432"),
        "DB_USER":os.getenv("PG_USER"),
        "DB_PASSWORD":os.getenv("PG_PASSWORD"),
        "DB_NAME":os.getenv("PG_DB"),
        "AZURE_STORAGE_CONNECTION_STRING":os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    }

def get_pipeline_config():
    return {
        "TICKER":"GLD",
        "START_DATE":"2021-01-01",
        "END_DATE":"2025-12-01",
        "DATE_FORMAT":'%Y-%m-%d',
        "MAX_RETRIES":3,
        "DELAY_BETWEEN_RETRIES":5, 
        "DEFAULT_SMA_PERIOD":20,
        "YEARLY_TRADING_DAYS":252,
        "FILE_SYSTEM_NAME":"bronze",
        "FILE_PATH":"rawdata/data.csv",
        "DATA_EXTRACTION_DATE":None  # Default to None, can be set via command line argument
    }