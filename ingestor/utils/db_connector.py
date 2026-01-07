import psycopg2
import logging
from utils.config_loader import get_db_config

config = get_db_config()
DB_CONFIG = {
    'host': config["DB_HOST"],
    'port': config["DB_PORT"],
    'user': config["DB_USER"],
    'password': config["DB_PASSWORD"],
    'dbname': config["DB_NAME"],
    'sslmode': 'require'
}
print(DB_CONFIG)
def connect_to_db() -> psycopg2.extensions.connection | None:
    logging.info("Attempting to connect to the database...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True 
        logging.info("Database connection established.")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        return None
    

if __name__ == '__main__':
    # Simple test to check database connection
    conn = connect_to_db()
    if conn:
        conn.close()
        print("Database connection test successful.")