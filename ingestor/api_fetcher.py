import logging 
import yfinance as yf
import pandas as pd
import time
import sys
from azure.storage.filedatalake import DataLakeFileClient
from utils.config_loader import get_db_config, get_pipeline_config
from datetime import datetime, timedelta

# Set up logging pipeline_cfguration
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)])


def fetch_data(pipeline_cfg) -> pd.DataFrame | None:
    if pipeline_cfg["DATA_EXTRACTION_DATE"]:
        start_date = pipeline_cfg["DATA_EXTRACTION_DATE"]
        end_date = datetime.strptime(start_date, pipeline_cfg["DATE_FORMAT"]) + timedelta(days=1)
        end_date = end_date.strftime(pipeline_cfg["DATE_FORMAT"])
    else:
        start_date = pipeline_cfg["START_DATE"]
        end_date = pipeline_cfg["END_DATE"]
        
    for attempt in range(pipeline_cfg["MAX_RETRIES"]):
        logging.info(f"Fetching data for {pipeline_cfg["TICKER"]} starting from {start_date}, attempt {attempt + 1}")
        try:
            data = yf.download(pipeline_cfg["TICKER"], start=start_date, end=end_date, rounding=True)
            if data.empty:
                logging.warning(f"Dataframe is empty for {pipeline_cfg["TICKER"]} from {start_date} to {end_date}. Check network connection or ticker validity.")
                return None
            else:
                data.reset_index(inplace=True)
                data["Ticker"] = pipeline_cfg["TICKER"]
                logging.info(f"Successfully fetched data for {pipeline_cfg["TICKER"]} with {len(data)} records")
                return data
        except Exception as e:
            logging.error(f"Error fetching data for {pipeline_cfg["TICKER"]}: {e}")
            if attempt < pipeline_cfg["MAX_RETRIES"] - 1:
                logging.info(f"Retrying in {pipeline_cfg["DELAY_BETWEEN_RETRIES"]} seconds...")
                time.sleep(pipeline_cfg["DELAY_BETWEEN_RETRIES"])
            else:
                logging.error(f"Fetching data for {pipeline_cfg["TICKER"]} failed after {pipeline_cfg["MAX_RETRIES"]} attempts.")
                return None
            
def upload_data(data: pd.DataFrame, db_config, pipeline_cfg) -> bool:
    if pipeline_cfg["DATA_EXTRACTION_DATE"]:
        logging.info(f"Uploading data for specific date: {pipeline_cfg["DATA_EXTRACTION_DATE"]}")
        file_path = f"rawdata/data_{pipeline_cfg["DATA_EXTRACTION_DATE"]}.csv"
    else:
        logging.info("Uploading data for the full date range.")
        file_path = pipeline_cfg["FILE_PATH"]
    try:
        file = DataLakeFileClient.from_connection_string(
            db_config["AZURE_STORAGE_CONNECTION_STRING"], 
            file_system_name=pipeline_cfg["FILE_SYSTEM_NAME"],
            file_path=file_path)
        csv_data = data.to_csv()
        
        file.create_file()
        file.append_data(csv_data, offset=0, length=len(csv_data))
        file.flush_data(len(csv_data))
        print(f"Successfully uploaded data to in Azure Data Lake.")
        return True
    except Exception as e:
        logging.error(f"Failed to upload data to Azure Data Lake: {e}")
    
if __name__ == "__main__":
    pipeline_cfg = get_pipeline_config()
    print(pipeline_cfg)
    data = fetch_data(pipeline_cfg)
    if data is not None and not data.empty:
        print("Raw data overview:   ")
        print(data.head())
        db_config = get_db_config()
        upload_data(data, db_config, pipeline_cfg)
    else:
        print("No data fetched.")
