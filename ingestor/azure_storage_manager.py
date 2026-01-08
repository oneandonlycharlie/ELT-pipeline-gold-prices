import logging
from azure.storage.filedatalake import DataLakeFileClient
import pandas as pd
import os

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
        logging.info(f"Successfully uploaded data to in Azure Data Lake.")
        return True
    except Exception as e:
        logging.error(f"Failed to upload data to Azure Data Lake: {e}")

def download_data(db_config, pipeline_cfg) -> pd.DataFrame:
    if pipeline_cfg["DATA_EXTRACTION_DATE"]:
        local_file_path = f"/tmp/downloaded_data_{pipeline_cfg["DATA_EXTRACTION_DATE"]}.csv"
        file_path = f"rawdata/data_{pipeline_cfg["DATA_EXTRACTION_DATE"]}.csv"
    else:
        file_path = pipeline_cfg["FILE_PATH"]
        local_file_path = "/tmp/downloaded_data.csv"
    try:
        file = DataLakeFileClient.from_connection_string(
            db_config["AZURE_STORAGE_CONNECTION_STRING"], 
            file_system_name=pipeline_cfg["FILE_SYSTEM_NAME"],
            file_path=file_path)
        with open(local_file_path, "wb") as tmp_file:
            download = file.download_file()
            download.readinto(tmp_file)
        print("Successfully downloaded data from Azure Data Lake.")
        df = pd.read_csv(local_file_path) 
        return df
    except Exception as e:
        logging.error(f"Failed to download data from Azure Data Lake: {e}")
        return None
    finally:
        if os.path.exists(local_file_path):
            os.remove(local_file_path)       