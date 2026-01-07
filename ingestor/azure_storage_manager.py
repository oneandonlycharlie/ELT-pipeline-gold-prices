import logging
from azure.storage.filedatalake import DataLakeFileClient
import pandas as pd

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
        