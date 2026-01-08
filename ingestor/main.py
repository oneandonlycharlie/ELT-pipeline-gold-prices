import logging
import sys
import argparse
from utils.config_loader import get_pipeline_config, get_db_config
from ingestor.api_fetcher import fetch_data
from ingestor.azure_storage_manager import upload_data, download_data
from ingestor.data_loader import standardize_and_clean, load_raw_data
from ingestor.utils.db_connector import connect_to_db
from transformer.transformer import run_transformer

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)])

pipeline_cfg = get_pipeline_config()
db_config = get_db_config()

def run_elt_pipeline():
    logging.info("🚀 Starting Gold Price ELT Pipeline Execution")
    
    logging.info("Checking data extraction date")

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Specific date for incremental load (YYYY-MM-DD)")
    parser.add_argument("--full", action="store_true", help="Run full history refresh")
    args = parser.parse_args()

    is_full_refresh = args.full
    if args.date:
        pipeline_cfg["DATA_EXTRACTION_DATE"] = args.date
        is_full_refresh = False # 如果指定了日期，强制进入增量模式
        logging.info(f"Data extraction date set from command line argument: {pipeline_cfg["DATA_EXTRACTION_DATE"]}")
    else:
        is_full_refresh = True

    # Extract
    logging.info("\n--- STEP 1: Starting Data Extraction (E) ---")
    if pipeline_cfg["DATA_EXTRACTION_DATE"]:
        logging.info(f"Extracting data for specific date: {pipeline_cfg["DATA_EXTRACTION_DATE"]}")
        pipeline_cfg["FILE_PATH"] = f"rawdata/data_{pipeline_cfg["DATA_EXTRACTION_DATE"]}.csv"
    else:
        logging.info("Extracting data for the full date range.")

    raw_data= fetch_data(pipeline_cfg)
    upload_success = upload_data(raw_data, db_config, pipeline_cfg)
    if upload_success:
        raw_data_df = download_data(db_config,pipeline_cfg)
        logging.info("Successfully uploaded raw data from Azure lake, Extraction completed.")
    else:
        logging.info("Failed to upload raw data, aborting pipeline")
        return 
    if raw_data_df is None or raw_data_df.empty:
        logging.error("Rawdata download failed or Azure returned empty data. Aborting pipeline.")
        return

    # Load
    logging.info("\n--- STEP 2: Starting Data Loading (L) ---")
    cleaned_df = standardize_and_clean(raw_data_df)
    if cleaned_df.empty:
        logging.error("Cleaned DataFrame is empty. Exiting.")
        return
    conn = connect_to_db()
    try:
        loading_success = load_raw_data(cleaned_df, conn)   
        if loading_success:
            logging.info("Success at loading raw gold prices")
            return
    finally:
        conn.close()
    
    #Transform
    transform_success = run_transformer(is_full_refresh)
    if transform_success:
        logging.info("Transformation success")
        logging.info("✅ ELT Pipeline completed successfully.")
    else:
        logging.error("❌ ELT Pipeline failed during Transform Stage.")

if __name__ == "__main__":
    run_elt_pipeline()