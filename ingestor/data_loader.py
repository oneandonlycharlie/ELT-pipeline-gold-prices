import logging
import pandas as pd
from utils.config_loader import get_db_config, get_pipeline_config
from azure_storage_manager import download_data
import psycopg2
from psycopg2 import extras
from typing import Dict
import sys
from utils.db_connector import connect_to_db

DIM_ASSET_NAME = '"public"."dim_asset"'
DIM_METRIC_NAME = '"public"."dim_metric"'
DIM_DATE_NAME = '"public"."dim_date"'
FACT_RAW_NAME = '"public"."fact_daily_prices_raw"'
FACT_CALCULATED_NAME = '"public"."fact_calculated_metrics"'
METRIC_METADATA = {
    'ma_20_day': {'desc': '20-day Simple Moving Average', 'unit': 'Price', 'formula': 'AVG(close) over 20 days'},
    'ma_50_day': {'desc': '50-day Simple Moving Average', 'unit': 'Price', 'formula': 'AVG(close) over 50 days'},
    'daily_return': {'desc': 'Daily Percentage Return', 'unit': 'Percent', 'formula': '(Close / LAG(Close)) - 1'},
    'volatility_20_day': {'desc': '20-day Rolling Volatility', 'unit': 'StdDev', 'formula': 'STDDEV(daily_return) over 20 days'},
    'price_rank_52w': {'desc': '52-week Price Percentile Rank', 'unit': 'Percent', 'formula': 'PERCENT_RANK() over 252 days'},
    'highest_52_week': {'desc': '52-week High Price', 'unit': 'Price', 'formula': 'MAX(high) over 252 days'},
    'lowest_52_week': {'desc': '52-week Low Price', 'unit': 'Price', 'formula': 'MIN(low) over 252 days'},
    'days_since_high': {'desc': 'Days Since 52-week High', 'unit': 'Days', 'formula': 'Date - IDxmax(high) over 252 days'},
    'days_since_low': {'desc': 'Days Since 52-week Low', 'unit': 'Days', 'formula': 'Date - IDxmin(low) over 252 days'},
}
RAW_PRICE_COLS = ['date_key', 'asset_key', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']
CALCULATED_METRIC_COLS = list(METRIC_METADATA.keys())

def standardize_and_clean(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        logging.warning("Input DataFrame is empty or None. Skipping data cleaning.")
        return pd.DataFrame()
    
    logging.info("Starting data cleaning process.")
    try:
        df.columns = [column.lower() for column in df.columns]
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        
        # convert string  to numeric types
        numeric_cols = ['close', 'high', 'low', 'open', 'volume', 'price']
        for col in numeric_cols:
            # errors='coerce' turns invalid parsing into NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df.rename(columns={
            'date': 'date_key',
            'open': 'open_price',
            'high': 'high_price',
            'low': 'low_price',
            'close': 'close_price'
        }, inplace=True)
        df.dropna(subset=['date_key', 'close_price'], inplace=True)
        df.set_index('date_key', inplace=True)
        df.index = pd.DatetimeIndex(df.index)
        print(df.head())
        logging.info("Standardization complete.")
        df.reset_index(inplace=True)
        return df
    except Exception as e:
        logging.error(f"Error during standardization: {e}")
        return pd.DataFrame()
    
def load_raw_data(df: pd.DataFrame, conn) -> bool:
    try:
        load_date_success = load_dim_date(conn, df['date_key'])
        if not load_date_success:
            logging.error("Failed to load date dimension. Aborting load.")
            return False
        
        load_metric_success = load_dim_metric(conn)
        if not load_metric_success:
            logging.error("Failed to load metric dimension. Aborting load.")
            return False

        current_ticker = df['ticker'].iloc[0] 
        asset_key = lookup_and_insert_asset_dimension(conn, current_ticker, df)
        if asset_key is None:
            logging.error("Failed to get asset_key. Aborting load.")
            return False
        
        df['asset_key'] = asset_key     
        load_price_success = load_fact_raw_prices(conn, df)
        if not load_price_success:
            logging.error("Failed to load raw prices. Aborting load.")
            return False
    except Exception as e:
        logging.critical(f"Unexpected error during raw data load: {e}")
        return False

def load_dim_date(conn: psycopg2.extensions.connection, dates: pd.Series) -> bool:
    # in case dates contain duplicates
    date_df = pd.DataFrame(dates.unique(), columns=['date_key'])
    logging.info(f"Generating and loading {len(date_df)} unique dates into {DIM_DATE_NAME}.")

    
    date_df['date_key'] = pd.to_datetime(date_df['date_key'])
    date_df['full_date_string'] = date_df['date_key'].dt.strftime('%Y-%m-%d')
    date_df['year'] = date_df['date_key'].dt.year
    date_df['quarter'] = date_df['date_key'].dt.quarter
    date_df['month'] = date_df['date_key'].dt.month
    date_df['day_of_month'] = date_df['date_key'].dt.day
    date_df['day_of_week'] = date_df['date_key'].dt.day_name()
    date_df['is_weekend'] = date_df['date_key'].dt.dayofweek >= 5
    date_df['is_trading_day'] = True
    dim_cols = [
        'date_key', 'full_date_string', 'year', 'quarter', 'month', 'day_of_month', 
        'day_of_week', 'is_weekend', 'is_trading_day'
    ]
    data_to_insert = [tuple(x) for x in date_df[dim_cols].values]
    cols_str = ', '.join(dim_cols)

    insert_query = f"""
        INSERT INTO {DIM_DATE_NAME} ({cols_str}) 
        VALUES %s 
        ON CONFLICT (date_key) DO NOTHING; 
    """
    try:
        with conn.cursor() as cur:
            extras.execute_values(cur, insert_query, data_to_insert, template=None, page_size=1000)
        logging.info(f"Successfully loaded dates into {DIM_DATE_NAME}.")
        return True
    except Exception as e:
        logging.critical(f"Error loading {DIM_DATE_NAME}: {e}")
        return False   
    
def load_dim_metric(conn: psycopg2.extensions.connection) -> Dict[str, int]:
    metric_map = {}
    
    try:
        with conn.cursor() as cur:
            for name, meta in METRIC_METADATA.items():
                insert_query = f"""
                    INSERT INTO {DIM_METRIC_NAME} (metric_name, metric_description, calculation_formala, unit_of_measure)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (metric_name) DO UPDATE 
                    SET metric_description = EXCLUDED.metric_description,
                        calculation_formala = EXCLUDED.calculation_formala,
                        unit_of_measure = EXCLUDED.unit_of_measure
                    RETURNING metric_key;
                """
                cur.execute(insert_query, (name, meta['desc'], meta['formula'], meta['unit']))
                metric_key = cur.fetchone()[0]
                metric_map[name] = metric_key
        logging.info(f"Loaded {len(metric_map)} metrics into {DIM_METRIC_NAME}.")
        return True
    except Exception as e:
        logging.critical(f"Error loading {DIM_METRIC_NAME}: {e}")
        return False
    
def load_fact_raw_prices(conn: psycopg2.extensions.connection, df_raw: pd.DataFrame) -> bool:
    logging.info(f"Loading raw prices into {FACT_RAW_NAME}.")
    
    data_to_insert = [tuple(x) for x in df_raw[RAW_PRICE_COLS].values]
    cols_str = ', '.join(RAW_PRICE_COLS)

    update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in RAW_PRICE_COLS if col not in ['date_key', 'asset_key']])
    insert_query = f"""
        INSERT INTO {FACT_RAW_NAME} ({cols_str}) 
        VALUES %s 
        ON CONFLICT (date_key, asset_key) DO UPDATE 
        SET {update_set};
    """
    
    try:
        with conn.cursor() as cur:
            extras.execute_values(cur, insert_query, data_to_insert, template=None, page_size=1000)
        logging.info(f"Bulk UPSERT complete for {FACT_RAW_NAME}.")
        return True
    except Exception as e:
        logging.critical(f"Error loading {FACT_RAW_NAME}: {e}")
        return False

def lookup_and_insert_asset_dimension(conn: psycopg2.extensions.connection, ticker: str, df: pd.DataFrame) -> int | None:

    logging.info(f"Checking dimension table for ticker: {ticker}")

    asset_name = f"{ticker} Gold ETF"
    asset_exchange = "NYSEARCA"
    region = "Global"
    
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT asset_key FROM {DIM_ASSET_NAME} WHERE ticker = %s;", (ticker,))
            result = cur.fetchone()
            if result:
                asset_key = result[0]
                logging.info(f"Asset '{ticker}' found, asset_key: {asset_key}")
                return asset_key
            else:
                insert_query = f"""
                    INSERT INTO {DIM_ASSET_NAME} (ticker, asset_name, asset_exchange, region)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (ticker) DO NOTHING 
                    RETURNING asset_key;
                """
                cur.execute(insert_query, (ticker, asset_name, asset_exchange, region))
                asset_key = cur.fetchone()[0]
                logging.info(f"New asset '{ticker}' inserted with asset_key: {asset_key}")
                return asset_key
    except Exception as e:
        logging.critical(f"Error during asset dimension lookup/insertion: {e}")
        return None

if __name__ == "__main__":
    db_config = get_db_config()
    pipeline_cfg = get_pipeline_config()
    raw_data_df = download_data(db_config,pipeline_cfg)
    cleaned_df = standardize_and_clean(raw_data_df)
    print(cleaned_df.head())
    if cleaned_df.empty:
        logging.error("Cleaned DataFrame is empty. Exiting.")
        sys.exit(1)
    else:
        conn = connect_to_db()
        load_raw_data(cleaned_df, conn)
        conn.close()