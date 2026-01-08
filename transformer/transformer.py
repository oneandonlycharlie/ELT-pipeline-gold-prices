import logging
import pandas as pd
from ingestor.utils.config_loader import get_pipeline_config
from psycopg2 import extras
import psycopg2
from typing import Dict
from ingestor.utils.db_connector import connect_to_db
import numpy as np


pipeline_config = get_pipeline_config()
YEARLY_TRADING_DAYS = pipeline_config["YEARLY_TRADING_DAYS"]
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
DIM_ASSET_NAME = '"public"."dim_asset"'
DIM_METRIC_NAME = '"public"."dim_metric"'
FACT_RAW_NAME = '"public"."fact_daily_prices_raw"'
CALCULATED_METRIC_COLS = list(METRIC_METADATA.keys())

def calculate_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    try:
    # indicator calculations
        logging.info("Calculating technical indicators.")
        df["ma_20_day"] = df["close_price"].rolling(window=20).mean()
        df["ma_50_day"] = df["close_price"].rolling(window=50).mean()
        df["daily_return"] = df["close_price"].pct_change()
        df["volatility_20_day"] = df["daily_return"].rolling(window=20).std()
        df["price_rank_52_week"] = df["close_price"].rolling(YEARLY_TRADING_DAYS).apply(
            lambda x: x.rank(pct=True).iloc[-1] * 100
        )
        df["highest_52_week"] = df["high_price"].rolling(window=YEARLY_TRADING_DAYS).max()
        df["lowest_52_week"] = df["low_price"].rolling(window=YEARLY_TRADING_DAYS).min()
        df["days_since_high"] = df["high_price"].rolling(window=YEARLY_TRADING_DAYS).apply(
            calculate_days_since_extreme, args=(True,)
        )
        df["days_since_low"] = df["low_price"].rolling(window=YEARLY_TRADING_DAYS).apply(
            calculate_days_since_extreme, args=(False,)
        )
        # df.reset_index(inplace=True)
        logging.info("Technical indicator calculation complete.")
        
        return df
    except Exception as e:
        logging.error(f"Error calculating technical indicators: {e}")
        return None

def calculate_days_since_extreme(window_series: pd.Series, is_high: bool) -> int:
    """
    Helper function that calculates the number of days since the highest (or lowest) price occurred in the current window.
    This function must return an integer to satisfy Pandas' rolling.apply() aggregation requirements.
    """
    if is_high:
        extreme_date = window_series.idxmax()
    else:
        extreme_date = window_series.idxmin()
    
    current_date = window_series.index[-1] 
    
    return (current_date - extreme_date).days 


def load_fact_calculated_metrics(conn: psycopg2.extensions.connection, df_calc: pd.DataFrame, metric_map: Dict[str, int]) -> bool:
    logging.info(f"Starting wide-to-long transformation for calculated metrics.")
    
    # convert wide to long format
    id_vars = ['date_key', 'asset_key']
    metric_cols = [col for col in CALCULATED_METRIC_COLS if col in df_calc.columns]

    df_long = df_calc.melt(
        id_vars=id_vars,
        value_vars=metric_cols,
        var_name='metric_name',
        value_name='metric_value'
    )
    df_long.dropna(subset=['metric_value'], inplace=True)
    
    # convert metric_name to metric_key
    df_long['metric_key'] = df_long['metric_name'].map(metric_map)
    df_long.drop(columns=['metric_name'], inplace=True)
    
    if df_long.empty:
        logging.warning("No valid calculated metrics found after melt and NaN removal.")
        return True
    
    # convert for insertion
    fact_calc_cols = ['date_key', 'asset_key', 'metric_key', 'metric_value']
    data_to_insert = [tuple(x) for x in df_long[fact_calc_cols].values]
    cols_str = ', '.join(fact_calc_cols)
    insert_query = f"""
        INSERT INTO {FACT_CALCULATED_NAME} ({cols_str}) 
        VALUES %s 
        ON CONFLICT (date_key, asset_key, metric_key) DO UPDATE 
        SET metric_value = EXCLUDED.metric_value ;
    """
    try:
        with conn.cursor() as cur:
            extras.execute_values(cur, insert_query, data_to_insert, template=None, page_size=1000)
        logging.info(f"Bulk UPSERT complete for {FACT_CALCULATED_NAME} ({len(data_to_insert)} records).")
        return True
    except Exception as e:
        logging.critical(f"Error loading {FACT_CALCULATED_NAME}: {e}")
        return False
    
    
def fetch_raw_data(conn, ticker: str, full_history: bool = False) -> pd.DataFrame:
    logging.info(f"Fetching raw data from DB for ticker: {ticker} (Full history: {full_history})")
    
    lookback_window = YEARLY_TRADING_DAYS + 50
    base_sql = f"""
        SELECT 
            f.date_key, a.ticker, f.asset_key,
            f.open_price, f.high_price, f.low_price, f.close_price, f.volume
        FROM {FACT_RAW_NAME} f
        JOIN {DIM_ASSET_NAME} a ON f.asset_key = a.asset_key
        WHERE a.ticker = %s
    """
    if full_history:
        sql = base_sql + " ORDER BY f.date_key ASC"
        params = (ticker,)
    else:
        sql = base_sql + f""" 
                AND f.date_key >= (SELECT MAX(date_key) FROM {FACT_RAW_NAME}) - INTERVAL '{lookback_window} days'
                ORDER BY f.date_key ASC
            """
        params = (ticker,)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        
            rows = cur.fetchall()
            if not rows:
                logging.warning(f"No data found for {ticker}")
                return pd.DataFrame()
            
            colnames = [desc[0] for desc in cur.description]
            df = pd.DataFrame(rows, columns=colnames)
            df['date_key'] = pd.to_datetime(df['date_key'])
            df.set_index('date_key', inplace=True, drop=False)         
            numeric_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'volume']
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)
            return df
    except Exception as e:
        logging.error(f"Error fetching data with cursor: {e}")
        return pd.DataFrame()

def get_metric_map(conn) -> Dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT metric_name, metric_key FROM {DIM_METRIC_NAME}")
        return dict(cur.fetchall())   

def run_transformer(is_full_refresh: bool = False):
    conn = connect_to_db()
    try:
        ticker = "GLD" 
        
        df_raw = fetch_raw_data(conn, ticker, full_history=is_full_refresh)
        if df_raw.empty: return

        df_indicators = calculate_technical_indicators(df_raw)

        df_final = df_indicators.tail(1) if not is_full_refresh else df_indicators
        
        target_cols = ['date_key', 'asset_key'] + CALCULATED_METRIC_COLS
        
        df_calc_slice = df_final[np.intersect1d(df_final.columns, target_cols)].copy()

        metric_map = get_metric_map(conn)
        
        load_fact_calculated_metrics(conn, df_calc_slice, metric_map)
    except Exception as e:
        logging.critical(f"Critical error in transformer pipeline: {e}")  
    finally:
        conn.close()
        
if __name__ == "__main__":
    run_transformer(is_full_refresh=True)              