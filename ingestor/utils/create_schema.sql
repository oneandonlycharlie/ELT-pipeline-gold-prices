
--dimension table for assets
CREATE TABLE IF NOT EXISTS dim_asset (
    asset_key INT GENERATED ALWAYS AS IDENTITY NOT NULL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL UNIQUE,
    asset_name VARCHAR(100) NOT NULL,
    asset_exchange VARCHAR(50) NOT NULL,
    region VARCHAR(50) NOT NULL,
    etl_load_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

--dimension table for metrics
CREATE TABLE IF NOT EXISTS dim_metric (
    metric_key INT GENERATED ALWAYS AS IDENTITY NOT NULL PRIMARY KEY,
    metric_name VARCHAR(50) NOT NULL UNIQUE,
    metric_description VARCHAR(255) NOT NULL,
    calculation_formala VARCHAR(255) NOT NULL,
    unit_of_measure VARCHAR(20) NOT NULL,
    etl_load_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

--dimension table for date
CREATE TABLE IF NOT EXISTS dim_date (
    date_key DATE NOT NULL PRIMARY KEY,
    full_date_string VARCHAR(10) NOT NULL,
    year SMALLINT NOT NULL,
    quarter SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    day_of_month SMALLINT NOT NULL,
    day_of_week VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_trading_day BOOLEAN NOT NULL,
    etl_load_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- raw prices of gold
CREATE TABLE IF NOT EXISTS fact_daily_prices_raw (
    date_key DATE NOT NULL,
    asset_key INT NOT NULL,
    open_price DECIMAL(20, 10) NOT NULL, 
    close_price DECIMAL(20, 10) NOT NULL,
    high_price DECIMAL(20, 10) NOT NULL,
    low_price DECIMAL(20, 10) NOT NULL,
    volume BIGINT NOT NULL,
    etl_load_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_date_asset PRIMARY KEY (date_key, asset_key),
    FOREIGN KEY (asset_key) 
        REFERENCES dim_asset (asset_key)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,
    FOREIGN KEY (date_key) 
        REFERENCES dim_date (date_key) 
        ON DELETE RESTRICT
        ON UPDATE CASCADE,
    CHECK (high_price >= low_price),
    CHECK (high_price >= open_price),
    CHECK (high_price >= close_price),
    CHECK (low_price <= open_price),
    CHECK (low_price <= close_price)
);

-- derivative metrics calculated from raw prices
CREATE TABLE IF NOT EXISTS fact_calculated_metrics (
    date_key DATE NOT NULL ,
    asset_key INT NOT NULL ,
    metric_key INT NOT NULL,
    metric_value DECIMAL(20, 10) NOT NULL,
    etl_load_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_date_asset_metric PRIMARY KEY (date_key, asset_key, metric_key),
    FOREIGN KEY (metric_key) 
        REFERENCES dim_metric (metric_key)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (date_key, asset_key) 
        REFERENCES fact_daily_prices_raw (date_key, asset_key) 
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO CURRENT_USER;