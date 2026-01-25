
# Gold Price ELT Pipeline on Azure

* What is it - I built this as an end-to-end, automated data pipeline to extract, persist, and model gold price price data. It follows an ELT (Extract-Load-Transform) architecture to fetch daily gold prices and derivative metrics.
* Why built it - As an investor myself, I have a natual interest in tapping into both historical and incremental data of the gold market. So I built this pipeline to keep track of them.
* How it's built - The project emphasizes **Star Schema Design**, **modular Python ETL engineering**, **containerization with docker** and serverless orchestration on **Microsoft Azure**.

## **🏗️ Architecture & Data Modeling**

### **📊 Schema Design (DDL Focused)**
<img width="703" height="508" alt="Screenshot 2026-01-25 at 21 57 32" src="https://github.com/user-attachments/assets/d36bbcc5-69c9-4739-ad4c-d08e996eaae2" />
<img width="1015" height="920" alt="image" src="https://github.com/user-attachments/assets/f247592f-238f-49a2-a4d6-7e3c3bee2dc1" />


* **Fact Tables**:  
  * fact\_daily\_prices\_raw: Stores granular data.  
  * fact\_calculated\_metrics: A long-format fact table storing derived financial indicators (MA20, Volatility, etc.).  
* **Dimension Tables**:  
  * dim\_asset: Contains metadata for financial instruments (Ticker, Exchange, Region)
  * dim\_date: A comprehensive time dimension (Year, Quarter, Month, Trading Day flags)
  * dim\_metric: A metadata catalog for all calculated indicators(Descrption, calculation formula)
* **Integrity & Constraints**: The DDL enforces strict data quality through PRIMARY KEY, FOREIGN KEY relationships, and CHECK constraints to prevent data anomalies at the storage layer.

## **🚀 Pipeline Implementation**

### **🐍 Python ETL Engineering**

The ingestion layer is built with modular Python scripts, demonstrating advanced data handling capabilities:

* **Extraction**: Utilizes yfinance API with custom retry logic
* **Load and Transformation**: Decoupled processing where Python manages data ingestion into the **data lake** and structured loading into **PostgreSQL database** on Azure.
* **State Management**: Implements a "Lookback Buffer" in Python to ensure continuity of rolling indicators during daily incremental updates and eliminate NaNs.

<img width="827" height="173" alt="Screenshot 2026-01-25 at 23 47 06" src="https://github.com/user-attachments/assets/e68ed847-fc82-420b-a6a3-e58de7fa030e" />
<img width="791" height="460" alt="Screenshot 2026-01-25 at 23 47 23" src="https://github.com/user-attachments/assets/41967f5d-1981-4a85-94ef-acb6f8627a2e" />


### **🐳 Containerization & Environment Parity**

* **Docker**: All workloads are containerized for environment consistency.
* Link to image: ➡️ https://hub.docker.com/repository/docker/charlieeeegu/gold-pipeline/general

### **🎼 Orchestration & Azure Integration**

* **Azure ACI**: Leverages **Azure Container Instances** for serverless, on-demand compute
* **Apache Airflow**: Orchestrates the end-to-end lifecycle, for both daily incremental runs and backfill of historical data
* **Persistence**: Integrated **Azure Data Lake Storage Gen2** for raw data persistence, ensuring a "Source of Truth"

<img width="1148" height="659" alt="image" src="https://github.com/user-attachments/assets/61fc0124-f704-4cda-9c72-dca605488e42" />


## **📂 Project Structure**
```text
.
├── dags \# Airflow DAGs for task orchestration  
│   └── fetch_daily_price.py
├── Dockerfile
├── ingestor
│   ├── api_fetcher.py
│   ├── azure_storage_manager.py \#manages Azure connections
│   ├── data_loader.py  
│   ├── main.py \# entry point for extract and load processes
│   └── utils \# pipelien config and data definition language for reference 
├── requirements.txt
├── transformer
│   └── transformer.py
```
## **🛠️ Tech Stack**      

* **Language**: Python (Pandas, yfinance, Psycopg2)  
* **Database**: PostgreSQL (Relational Star Schema)  
* **Orchestrator**: Apache Airflow  
* **Cloud Platform**: Microsoft Azure (ADLS Gen2, ACI)  
* **DevOps**: Docker, Git
