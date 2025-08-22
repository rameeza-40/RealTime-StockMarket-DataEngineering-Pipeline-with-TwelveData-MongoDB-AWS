## **Real time-stock Market data engineer with TwelveData-MongoDB-Aws**
<img width="10108" height="4304" alt="image" src="https://github.com/user-attachments/assets/a78e7077-acf2-4dca-acd9-5fcc701279ea" />
 
 ## *Project Overview:*
* *The Realtime Stock Market Data Engineering Pipeline is designed to capture, process, and analyze live stock market data from external APIs.The financial markets generate high-frequency, real-time data that must be collected, processed, and analyzed efficiently to enable decision-making for traders, analysts, and businesses. This project focuses on building a cloud-based, scalable, real-time data engineering pipeline that ingests stock market data from the TwelveData API, stores raw data in MongoDB, processes and transforms it using PySpark on AWS EMR, aggregates it with AWS Glue, and loads the results into AWS RDS for analytics and alerting.
The pipeline ensures automation, reliability, and scalability with Apache Airflow orchestration and supports real-time monitoring and alerting for stock price fluctuations*
##  <ins>*Key Components:*</ins> 
 
* *Data Source: TwelveData API (providing OHLCV data every 5 minutes)*.

* *Ingestion Layer: Apache Airflow schedules and orchestrates API calls*.

* *Staging Layer: Raw JSON data stored in MongoDB, partitioned by date/time*.

* *Processing Layer: PySpark on Google Colab transforms JSON → DataFrame → CSV, stored in S3 with partitions.*

* *Aggregation Layer: AWS Glue jobs create 30-min and 1-hour summaries.*

* *Analytics Layer: Aggregated data loaded into AWS RDS (PostgreSQL/MySQL).*

 <br/>


 ### Data Set Info
- *Source: TwelveData API*
- *Format: JSON*
- *Variables:*
  
    -`symbol`: Stock symbol (e.g., AAPL, TSLA)

    -`datetime`: Timestamp
    
    -`open, high, low, close`: Stock prices

    -`volume`: Volume of trade

- *Each API call provides OHLCV (Open, High, Low, Close, Volume) data at 5-minute intervals.*
  <br/>
  ## ⚙️ Architecture Workflow:
 ### 1️⃣ Ingestion

* *Tool: Apache Airflow*
    
* *Process:An Airflow DAG is scheduled to fetch real-time OHLCV stock price data every 5 minutes from the TwelveData API.*
    
 * *Companies tracked: Apple (AAPL), Tesla (TSLA), Google (GOOGL), Microsoft (MSFT), Nvidia (NVDA).*

<br/>

![WhatsApp Image 2025-07-26 at 16 09 52_eee5c4ee](https://github.com/user-attachments/assets/c9442e13-8546-4b95-a294-30678fe35e5e)

<br/>

 ### 2️⃣  Storage (Raw Layer)

 * *Tool: MongoDB (staging database)*
 * *Process:*
    * *Raw validated JSON data stored in MongoDB.*
    * *Partition strategy → organized by symbol and date for faster retrieval.*
        
<br/>

 ###  3️ Processing

* *Tool: PySpark running on Google Colab (for dev) / AWS EMR (for production scale)*

* *Process:*

    * *Data extracted from MongoDB.*
    * *JSON → PySpark DataFrame → validated & transformed → converted to CSV format.*
    * *Data is stored in Amazon S3 with partitioning by date/hour/symbol.*

<br/>

 ### 4️⃣Aggregation

* *Tool: AWS Glue*

* *Process:*

    * *Create time-window aggregations (30 minutes, 1 hour).*
    * *Generate summary tables (average close price, min/max high-low, total volume).*
    * *Aggregated results stored back into Amazon S3.*
* *![WhatsApp Image 2025-08-17 at 22 16 31_60787a9e](https://github.com/user-attachments/assets/15dd90cb-f2ef-44b9-84bc-ecc3744b037e)*

<br/>

- ### 5️⃣  Analytics & Alerts

* *Analytics Storage:*
  
* *Final processed & aggregated data is stored in TiDB Cloud Database (distributed SQL DB).*
* *![WhatsApp Image 2025-08-18 at 19 04 34_d164cecf](https://github.com/user-attachments/assets/33413532-6f88-45b2-a66b-12471340a822)*

* *Slack Alerts:*

* *If data is successfully written into TiDB Cloud, then automatically a Slack notification.**
* *![WhatsApp Image 2025-08-21 at 16 52 41_b507f5ca](https://github.com/user-attachments/assets/699e13f3-95b2-44fa-98e8-c5b3e342c347)*

* *Slack message"✅ All CSV files are stored successfully in S3 and TiDB Cloud.*

- ### 🔄 End-to-End Data Flow

* *TwelveData API → Airflow Ingestion → Validation → MongoDB (Raw Data) → PySpark (Colab/EMR) → S3 (Processed CSVs) → AWS Glue (Aggregations) → S3 (Aggregated Data) → TiDB Cloud (Final DB) → Slack (Notification)*


























