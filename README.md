# ETL-Project1



**Project Overview**

The project reads the bank transactions from a large CSV file, transforms (cleans, aggregates) it in chunks, and loads the final aggregated data into a MySQL database. A small Flask dashboard displays the ETL logs and execution time.


**Data Source**

Dataset: Kaggle - Bank Customer Segmentation. link - https://www.kaggle.com/datasets/shivamb/bank-customer-segmentation


**ETL Flow**

1. Extraction

I used Pandas read_csv with chunksize to stream data in batches.
This prevents loading the entire dataset (potentially 1M+ rows) into memory at once.

2. Transformation

Remove duplicates (e.g., based on TransactionID).
Convert numeric columns, fix missing values.
Normalize text fields (e.g., uppercase city names).
Filter out invalid transactions (e.g., negative or zero amounts).
Aggregate on-the-fly (e.g., partial sum & count per (CustLocation, CustomerID)) so we can compute final averages later.

3. Loading

I create a final CSV with aggregated results (average transaction amount per customer per location).
That CSV is then loaded into a MySQL table using pandas.DataFrame.to_sql() or mysql.connector.executemany() for bulk inserts.

4. Monitoring & Logging

We configure Python’s logging module to record steps and errors in an etl_pipeline.log file.
If an error occurs, we send an email alert via SMTP.
A Flask dashboard (etl_dashboard.py) reads the log file and displays:
  Last ETL start/end times
  Recent log entries
  Execution time


Tech Stack
* Python 
* Pandas (data manipulation & chunk reading)
* SQLAlchemy / mysql-connector-python (for MySQL interaction)
* MySQL (relational database)
* Flask (simple dashboard to monitor ETL status/logs)
* smtplib (sending email alerts on failures)



**Performance Optimizations**

1. Chunk-Based Extraction

Using read_csv(..., chunksize=10000) to limit memory usage.

2. Partial Aggregations

I compute sums & counts for each (CustLocation, CustomerID) within each chunk and accumulate globally, rather than storing all rows in memory.

3. Multi-Threading

Optionally use ThreadPoolExecutor to parallelize chunk transformation or database loading.
Helps if the bottleneck is I/O-bound (e.g., large data insertion into MySQL).


**SQL Schema / Database Structure**

I used a MySQL database named mydatabase with tables, for example:

1. transactions Table

CREATE TABLE transactions (
    TransactionID VARCHAR(50) PRIMARY KEY,
    CustomerID VARCHAR(50),
    CustGender VARCHAR(10),
    CustLocation VARCHAR(100),
    CustAccou VARCHAR(100),
    TransactionAmount DECIMAL(15,2)
);

This table stores raw or partially transformed rows (if needed).

2. location_customer_avg Table

CREATE TABLE location_customer_avg (
    CustLocation VARCHAR(255),
    CustomerID VARCHAR(255),
    SumAmount DECIMAL(15,2),
    Count INT,
    AvgAmount DECIMAL(15,2)
);

Indexing query
CREATE INDEX idx_location ON location_customer_avg (CustLocation);
CREATE INDEX idx_customerid ON location_customer_avg (CustomerID);


**This is how the flask dashboard looks like:**
![image](https://github.com/user-attachments/assets/e7836a8c-ac6d-4ea8-89f7-840c1b490b1f)

