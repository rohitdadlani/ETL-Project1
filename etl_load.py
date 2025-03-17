import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import math
from concurrent.futures import ThreadPoolExecutor, as_completed

def load_chunk_to_mysql(df_chunk, engine, table_name):
    df_chunk.to_sql(
        name=table_name,
        con=engine,
        if_exists='append',
        index=False
    )
    return len(df_chunk)

def load_csv_to_mysql(csv_file_path, db_user, db_pass, db_host, db_port, db_name, table_name, num_threads=4):
    df = pd.read_csv(csv_file_path)
    db_pass_encoded = quote_plus(db_pass)
    engine_url = f"mysql+mysqlconnector://{db_user}:{db_pass_encoded}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(engine_url)
    num_rows = len(df)
    chunk_size = math.ceil(num_rows / num_threads)
    futures = []
    rows_inserted = 0
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        for i in range(num_threads):
            start = i * chunk_size
            end = min(start + chunk_size, num_rows)
            df_sub = df.iloc[start:end]
            future = executor.submit(load_chunk_to_mysql, df_sub, engine, table_name)
            futures.append(future)
        for f in as_completed(futures):
            rows_inserted += f.result()
    print(f"Successfully loaded {rows_inserted} rows into '{db_name}.{table_name}' table.")

def main():
    csv_file = "averages_per_location_customer.csv"
    db_user = "root"
    db_pass = "Rohit@2311"
    db_host = "localhost"
    db_port = 3306
    db_name = "mydatabase"
    table_name = "location_customer_avg"
    load_csv_to_mysql(
        csv_file_path=csv_file,
        db_user=db_user,
        db_pass=db_pass,
        db_host=db_host,
        db_port=db_port,
        db_name=db_name,
        table_name=table_name,
        num_threads=4
    )

if __name__ == "__main__":
    main()
