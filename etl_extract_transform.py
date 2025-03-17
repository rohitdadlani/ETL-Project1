import pandas as pd
from collections import defaultdict

def transform_chunk(df_chunk: pd.DataFrame) -> pd.DataFrame:
    df_chunk.drop_duplicates(subset=["TransactionID"], inplace=True)
    if "TransactionAmount (INR)" in df_chunk.columns:
        df_chunk["TransactionAmount (INR)"] = pd.to_numeric(
            df_chunk["TransactionAmount (INR)"], errors="coerce"
        ).fillna(0)
    if "CustLocation" in df_chunk.columns:
        df_chunk["CustLocation"] = df_chunk["CustLocation"].str.upper()
    if "TransactionAmount (INR)" in df_chunk.columns:
        df_chunk = df_chunk[df_chunk["TransactionAmount (INR)"] > 0]
    return df_chunk

def extract_transform_aggregate(csv_file_path: str, chunksize: int = 10_000):
    agg_dict = defaultdict(lambda: [0, 0])
    unique_locations = set()
    df_iterator = pd.read_csv(csv_file_path, chunksize=chunksize)
    for i, chunk in enumerate(df_iterator, start=1):
        print(f"Processing chunk {i} ...")
        chunk = transform_chunk(chunk)
        if "CustLocation" in chunk.columns:
            unique_locations.update(chunk["CustLocation"].dropna().unique())
        required_cols = {"CustLocation", "CustomerID", "TransactionAmount (INR)"}
        if required_cols.issubset(chunk.columns):
            grouped = chunk.groupby(["CustLocation", "CustomerID"])["TransactionAmount (INR)"].agg(["sum", "count"])
            for (loc, cust), row in grouped.iterrows():
                agg_dict[(loc, cust)][0] += row["sum"]
                agg_dict[(loc, cust)][1] += row["count"]
    return unique_locations, agg_dict

def finalize_average(agg_dict):
    records = []
    for (location, customer), (sum_amt, cnt) in agg_dict.items():
        avg_amt = sum_amt / cnt if cnt > 0 else 0
        records.append((location, customer, sum_amt, cnt, avg_amt))
    final_df = pd.DataFrame(records, columns=["CustLocation", "CustomerID", "SumAmount", "Count", "AvgAmount"])
    return final_df

def main():
    csv_file = "bank_transactions.csv"
    unique_locations, agg_dict = extract_transform_aggregate(csv_file, chunksize=10_000)
    print("\nTotal unique customer locations:", len(unique_locations))
    avg_df = finalize_average(agg_dict)
    print("\n--- Average transaction amount per (CustLocation, CustomerID) ---")
    print(avg_df.head(10))
    avg_df.to_csv("averages_per_location_customer.csv", index=False)

if __name__ == "__main__":
    main()
