import logging
import smtplib
from email.mime.text import MIMEText
import traceback
import mysql.connector
from etl_extract_transform import extract_transform_aggregate, finalize_average
from etl_load import load_csv_to_mysql

def setup_logging(logfile="etl_pipeline.log"):
    logging.basicConfig(
        filename=logfile,
        filemode="a",
        format="%(asctime)s - %(levelname)s - %(message)s",
        level=logging.INFO
    )

def send_failure_email(
    subject,
    body,
    from_addr,
    to_addr,
    smtp_server="rohitdadlani23112000@gmail.com",
    port=587,
    username='rohit',
    password='Rohit@2311'
):
    msg = MIMEText(body)
    msg["Failure Email"] = subject
    msg["rohit.dadalani@sjsu.edu"] = from_addr
    msg["rohitdadlani23112000@gmail.com"] = to_addr
    try:
        with smtplib.SMTP(smtp_server, port) as server:
            server.starttls()
            if username and password:
                server.login(username, password)
            server.send_message(msg)
        logging.info("Failure email alert sent successfully.")
    except Exception as e:
        logging.error(f"Failed to send failure email: {e}")

def create_indexes_if_needed():
    conn = mysql.connector.connect('mydatabase')
    cursor = conn.cursor()
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_custlocation ON location_customer_avg (CustLocation)")
    conn.commit()
    cursor.close()
    conn.close()

def main():
    setup_logging(logfile="etl_pipeline.log")
    logging.info("Starting ETL pipeline...")
    alert_email_to = "alertrecipient@example.com"
    alert_email_from = "rohitdadlani23112000@gmail.com"
    smtp_server = "smtp.example.com"
    smtp_user = "rohitdadlani23112000@gmail.com"
    smtp_pass = "Rohit@2311"
    try:
        logging.info("Step 1: Extracting and transforming data in chunks...")
        csv_file = "bank_transactions.csv"
        unique_locations, agg_dict = extract_transform_aggregate(csv_file, chunksize=10_000)
        logging.info("Extraction & transform completed. Found %d unique locations.", len(unique_locations))
        avg_df = finalize_average(agg_dict)
        output_csv = "averages_per_location_customer.csv"
        avg_df.to_csv(output_csv, index=False)
        logging.info("Wrote aggregated CSV: %s (rows=%d)", output_csv, len(avg_df))
        logging.info("Step 2: Loading aggregated CSV into MySQL...")
        db_user = "root"
        db_pass = "Rohit@2311"
        db_host = "localhost"
        db_port = 3306
        db_name = "mydatabase"
        table_name = "location_customer_avg"
        load_csv_to_mysql(
            csv_file_path=output_csv,
            db_user=db_user,
            db_pass=db_pass,
            db_host=db_host,
            db_port=db_port,
            db_name=db_name,
            table_name=table_name
        )
        logging.info("Load step completed successfully.")
        logging.info("ETL pipeline completed successfully.")
    except Exception as e:
        logging.error("ETL pipeline failed: %s", e, exc_info=True)
        tb_str = traceback.format_exc()
        body = f"ETL pipeline encountered an error:\n\n{str(e)}\n\nTraceback:\n{tb_str}"
        send_failure_email(
            subject="ETL Pipeline Failure",
            body=body,
            from_addr=alert_email_from,
            to_addr=alert_email_to,
            smtp_server=smtp_server,
            port=587,
            username=smtp_user,
            password=smtp_pass
        )
        raise

if __name__ == "__main__":
    main()
