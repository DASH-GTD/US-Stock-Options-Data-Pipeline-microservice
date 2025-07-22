import os
import pyodbc
import pandas as pd
import boto3
from datetime import datetime, timedelta
import time
import warnings
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")


class HistoricalDataFileWriter:
    def __init__(self):
        # Environment Variables
        self.connection_string = os.getenv('AZURE_SQL_CONNECTION_STRING')
        self.aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.s3_bucket = os.getenv('S3_BUCKET_NAME')
        self.output_folder = './daily_data/'

        os.makedirs(self.output_folder, exist_ok=True)

        # Initialize AWS S3 client
        self.s3_client = boto3.client('s3',
                                      aws_access_key_id=self.aws_access_key,
                                      aws_secret_access_key=self.aws_secret_key)

        # Initialize DB connection
        self.conn = self.get_db_connection()

    def get_db_connection(self, retries=5, delay=20):
        attempt = 0
        while attempt < retries:
            try:
                print(f"Attempt {attempt + 1} to connect to Azure SQL...")
                conn = pyodbc.connect(self.connection_string)
                print("Successfully connected to Azure SQL!")
                return conn
            except pyodbc.Error as e:
                print(f"Connection failed: {e}")
                attempt += 1
                if attempt < retries:
                    print(f" Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    print("All retry attempts failed. Exiting.")
                    raise

    def export_historical_data(self):
        today = datetime.today()

        for day_offset in range(365):
            target_date = today - timedelta(days=day_offset)
            date_str = target_date.strftime('%Y-%m-%d')
            date_filename = target_date.strftime('%d%m%Y')

            print(f"\nProcessing data for date: {date_str}")

            queries = {
                f'stock_data_{date_filename}.csv': f"""
                    SELECT * FROM stockdata 
                    WHERE CAST([Date] AS DATE) = '{date_str}'
                """,
                f'call_options_{date_filename}.csv': f"""
                    SELECT * FROM call_options 
                    WHERE CAST([expirationDate] AS DATE) = '{date_str}'
                """,
                f'put_options_{date_filename}.csv': f"""
                    SELECT * FROM put_options 
                    WHERE CAST([expirationDate] AS DATE) = '{date_str}'
                """
            }

            for filename, query in queries.items():
                try:
                    df = pd.read_sql(query, self.conn)
                    local_path = os.path.join(self.output_folder, filename)

                    if df.empty:
                        print(f" No data for {filename} on {date_str}. Skipping.")
                        continue

                    # Save CSV locally
                    df.to_csv(local_path, index=False)
                    print(f"Saved CSV: {local_path} ({len(df)} rows)")

                    # Decide S3 folder based on filename
                    if filename.startswith('stock_data'):
                        s3_folder = 'stock_data'
                    else:
                        s3_folder = 'options_data'

                    # Upload to S3
                    self.upload_to_s3(local_path, filename, s3_folder)

                except Exception as e:
                    print(f"Failed processing {filename}: {e}")

    def upload_to_s3(self, file_path, s3_key, folder_name):
        try:
            s3_full_key = f"{folder_name}/{s3_key}"
            self.s3_client.upload_file(file_path, self.s3_bucket, s3_full_key)
            print(f"Uploaded to S3: s3://{self.s3_bucket}/{s3_full_key}")
        except Exception as e:
            print(f"S3 upload failed for {s3_key}: {e}")

    def close_connection(self):
        if self.conn:
            self.conn.close()
            print(" Database connection closed.")


