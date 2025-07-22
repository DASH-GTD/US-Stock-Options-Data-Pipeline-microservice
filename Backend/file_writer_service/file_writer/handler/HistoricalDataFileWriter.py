# import os
# import pyodbc
# import pandas as pd
# import boto3
# from datetime import datetime, timedelta
# import time
# import warnings
# warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")


# class HistoricalDataFileWriter:
#     def __init__(self):
#         # Environment Variables
#         self.connection_string = os.getenv('AZURE_SQL_CONNECTION_STRING')
#         self.aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
#         self.aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
#         self.s3_bucket = os.getenv('S3_BUCKET_NAME')
#         self.output_folder = './daily_data/'

#         os.makedirs(self.output_folder, exist_ok=True)

#         # Initialize AWS S3 client
#         self.s3_client = boto3.client('s3',
#                                       aws_access_key_id=self.aws_access_key,
#                                       aws_secret_access_key=self.aws_secret_key)

#         # Initialize DB connection
#         self.conn = self.get_db_connection()

#     def get_db_connection(self, retries=5, delay=20):
#         attempt = 0
#         while attempt < retries:
#             try:
#                 print(f"Attempt {attempt + 1} to connect to Azure SQL...")
#                 conn = pyodbc.connect(self.connection_string)
#                 print("Successfully connected to Azure SQL!")
#                 return conn
#             except pyodbc.Error as e:
#                 print(f"Connection failed: {e}")
#                 attempt += 1
#                 if attempt < retries:
#                     print(f" Retrying in {delay} seconds...")
#                     time.sleep(delay)
#                 else:
#                     print("All retry attempts failed. Exiting.")
#                     raise

#     def export_historical_data(self):
#         today = datetime.today()

#         for day_offset in range(365):
#             target_date = today - timedelta(days=day_offset)
#             date_str = target_date.strftime('%Y-%m-%d')
#             date_filename = target_date.strftime('%d%m%Y')

#             print(f"\nProcessing data for date: {date_str}")

#             queries = {
#                 f'stock_data_{date_filename}.csv': f"""
#                     SELECT * FROM stockdata 
#                     WHERE CAST([Date] AS DATE) = '{date_str}'
#                 """,
#                 f'call_options_{date_filename}.csv': f"""
#                     SELECT * FROM call_options 
#                     WHERE CAST([expirationDate] AS DATE) = '{date_str}'
#                 """,
#                 f'put_options_{date_filename}.csv': f"""
#                     SELECT * FROM put_options 
#                     WHERE CAST([expirationDate] AS DATE) = '{date_str}'
#                 """
#             }

#             for filename, query in queries.items():
#                 try:
#                     df = pd.read_sql(query, self.conn)
#                     local_path = os.path.join(self.output_folder, filename)

#                     if df.empty:
#                         print(f" No data for {filename} on {date_str}. Skipping.")
#                         continue

#                     # Save CSV locally
#                     df.to_csv(local_path, index=False)
#                     print(f"Saved CSV: {local_path} ({len(df)} rows)")

#                     # Decide S3 folder based on filename
#                     if filename.startswith('stock_data'):
#                         s3_folder = 'stock_data'
#                     else:
#                         s3_folder = 'options_data'

#                     # Upload to S3
#                     self.upload_to_s3(local_path, filename, s3_folder)

#                 except Exception as e:
#                     print(f"Failed processing {filename}: {e}")

#     def upload_to_s3(self, file_path, s3_key, folder_name):
#         try:
#             s3_full_key = f"{folder_name}/{s3_key}"
#             self.s3_client.upload_file(file_path, self.s3_bucket, s3_full_key)
#             print(f"Uploaded to S3: s3://{self.s3_bucket}/{s3_full_key}")
#         except Exception as e:
#             print(f"S3 upload failed for {s3_key}: {e}")

#     def close_connection(self):
#         if self.conn:
#             self.conn.close()
#             print(" Database connection closed.")

import os
import psycopg2 # Changed from pyodbc to psycopg2 for PostgreSQL
import pandas as pd
import boto3
from datetime import datetime, timedelta
import time
import socket # Added for hostname resolution
import logging
import warnings

warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")

# Set up logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HistoricalDataFileWriter:
    def __init__(self):
        # Environment Variables
        # Changed to use POSTGRES_CONNECTION_STRING for local PostgreSQL
        self.conn_strings = [os.getenv('POSTGRES_CONNECTION_STRING', 'postgresql://sa:Passw0rd!@postgres:5432/us_stock_options_db')]
        self.max_retries = 5
        self.retry_delay = 30
        self.conn = None
        self.cursor = None

        # Call the new connect method for PostgreSQL
        self.connect_postgres()

        self.aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.s3_bucket = os.getenv('S3_BUCKET_NAME')
        self.output_folder = './daily_data/' # KEEPING ORIGINAL FOLDER NAME

        os.makedirs(self.output_folder, exist_ok=True)

        # Initialize AWS S3 client
        self.s3_client = boto3.client('s3',
                                      aws_access_key_id=self.aws_access_key,
                                      aws_secret_access_key=self.aws_secret_key)

    def connect_postgres(self):
        """Establish a connection to the PostgreSQL database."""
        for conn_str in self.conn_strings:
            logger.info(f"Attempting connection using connection string: {conn_str}")
            for attempt in range(1, self.max_retries + 1):
                try:
                    server = "postgres"
                    server_ip = socket.gethostbyname(server)
                    logger.info(f"Resolved server '{server}' to IP: {server_ip}")

                    self.conn = psycopg2.connect(conn_str)
                    self.cursor = self.conn.cursor()

                    # Verify table existence for StockData
                    self.cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'StockData')")
                    if not self.cursor.fetchone()[0]:
                        raise Exception("Table 'StockData' does not exist in database.")
                    
                    # Verify table existence for call_options
                    self.cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'call_options')")
                    if not self.cursor.fetchone()[0]:
                        raise Exception("Table 'call_options' does not exist in database.")

                    # Verify table existence for put_options
                    self.cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'put_options')")
                    if not self.cursor.fetchone()[0]:
                        raise Exception("Table 'put_options' does not exist in database.")

                    logger.info("Successfully connected to PostgreSQL")
                    return
                except (psycopg2.Error, socket.gaierror) as e:
                    logger.error(f"Connection attempt {attempt}/{self.max_retries} failed: {e}")
                    if attempt < self.max_retries:
                        logger.info(f"Waiting {self.retry_delay} seconds before retrying...")
                        time.sleep(self.retry_delay)
                    else:
                        logger.warning(f"All attempts failed for connection string.")
            raise Exception("All connection attempts failed to PostgreSQL.")


    def export_historical_data(self):
        today = datetime.today()

        for day_offset in range(365):
            target_date = today - timedelta(days=day_offset)
            date_str = target_date.strftime('%Y-%m-%d')
            date_filename = target_date.strftime('%d%m%Y')

            logger.info(f"\nProcessing data for date: {date_str}") # Changed print to logger.info

            queries = {
                # Ensure correct table casing and quoting for PostgreSQL
                f'stock_data_{date_filename}.csv': f"""
                    SELECT * FROM "StockData"
                    WHERE CAST("Date" AS DATE) = '{date_str}'
                """,
                f'call_options_{date_filename}.csv': f"""
                    SELECT * FROM "call_options"
                    WHERE CAST("expirationDate" AS DATE) = '{date_str}'
                """,
                f'put_options_{date_filename}.csv': f"""
                    SELECT * FROM "put_options"
                    WHERE CAST("expirationDate" AS DATE) = '{date_str}'
                """
            }

            for filename, query in queries.items():
                try:
                    df = pd.read_sql(query, self.conn)
                    local_path = os.path.join(self.output_folder, filename)

                    if df.empty:
                        logger.info(f" No data for {filename} on {date_str}. Skipping.") # Changed print to logger.info
                        continue

                    # Save CSV locally
                    df.to_csv(local_path, index=False)
                    logger.info(f"Saved CSV: {local_path} ({len(df)} rows)") # Changed print to logger.info

                    # Decide S3 folder based on filename (KEEPING ORIGINAL LOGIC)
                    if filename.startswith('stock_data'):
                        s3_folder = 'stock_data'
                    else:
                        s3_folder = 'options_data'

                    # Upload to S3
                    self.upload_to_s3(local_path, filename, s3_folder)

                except Exception as e:
                    logger.error(f"Failed processing {filename}: {e}") # Changed print to logger.error

    def upload_to_s3(self, file_path, s3_key, folder_name):
        try:
            s3_full_key = f"{folder_name}/{s3_key}"
            self.s3_client.upload_file(file_path, self.s3_bucket, s3_full_key)
            logger.info(f"Uploaded to S3: s3://{self.s3_bucket}/{s3_full_key}") # Changed print to logger.info
        except Exception as e:
            logger.error(f"S3 upload failed for {s3_key}: {e}") # Changed print to logger.error

    def close_connection(self):
        """Close database cursor and connection."""
        try:
            if self.cursor:
                self.cursor.close()
                logger.info("Database cursor closed.") # Changed print to logger.info
            if self.conn:
                self.conn.close()
                logger.info("Database connection closed.") # Changed print to logger.info
        except Exception as e:
            logger.error(f"Error during closing resources: {e}") # Changed print to logger.error
        finally:
            self.cursor = None
            self.conn = None
