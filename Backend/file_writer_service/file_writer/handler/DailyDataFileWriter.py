# import os
# import pyodbc
# import pandas as pd
# import boto3
# from datetime import datetime
# import time

# class DailyDataFileWriter:
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

#         # Date strings
#         self.today_str = datetime.today().strftime('%Y-%m-%d')
#         self.today_filename = datetime.today().strftime('%d%m%Y')

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

#     def export_data(self):
#         queries = {
#             f'stock_data_{self.today_filename}.csv': f"""
#                 SELECT * FROM stockdata 
#                 WHERE CAST([Date] AS DATE) = '{self.today_str}'
#             """
#         }

#         for filename, query in queries.items():
#             try:
#                 df = pd.read_sql(query, self.conn)
#                 local_path = os.path.join(self.output_folder, filename)

#                 # Save to CSV
#                 df.to_csv(local_path, index=False)
#                 print(f"Saved CSV: {local_path} ({len(df)} rows)")

#                 # Upload to S3
#                 self.upload_to_s3(local_path, filename,'stock_data')
#             except Exception as e:
#                 print(f"Failed processing {filename}: {e}")

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
import psycopg2
import pandas as pd
import boto3
from datetime import datetime
import time
import socket
import logging

# Set up logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DailyDataFileWriter:
    def __init__(self):
        # Environment Variables
        self.conn_strings = [os.getenv('POSTGRES_CONNECTION_STRING', 'postgresql://sa:Passw0rd!@postgres:5432/us_stock_options_db')]
        self.max_retries = 5
        self.retry_delay = 30
        self.conn = None
        self.cursor = None
        logger.info("Waiting 30 seconds for PostgreSQL to be fully ready...")
        #time.sleep(30)
        self.connect()

        self.aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.s3_bucket = os.getenv('S3_BUCKET_NAME')
        self.output_folder = './daily_data/'

        os.makedirs(self.output_folder, exist_ok=True)

        # Initialize AWS S3 client
        self.s3_client = boto3.client('s3',
                                      aws_access_key_id=self.aws_access_key,
                                      aws_secret_access_key=self.aws_secret_key)

        # Wait until DB is reachable
        #self.wait_for_db()

        # Initialize DB connection
        #self.conn = self.get_db_connection()

        # Date strings
        self.today_str = datetime.today().strftime('%Y-%m-%d')
        self.today_filename = datetime.today().strftime('%d%m%Y')

    def connect(self):
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

                    # Verify table existence
                    self.cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'StockData')")
                    if not self.cursor.fetchone()[0]:
                        raise Exception("Table 'StockData' does not exist in database.")

                    logger.info("Successfully connected to PostgreSQL")
                    return
                except (psycopg2.Error, socket.gaierror) as e:
                    logger.error(f"Connection attempt {attempt}/{self.max_retries} failed: {e}")
                    if attempt < self.max_retries:
                        logger.info(f"Waiting {self.retry_delay} seconds before retrying...")
                        time.sleep(self.retry_delay)
                    else:
                        logger.warning(f"All attempts failed for connection string.")
        raise Exception("All connection attempts failed.")


    def export_data(self):
        queries = {
            f'stock_data_{self.today_filename}.csv': f"""
                SELECT * FROM "StockData" 
                WHERE CAST("Date" AS DATE) = '{self.today_str}'
            """
        }

        for filename, query in queries.items():
            try:
                df = pd.read_sql(query, self.conn)
                local_path = os.path.join(self.output_folder, filename)

                # Save to CSV
                df.to_csv(local_path, index=False)
                logger.info(f"Saved CSV: {local_path} ({len(df)} rows)")

                # Upload to S3
                self.upload_to_s3(local_path, filename, 'stock_data')
            except Exception as e:
                logger.error(f"Failed processing {filename}: {e}")

    def upload_to_s3(self, file_path, s3_key, folder_name):
        try:
            s3_full_key = f"{folder_name}/{s3_key}"
            self.s3_client.upload_file(file_path, self.s3_bucket, s3_full_key)
            logger.info(f"Uploaded to S3: s3://{self.s3_bucket}/{s3_full_key}")
        except Exception as e:
            logger.error(f"S3 upload failed for {s3_key}: {e}")

    def close(self):
        """Close database cursor and connection."""
        try:
            if self.cursor:
                self.cursor.close()
                logger.info("Database cursor closed.")
            if self.conn:
                self.conn.close()
                logger.info("Database connection closed.")
        except Exception as e:
            logger.error(f"Error during closing resources: {e}")
        finally:
            self.cursor = None
            self.conn = None