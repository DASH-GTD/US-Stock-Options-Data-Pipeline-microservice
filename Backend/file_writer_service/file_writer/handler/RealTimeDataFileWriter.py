import os
import pandas as pd
import boto3
from influxdb_client import InfluxDBClient
from datetime import datetime, timedelta, timezone
import time
from requests.exceptions import HTTPError

class RealTimeDataFileWriter:
    def __init__(self):
        self.influx_url = os.getenv('INFLUXDB_URL')
        self.influx_token = os.getenv('INFLUXDB_TOKEN')
        self.influx_org = os.getenv('INFLUXDB_ORG')
        self.influx_bucket = os.getenv('INFLUXDB_BUCKET')

        self.aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.s3_bucket = os.getenv('S3_BUCKET_NAME')

        self.output_folder = './fifteenmin_data/'
        os.makedirs(self.output_folder, exist_ok=True)

        self.client = InfluxDBClient(url=self.influx_url, token=self.influx_token, org=self.influx_org)
        self.s3_client = boto3.client('s3',
                                      aws_access_key_id=self.aws_access_key,
                                      aws_secret_access_key=self.aws_secret_key)

    def export_fifteen_min_data(self):
        today = datetime.now(timezone.utc).date()
        yesterday = today - timedelta(days=1)

        # Option to override dates via environment (for Airflow or debugging)
        #start_time = os.getenv('START_TIME') or datetime.combine(yesterday, datetime.min.time(), tzinfo=timezone.utc).isoformat()
        #end_time = os.getenv('END_TIME') or datetime.combine(today, datetime.min.time(), tzinfo=timezone.utc).isoformat()
        start_time = "2025-07-02T00:00:00Z"
        end_time = "2025-07-03T00:00:00Z"


        print(f"Running InfluxDB query from {start_time} to {end_time}...")

        query = f'''
            from(bucket: "{self.influx_bucket}")
              |> range(start: {start_time}, stop: {end_time})
              |> filter(fn: (r) => r._measurement == "stock_15min")
              |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''

        for attempt in range(3):
            try:
                query_api = self.client.query_api()
                tables = query_api.query_data_frame(query, org=self.influx_org)

                if tables.empty:
                    print("No data retrieved for the specified range.")
                    return

                tables = tables.drop(columns=[col for col in tables.columns if col.startswith('result') or col.startswith('table')], errors='ignore')

                filename = f'stock_15min_{yesterday.strftime("%d%m%Y")}.csv'
                local_path = os.path.join(self.output_folder, filename)
                tables.to_csv(local_path, index=False)

                print(f"Saved CSV: {local_path} ({len(tables)} rows)")
                self.upload_to_s3(local_path, filename,'real_time_15min_data')
                break  # Success, no need to retry

            except HTTPError as http_err:
                print(f"HTTP error on attempt {attempt+1}: {http_err}")
                time.sleep(5)
            except Exception as e:
                print(f"General error on attempt {attempt+1}: {e}")
                time.sleep(5)

    def upload_to_s3(self, file_path, s3_key, folder_name):
        try:
            s3_full_key = f"{folder_name}/{s3_key}"
            self.s3_client.upload_file(file_path, self.s3_bucket, s3_full_key)
            print(f"Uploaded to S3: s3://{self.s3_bucket}/{s3_full_key}")
        except Exception as e:
            print(f"S3 upload failed for {s3_key}: {e}")

    def close_connection(self):
        self.client.close()
        print("InfluxDB connection closed.")


