import pyodbc
import socket
import time
from django.conf import settings
from db_writer.utils.logConfig import LogConfig

logger = LogConfig()

class Connect_test:
    def __init__(self):
        self.conn_strings = [
            settings.AZURE_SQL_CONNECTION_STRING,
            settings.AZURE_SQL_CONNECTION_STRING.replace("ODBC Driver 18", "ODBC Driver 17")
        ]
        self.max_retries = 5
        self.retry_delay = 20
        self.conn = None
        self.cursor = None
        self.connect()

    def connect(self):
        for conn_str in self.conn_strings:
            driver = conn_str.split(';')[0].split('=')[1]
            logger.info(f"Trying connection with {driver}")
            for attempt in range(self.max_retries):
                try:
                    logger.info(f"Connection attempt {attempt + 1}/{self.max_retries}")
                    server_ip = socket.gethostbyname("dash-gtd.database.windows.net")
                    logger.info(f"Server resolved to IP: {server_ip}")
                    self.conn = pyodbc.connect(conn_str)
                    self.cursor = self.conn.cursor()
                    self.cursor.execute("SELECT 1 FROM sys.tables WHERE name = 'StockData'")
                    if not self.cursor.fetchone():
                        raise Exception("StockData table does not exist")
                    logger.info("Connected successfully")
                    return
                except (pyodbc.Error, socket.gaierror) as e:
                    logger.error(f"Connection failed: {e}")
                    if attempt < self.max_retries - 1:
                        logger.info(f"Retrying in {self.retry_delay} seconds...")
                        time.sleep(self.retry_delay)
                logger.info(f"Failed with {driver}, trying next driver if available")
        raise Exception("All connection attempts failed")
    
    def Connect_test(self, data):
        logger.info(f"Connected successfully in Connect test {data}" )