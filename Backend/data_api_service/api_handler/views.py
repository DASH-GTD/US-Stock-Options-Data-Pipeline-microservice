import pyodbc
import socket
import time
import logging
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.conf import settings
from .utils import get_connection_string, connect_with_retry

logger = logging.getLogger(__name__)

class StockDataView(APIView):

    def get(self, request):
        conn = None
        cursor = None
        try:
            logger.info("Resolving server name...")
            server_ip = socket.gethostbyname("dash-gtd.database.windows.net")
            logger.info(f"Server resolved to IP: {server_ip}")

            conn_str = get_connection_string(settings.DATABASES['default'])
            conn = connect_with_retry(conn_str)
            cursor = conn.cursor()

            # Verify tables
            cursor.execute("SELECT 1 FROM sys.tables WHERE name = 'StockData'")
            if not cursor.fetchone():
                raise Exception("StockData table not found in default DB")

            # Check if symbol parameter is provided
            symbol = request.GET.get('symbol')
            
            if symbol:
                # Filter by specific symbol
                select_query = "SELECT StockName, Date, [Open], High, Low, [Close], Volume FROM StockData WHERE StockName = ?"
                cursor.execute(select_query, (symbol.upper(),))
            else:
                # Get all stock data
                select_query = "SELECT StockName, Date, [Open], High, Low, [Close], Volume FROM StockData"
                cursor.execute(select_query)
            
            data = []

            # Fetch from first database
            for row in cursor.fetchall():
                data.append({
                    'symbol': row.StockName,
                    'date': row.Date.isoformat().split('T')[0] if row.Date else None,
                    'open': float(row.Open) if row.Open is not None else None,
                    'high': float(row.High) if row.High is not None else None,
                    'low': float(row.Low) if row.Low is not None else None,
                    'close': float(row.Close) if row.Close is not None else None,
                    'volume': int(row.Volume) if row.Volume is not None else None
                })

            response_data = {
                'status': 'success',
                'data': {
                    'stock_data': data,
                    'total_db1_rows': len(data)
                }
            }

            return Response(response_data, status=status.HTTP_200_OK)

        except pyodbc.Error as e:
            logger.error(f"Database error: {e}")
            return Response({
                'status': 'error',
                'message': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        except socket.gaierror as e:
            logger.error(f"DNS resolution error: {e}")
            return Response({
                'status': 'error',
                'message': 'DNS resolution failed'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        except Exception as e:
            logger.error(f"General error: {e}")
            return Response({
                'status': 'error',
                'message': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            if cursor:
                try:
                    cursor.close()
                except pyodbc.Error as e:
                    logger.error(f"Error closing cursor: {e}")
            if conn:
                try:
                    conn.close()
                except pyodbc.Error as e:
                    logger.error(f"Error closing connection: {e}")

class OptionsDataView(APIView):

    def get(self, request):
        conn = None
        cursor1 = None
        cursor2 = None
        try:
            # Resolve DNS
            logger.info("Resolving server name...")
            server_ip = socket.gethostbyname("dash-gtd.database.windows.net")
            logger.info(f"Server resolved to IP: {server_ip}")

            # Connect to first database
            conn_str = get_connection_string(settings.DATABASES['default'])
            conn = connect_with_retry(conn_str)
            cursor1 = conn.cursor()
            cursor2 = conn.cursor()

            # Verify tables
            cursor1.execute("SELECT 1 FROM sys.tables WHERE name = 'put_options'")
            if not cursor1.fetchone():
                raise Exception("put_options table not found in default DB")
            
            select_put_option_query = "SELECT contractSymbol, lastTradeDate, expirationDate, strike, lastPrice, bid, ask, change, percentChange, volume, openInterest, impliedVolatility, inTheMoney, contractSize, currency, StockName FROM put_options"
            data1 = []

            cursor1.execute(select_put_option_query)
            for row in cursor1.fetchall():
                data1.append({
                    'contractSymbol': row.contractSymbol,
                    'lastTradeDate': row.lastTradeDate.isoformat().split('T')[0] if row.lastTradeDate else None,
                    'expirationDate': row.expirationDate.isoformat().split('T')[0] if row.expirationDate else None,
                    'strike': float(row.strike) if row.strike is not None else None,
                    'lastPrice': float(row.lastPrice) if row.lastPrice is not None else None,
                    'bid': float(row.bid) if row.bid is not None else None,
                    'ask': float(row.ask) if row.ask is not None else None,
                    'change': float(row.change) if row.change is not None else None,
                    'percentChange': float(row.percentChange) if row.percentChange is not None else None,
                    'volume': int(row.volume) if row.volume is not None else None,
                    'openInterest': int(row.openInterest) if row.openInterest is not None else None,
                    'impliedVolatility': float(row.impliedVolatility) if row.impliedVolatility is not None else None,
                    'inTheMoney': bool(row.inTheMoney) if (row.inTheMoney)is not None else None,
                    'contractSize': row.contractSize,
                    'currency': row.currency,
                    'StockName': row.StockName,
                })
            
            cursor2.execute("SELECT 1 FROM sys.tables WHERE name = 'call_options'")
            if not cursor2.fetchone():
                raise Exception("call_options table not found in default DB")
            
            select_call_option_query = "SELECT contractSymbol, lastTradeDate, expirationDate, strike, lastPrice, bid, ask, change, percentChange, volume, openInterest, impliedVolatility, inTheMoney, contractSize, currency, StockName FROM call_options"
            data2 = []

            cursor2.execute(select_call_option_query)
            for row in cursor2.fetchall():
                data2.append({
                    'contractSymbol': row.contractSymbol,
                    'lastTradeDate': row.lastTradeDate.isoformat().split('T')[0] if row.lastTradeDate else None,
                    'expirationDate': row.expirationDate.isoformat().split('T')[0] if row.expirationDate else None,
                    'strike': float(row.strike) if row.strike is not None else None,
                    'lastPrice': float(row.lastPrice) if row.lastPrice is not None else None,
                    'bid': float(row.bid) if row.bid is not None else None,
                    'ask': float(row.ask) if row.ask is not None else None,
                    'change': float(row.change) if row.change is not None else None,
                    'percentChange': float(row.percentChange) if row.percentChange is not None else None,
                    'volume': int(row.volume) if row.volume is not None else None,
                    'openInterest': int(row.openInterest) if row.openInterest is not None else None,
                    'impliedVolatility': float(row.impliedVolatility) if row.impliedVolatility is not None else None,
                    'inTheMoney': bool(row.inTheMoney) if (row.inTheMoney)is not None else None,
                    'contractSize': row.contractSize,
                    'currency': row.currency,
                    'StockName': row.StockName,
                })

            # Combine data
            response_data = {
                'status': 'success',
                'data': {
                    'put_options': data1,
                    'call_options': data2,
                }
            }

            return Response(response_data, status=status.HTTP_200_OK)

        except pyodbc.Error as e:
            logger.error(f"Database error: {e}")
            return Response({
                'status': 'error',
                'message': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        except socket.gaierror as e:
            logger.error(f"DNS resolution error: {e}")
            return Response({
                'status': 'error',
                'message': 'DNS resolution failed'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        except Exception as e:
            logger.error(f"General error: {e}")
            return Response({
                'status': 'error',
                'message': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            if cursor1:
                try:
                    cursor1.close()
                except pyodbc.Error as e:
                    logger.error(f"Error closing cursor1: {e}")
            if cursor2:
                try:
                    cursor2.close()
                except pyodbc.Error as e:
                    logger.error(f"Error closing cursor2: {e}")
            if conn:
                try:
                    conn.close()
                except pyodbc.Error as e:
                    logger.error(f"Error closing connection: {e}")


class SearchStockView(APIView):
    def get(self, request):
        stock_name = request.query_params.get('stock_name', '')
        if not stock_name:
            return Response({
                'status': 'error',
                'message': 'stock_name parameter is required'
            }, status=status.HTTP_400_BAD_REQUEST)

        conn1 = None
        conn2 = None
        conn3 = None
        cursor1 = None
        cursor2 = None
        cursor3 = None
        
        try:
            logger.info("Resolving server name...")
            server_ip = socket.gethostbyname("dash-gtd.database.windows.net")
            logger.info(f"Server resolved to IP: {server_ip}")

            conn_str = get_connection_string(settings.DATABASES['default'])
            conn1 = connect_with_retry(conn_str)
            conn2 = connect_with_retry(conn_str)
            conn3 = connect_with_retry(conn_str)
            cursor1 = conn1.cursor()
            cursor2 = conn2.cursor()
            cursor3 = conn3.cursor()

            cursor1.execute("SELECT 1 FROM sys.tables WHERE name = 'StockData'")
            if not cursor1.fetchone():
                raise Exception("StockData table not found in default DB")
            
            cursor2.execute("SELECT 1 FROM sys.tables WHERE name = 'put_options'")
            if not cursor2.fetchone():
                raise Exception("put_options table not found in default DB")
            
            cursor3.execute("SELECT 1 FROM sys.tables WHERE name = 'call_options'")
            if not cursor3.fetchone():
                raise Exception("call_options table not found in default DB")

            stock_query = """
                SELECT StockName, Date, [Open], High, Low, [Close], Volume 
                FROM StockData 
                WHERE UPPER(StockName) LIKE UPPER(?)
            """
            put_query = """
                SELECT contractSymbol, lastTradeDate, expirationDate, strike, lastPrice, 
                       bid, ask, change, percentChange, volume, openInterest, 
                       impliedVolatility, inTheMoney, contractSize, currency, StockName 
                FROM put_options 
                WHERE UPPER(StockName) LIKE UPPER(?)
            """
            call_query = """
                SELECT contractSymbol, lastTradeDate, expirationDate, strike, lastPrice, 
                       bid, ask, change, percentChange, volume, openInterest, 
                       impliedVolatility, inTheMoney, contractSize, currency, StockName 
                FROM call_options 
                WHERE UPPER(StockName) LIKE UPPER(?)
            """

            search_pattern = f'%{stock_name}%'
            cursor1.execute(stock_query, (search_pattern,))
            stock_data = []
            for row in cursor1.fetchall():
                stock_data.append({
                    'symbol': row.StockName,
                    'date': row.Date.isoformat().split('T')[0] if row.Date else None,
                    'open': float(row.Open) if row.Open is not None else None,
                    'high': float(row.High) if row.High is not None else None,
                    'low': float(row.Low) if row.Low is not None else None,
                    'close': float(row.Close) if row.Close is not None else None,
                    'volume': int(row.Volume) if row.Volume is not None else None
                })

            cursor2.execute(put_query, (search_pattern,))
            put_options = []
            for row in cursor2.fetchall():
                put_options.append({
                    'contractSymbol': row.contractSymbol,
                    'lastTradeDate': row.lastTradeDate.isoformat().split('T')[0] if row.lastTradeDate else None,
                    'expirationDate': row.expirationDate.isoformat().split('T')[0] if row.expirationDate else None,
                    'strike': float(row.strike) if row.strike is not None else None,
                    'lastPrice': float(row.lastPrice) if row.lastPrice is not None else None,
                    'bid': float(row.bid) if row.bid is not None else None,
                    'ask': float(row.ask) if row.ask is not None else None,
                    'change': float(row.change) if row.change is not None else None,
                    'percentChange': float(row.percentChange) if row.percentChange is not None else None,
                    'volume': int(row.volume) if row.volume is not None else None,
                    'openInterest': int(row.openInterest) if row.openInterest is not None else None,
                    'impliedVolatility': float(row.impliedVolatility) if row.impliedVolatility is not None else None,
                    'inTheMoney': bool(row.inTheMoney) if row.inTheMoney is not None else None,
                    'contractSize': row.contractSize,
                    'currency': row.currency,
                    'StockName': row.StockName,
                })

            cursor3.execute(call_query, (search_pattern,))
            call_options = []
            for row in cursor3.fetchall():
                call_options.append({
                    'contractSymbol': row.contractSymbol,
                    'lastTradeDate': row.lastTradeDate.isoformat().split('T')[0] if row.lastTradeDate else None,
                    'expirationDate': row.expirationDate.isoformat().split('T')[0] if row.expirationDate else None,
                    'strike': float(row.strike) if row.strike is not None else None,
                    'lastPrice': float(row.lastPrice) if row.lastPrice is not None else None,
                    'bid': float(row.bid) if row.bid is not None else None,
                    'ask': float(row.ask) if row.ask is not None else None,
                    'change': float(row.change) if row.change is not None else None,
                    'percentChange': float(row.percentChange) if row.percentChange is not None else None,
                    'volume': int(row.volume) if row.volume is not None else None,
                    'openInterest': int(row.openInterest) if row.openInterest is not None else None,
                    'impliedVolatility': float(row.impliedVolatility) if row.impliedVolatility is not None else None,
                    'inTheMoney': bool(row.inTheMoney) if row.inTheMoney is not None else None,
                    'contractSize': row.contractSize,
                    'currency': row.currency,
                    'StockName': row.StockName,
                })

            response_data = {
                'status': 'success',
                'data': {
                    'stock_data': stock_data,
                    'put_options': put_options,
                    'call_options': call_options,
                    'total_stock_rows': len(stock_data),
                    'total_put_rows': len(put_options),
                    'total_call_rows': len(call_options)
                }
            }

            return Response(response_data, status=status.HTTP_200_OK)

        except pyodbc.Error as e:
            logger.error(f"Database error: {e}")
            return Response({
                'status': 'error',
                'message': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        except socket.gaierror as e:
            logger.error(f"DNS resolution error: {e}")
            return Response({
                'status': 'error',
                'message': 'DNS resolution failed'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        except Exception as e:
            logger.error(f"General error: {e}")
            return Response({
                'status': 'error',
                'message': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            for cursor in [cursor1, cursor2, cursor3]:
                if cursor:
                    try:
                        cursor.close()
                    except pyodbc.Error as e:
                        logger.error(f"Error closing cursor: {e}")
            for conn in [conn1, conn2, conn3]:
                if conn:
                    try:
                        conn.close()
                    except pyodbc.Error as e:
                        logger.error(f"Error closing connection: {e}")

class SearchStockName(APIView):
    def get(self, request):
        stock_name = request.query_params.get('stock_name', '')
        if not stock_name:
            return Response({
                'status': 'error',
                'message': 'stock_name parameter is required'
            }, status=status.HTTP_400_BAD_REQUEST)

        conn = None
        cursor = None
        
        try:
            logger.info("Resolving server name...")
            server_ip = socket.gethostbyname("dash-gtd.database.windows.net")
            logger.info(f"Server resolved to IP: {server_ip}")

            conn_str = get_connection_string(settings.DATABASES['default'])
            conn = connect_with_retry(conn_str)
            cursor = conn.cursor()

            cursor.execute("SELECT 1 FROM sys.tables WHERE name = 'StockData'")
            if not cursor.fetchone():
                raise Exception("StockData table not found in default DB")

            stock_query = """
                SELECT DISTINCT StockName FROM StockData 
                WHERE UPPER(StockName) LIKE UPPER(?)
            """

            search_pattern = f'%{stock_name}%'
            cursor.execute(stock_query, (search_pattern,))
            stockname = []
            for row in cursor.fetchall():
                stockname.append({
                    'stocknames': row.StockName
                })

            response_data = {
                'status': 'success',
                'data': {
                    'stock_data': stockname,
                    'total_stock_rows': len(stockname),
                }
            }

            return Response(response_data, status=status.HTTP_200_OK)

        except pyodbc.Error as e:
            logger.error(f"Database error: {e}")
            return Response({
                'status': 'error',
                'message': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        except socket.gaierror as e:
            logger.error(f"DNS resolution error: {e}")
            return Response({
                'status': 'error',
                'message': 'DNS resolution failed'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        except Exception as e:
            logger.error(f"General error: {e}")
            return Response({
                'status': 'error',
                'message': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            if cursor:
                try:
                    cursor.close()
                except pyodbc.Error as e:
                    logger.error(f"Error closing cursor: {e}")
            if conn:
                try:
                    conn.close()
                except pyodbc.Error as e:
                    logger.error(f"Error closing connection: {e}")