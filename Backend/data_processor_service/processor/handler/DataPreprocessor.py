import pandas as pd
from processor.utils.logConfig import LogConfig

logger = LogConfig()

class DataPreprocessor:
    """
    Comprehensive data preprocessing class for stock market data
    Handles the 5 required preprocessing steps:
    1. Handling missing values
    2. Removing duplicates
    3. Check invalid rows
    4. Fixing data types
    5. Correcting inconsistent formatting
    """
    
    def __init__(self, data_type='stock'):
        self.data_type = data_type
        self.processed_count = 0
        self.invalid_count = 0
        self.duplicate_count = 0
        self.missing_handled_count = 0
        
    def preprocess_stock_data(self, df):
        """
        Main preprocessing pipeline for stock data
        Args:
            data: Dictionary containing stock data with 'values' array
        Returns:
            Processed data dictionary
        """
        logger.info(f"Inside preprocess stock data")
        try:
            
            # Step 1: Fix data types first
            df = self._fix_data_types(df)

            # Step 2: Handle missing values
            df = self._handle_missing_values(df)
            
            # Step 3: Remove duplicates
            df = self._remove_duplicates(df)
            
            # Step 4: Check and handle invalid rows
            df = self._validate_and_clean_rows(df)
            
            # Step 5: Correct inconsistent formatting
            df = self._standardize_formatting(df)
            
            # Convert back to the original structure
            processed_data = df.copy()
            processed_data = df.to_dict(orient='records')
            
            return processed_data
            
        except Exception as e:
            logger.error(f"Preprocessing error: {str(e)}")
            return df
    
    def _handle_missing_values(self, df):
        """Step 1: Handle missing values"""

        logger.info(f"Handeling Missing Value")

        try:
            original_missing = df.isnull().sum().sum()
            
            # Strategy for different columns
            price_columns = ['open', 'high', 'low', 'close']
            
            # For price data, use forward fill then backward fill
            for col in price_columns:
                if col in df.columns:
                    if df[col].isnull().any():
                        # Forward fill first, then backward fill
                        df[col] = df[col].ffill().bfill()
                        
                        # If still missing, use the mean of available data
                        if df[col].isnull().any():
                            mean_value = df[col].mean()
                            df[col] = df[col].fillna(mean_value)
            
            # For volume, fill with 0 or median
            if 'volume' in df.columns and df['volume'].isnull().any():
                median_volume = df['volume'].median()
                df['volume'] = df['volume'].fillna(median_volume if not pd.isna(median_volume) else 0)
            
            final_missing = df.isnull().sum().sum()
            self.missing_handled_count = original_missing - final_missing
            
            logger.info(f"Missing values handled: {self.missing_handled_count} values filled")
            return df
            
        except Exception as e:
            logger.error(f"Missing values handling error: {str(e)}")
            return df
    
    def _remove_duplicates(self, df):
        """Step 2: Remove duplicates"""

        logger.info(f"Removing Duplicate")

        try:
            original_count = len(df)
            
            # Remove duplicates based on datetime (primary key for stock data)
            if 'datetime' in df.columns:
                df = df.drop_duplicates(subset=['datetime'], keep='last')
            else:
                # If no datetime, remove complete duplicates
                df = df.drop_duplicates(keep='last')
            
            self.duplicate_count = original_count - len(df)
            
            if self.duplicate_count > 0:
                logger.info(f"Duplicates removed: {self.duplicate_count} records")
            
            return df
            
        except Exception as e:
            logger.error(f"Duplicate removal error: {str(e)}")
            return df
    
    def _validate_and_clean_rows(self, df):
        """Step 3: Check invalid rows and remove them"""

        logger.info(f"Data validation and cleaning")

        try:
            original_count = len(df)
            invalid_rows = []
            
            # Validate stock data rules
            price_columns = ['open', 'high', 'low', 'close']
            
            # Check if all required price columns exist
            for col in price_columns:
                if col in df.columns:
                    # Remove rows with negative or zero prices
                    invalid_mask = (df[col] <= 0) | df[col].isnull()
                    invalid_rows.extend(df[invalid_mask].index.tolist())
            
            # Validate OHLC relationships
            if all(col in df.columns for col in price_columns):
                # High should be >= Low
                invalid_mask = df['high'] < df['low']
                invalid_rows.extend(df[invalid_mask].index.tolist())
                
                # High should be >= Open and Close
                invalid_mask = (df['high'] < df['open']) | (df['high'] < df['close'])
                invalid_rows.extend(df[invalid_mask].index.tolist())
                
                # Low should be <= Open and Close
                invalid_mask = (df['low'] > df['open']) | (df['low'] > df['close'])
                invalid_rows.extend(df[invalid_mask].index.tolist())
            
            # Validate volume (should be non-negative)
            if 'volume' in df.columns:
                invalid_mask = df['volume'] < 0
                invalid_rows.extend(df[invalid_mask].index.tolist())
            
            # Remove invalid rows
            invalid_rows = list(set(invalid_rows))  # Remove duplicates
            if invalid_rows:
                df = df.drop(invalid_rows)
                self.invalid_count = len(invalid_rows)
                logger.warning(f"Invalid rows removed: {self.invalid_count} records")
            
            logger.info(f"Data validation completed: {len(df)} valid records from {original_count}")
            return df
            
        except Exception as e:
            logger.error(f"Row validation error: {str(e)}")
            return df
    
    def _fix_data_types(self, df):
        """Step 4: Fix data types"""
        logger.info(f"Fixing Data Type")

        try:
            original_count = len(df)
            
            # Convert datetime
            if 'datetime' in df.columns:
                df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
                # Remove rows with invalid dates
                df = df.dropna(subset=['datetime'])
            
            # Convert OHLCV to numeric
            price_columns = ['open', 'high', 'low', 'close']
            for col in price_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Convert volume to integer
            if 'volume' in df.columns:
                df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
                df['volume'] = df['volume'].fillna(0).astype('int64')
            
            logger.info(f"Data types fixed: {len(df)} valid records from {original_count}")
            return df
            
        except Exception as e:
            logger.error(f"Data type fixing error: {str(e)}")
            return df
        
    def _standardize_formatting(self, df):
        """Step 5: Correct inconsistent formatting"""

        logger.info(f"Data Formatting and standardizing")

        try:
            # Standardize datetime format
            if 'datetime' in df.columns:
                df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Standardize price precision to 4 decimal places
            price_columns = ['open', 'high', 'low', 'close']
            for col in price_columns:
                if col in df.columns:
                    df[col] = df[col].round(4)
            
            # Ensure volume is integer
            if 'volume' in df.columns:
                df['volume'] = df['volume'].astype('int64')
            
            # Sort by datetime if available
            if 'datetime' in df.columns:
                df = df.sort_values('datetime').reset_index(drop=True)
            
            self.processed_count = len(df)
            logger.info(f"Formatting standardized for {self.processed_count} records")
            return df
            
        except Exception as e:
            logger.error(f"Formatting standardization error: {str(e)}")
            return df