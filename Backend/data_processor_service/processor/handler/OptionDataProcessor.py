import pandas as pd
import logging
from processor.handler.DataPreprocessor import DataPreprocessor
from processor.utils.logConfig import LogConfig

logger = LogConfig()

def OptionDataProcessor(data):
    
    logger.info(f"PROCESSOR CALLED OPTIONDATAPROCESSOR")

    try:
        sym = data[0]["contractSymbol"][:3]

        for value in data:
            value['symbol'] = value["contractSymbol"][:3]

        # Convert values to DataFrame for processing
        data_df = pd.DataFrame(data)
        
        df = data_df[["contractSymbol", "symbol", "lastTradeDate", "strike", "lastPrice", "bid", "ask", "change", "percentChange", "volume", "openInterest", "impliedVolatility", "inTheMoney", "contractSize", "currency", "expirationDate", "type"]]
        
        logger.info(f"Processing options data for symbol: {sym}")
        
        # Initialize preprocessor for options data
        preprocessor = DataPreprocessor(data_type='options')
        
        # Apply comprehensive preprocessing
        processed_data = preprocessor.preprocess_stock_data(df)

        # Additional options-specific validation
        processed_data = _validate_options_specific_data(processed_data)
        
        logger.info(f"Options data processing completed for {sym}")
        logger.info(f"Processed data sending back to Processor: {processed_data}")

        return processed_data
        
    except Exception as e:
        logger.error(f"Options data processing error: {str(e)}")
        # Return original data if processing fails
        return data

def _validate_options_specific_data(data):
    """
    Additional validation specific to options data
    """
    try:
        df = pd.DataFrame(data['values'])
        original_count = len(df)
        
        # Options-specific validation rules can be added here
        # For now, we'll use the same stock validation
        # but this can be extended for options-specific fields like:
        # - strike price validation
        # - expiration date validation
        # - option type validation (call/put)
        # - implied volatility ranges
        
        # Example: If options data has strike prices
        if 'strike' in df.columns:
            # Remove invalid strike prices (negative or zero)
            df = df[df['strike'] > 0]
        
        # Example: If options data has expiration dates
        if 'expiration' in df.columns:
            df['expiration'] = pd.to_datetime(df['expiration'], errors='coerce')
            df = df.dropna(subset=['expiration'])
            # Remove expired options (optional based on business logic)
            # current_date = pd.Timestamp.now()
            # df = df[df['expiration'] >= current_date]
        
        data['values'] = df.to_dict(orient='records')
        
        if len(df) != original_count:
            logger.info(f"Options-specific validation removed {original_count - len(df)} invalid records")
        
        return data
        
    except Exception as e:
        logger.error(f"Options-specific validation error: {str(e)}")
        return data