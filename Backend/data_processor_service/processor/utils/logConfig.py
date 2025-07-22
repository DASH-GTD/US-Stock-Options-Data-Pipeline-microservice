import logging

class LogConfig:
    def __init__(self, name='collector_app'):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO) # Default level

        # Check if handlers already exist to prevent duplicate logs in Gunicorn
        if not self.logger.handlers:
            # Create console handler and set level to debug
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO) # Console handler level

            # Create formatter
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - (%(threadName)s) - (%(process)d) - %(message)s'
            )

            # Add formatter to ch
            ch.setFormatter(formatter)

            # Add ch to logger
            self.logger.addHandler(ch)

    def get_logger(self):
        return self.logger

    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message, exc_info=False):
        self.logger.error(message, exc_info=exc_info)

    def critical(self, message):
        self.logger.critical(message)