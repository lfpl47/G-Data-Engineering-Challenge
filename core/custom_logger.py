import logging

class CustomLogger:
    """
    CustomLogger class encapsulates the logging configuration.
    It provides methods to log messages at different levels: debug, info, warning, and error.
    """
    def __init__(self, name: str):
        """
        Initializes the logger with a specific name and sets the logging format and level.
        :param name: The name of the logger, typically the class or module name.
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)

        if not self.logger.handlers:
            self.logger.addHandler(ch)

    def debug(self, message: str):
        """
        Logs a message at DEBUG level.
        :param message: The message to log.
        """
        self.logger.debug(message)

    def info(self, message: str):
        """
        Logs a message at INFO level.
        :param message: The message to log.
        """
        self.logger.info(message)

    def warning(self, message: str):
        """
        Logs a message at WARNING level.
        :param message: The message to log.
        """
        self.logger.warning(message)

    def error(self, message: str):
        """
        Logs a message at ERROR level.
        :param message: The message to log.
        """
        self.logger.error(message)
