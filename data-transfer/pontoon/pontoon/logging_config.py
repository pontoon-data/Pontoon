import os
import logging
from logging.handlers import RotatingFileHandler


logger = logging.getLogger(__name__)


def configure_logging(env:str = 'dev'):
    """
    Configure logging for the package based on the environment.
    
    Args:
        env (str): The environment ("dev", "prod", etc.).
    """
 
    # Define log level and format based on the environment
    if env == "prod":
        log_level = logging.INFO
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    else:  # dev environment
        log_level = logging.DEBUG
        log_format = "%(levelname)s: %(message)s"

    # Create the root logger
    package_logger = logging.getLogger(__name__)
    package_logger.setLevel(log_level)

    # Remove existing handlers if reconfiguring
    if package_logger.hasHandlers():
        package_logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter(log_format)
    console_handler.setFormatter(console_formatter)
    package_logger.addHandler(console_handler)

    # if env == "prod":
    #     # Add handler to log somewhere persistent?

