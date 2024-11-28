import logging
from logging.handlers import RotatingFileHandler
import os


class LoggerSetup:
    """Configures and manages a rotating file logger with up to 2 file copies."""

    def __init__(
        self,
        logger_name: str,
        log_directory: str = "logging",
        log_filename: str = "default.log",
    ):
        """
        Initializes the logger setup with a specified logger name and optional directory and filename.

        Args:
            logger_name (str): The name of the logger to configure.
            log_directory (str): Directory path where logs will be stored. Defaults to 'logging'.
            log_filename (str): Filename for the log file. Defaults to 'default.log'.
        """
        self.logger_name = logger_name
        self.log_directory = log_directory
        self.log_filename = log_filename
        self.script_dir = os.path.dirname(os.path.realpath(__file__))
        self.full_log_path = os.path.join(
            self.script_dir, "..", self.log_directory
        )
        self.logger = logging.getLogger(logger_name)
        self.setup_logging()

    def setup_logging(self):
        """Configures logging to use a rotating file handler."""
        os.makedirs(self.full_log_path, exist_ok=True)

        log_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )

        handler = RotatingFileHandler(
            os.path.join(self.full_log_path, self.log_filename),
            maxBytes=5 * 1024 * 1024,  # 5 MB
            backupCount=1,
        )
        handler.setFormatter(log_formatter)

        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(handler)
        self.logger.propagate = False
