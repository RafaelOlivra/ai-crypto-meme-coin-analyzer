import os
import logging
import json

from logging.handlers import RotatingFileHandler


class SimpleLogger:
    """
    A utility class for logging messages with support for log rotation.

    This class provides a structured way to log messages to a file, with the
    ability to handle rotating log files and optionally include additional
    objects in the log messages. Log settings can be customized, and the
    log directory is retrieved from a configuration file.

    Attributes:
        logger (logging.Logger): The main logger object used for logging messages.

    Methods:
        log(message: str, obj: Any = None, level: str = "INFO"):
            Log a message at a specified level.
        log_info(message: str, obj: Any = None):
            Log an informational message.
        log_warning(message: str, obj: Any = None):
            Log a warning message.
        log_error(message: str, obj: Any = None):
            Log an error message.
        log_debug(message: str, obj: Any = None):
            Log a debug message.
        get_log_dir() -> Optional[str]:
            Retrieve the log folder path from the configuration file.
    """

    def __init__(
        self,
        log_filename="app.log",
        max_size=1_000_000,
        backup_count=5,
    ):
        """
        Initialize the SimpleLogger class.

        Args:
            log_filename (str): The name of the log file.
            max_size (int): The maximum size of a log file before rotation occurs (in bytes).
            backup_count (int): The number of backup files to keep.

        Raises:
            ValueError: If the log directory is not found in the configuration file.
        """

        # Get the log folder from the config file
        log_folder = self.get_log_dir()

        if not log_folder:
            raise ValueError("Log directory not found in config file")

        # Create the log folder if it does not exist
        if not os.path.exists(log_folder):
            os.makedirs(log_folder)

        # Define the full log file path
        log_path = os.path.join(log_folder, log_filename)

        # Set up logging with file rotation
        self.logger = logging.getLogger("SimpleLogger")
        self.logger.setLevel(logging.DEBUG)

        # Create a rotating file handler
        handler = RotatingFileHandler(
            log_path, maxBytes=max_size, backupCount=backup_count, encoding="utf-8"
        )

        # Define the log format
        formatter = logging.Formatter("[%(asctime)s][%(levelname)s] %(message)s")
        handler.setFormatter(formatter)

        # Add the handler to the logger
        self.logger.addHandler(handler)

    # --------------------------
    # Log functions
    # --------------------------

    def log(self, message, obj=None, level="INFO"):
        """
        Log a message at the specified level, optionally including an object.

        Args:
            message (str): The log message.
            obj (Any): An optional object to include in the log message.
            level (str): The log level (e.g., "INFO", "WARNING", "ERROR", "DEBUG").
        """
        if level == "WARNING":
            self.log_warning(message, obj)
        elif level == "ERROR":
            self.log_error(message, obj)
        elif level == "DEBUG":
            self.log_debug(message, obj)
        else:
            self.log_info(message, obj)

    def log_info(self, message, obj=None):
        """
        Log an informational message.

        Args:
            message (str): The log message.
            obj (Any): An optional object to include in the log message.
        """
        self._log_with_object(logging.INFO, message, obj)

    def log_warning(self, message, obj=None):
        """
        Log a warning message.

        Args:
            message (str): The log message.
            obj (Any): An optional object to include in the log message.
        """
        self._log_with_object(logging.WARNING, message, obj)

    def log_error(self, message, obj=None):
        """
        Log an error message.

        Args:
            message (str): The log message.
            obj (Any): An optional object to include in the log message.
        """
        self._log_with_object(logging.ERROR, message, obj)

    def log_debug(self, message, obj=None):
        """
        Log a debug message.

        Args:
            message (str): The log message.
            obj (Any): An optional object to include in the log message.
        """
        self._log_with_object(logging.DEBUG, message, obj)

    def _log_with_object(self, level, message, obj):
        """
        Log a message containing an object.

        Args:
            level (str): The log level (e.g., "INFO", "WARNING", "ERROR", "DEBUG").
            message (str): The log message.
            obj (Any): An optional object to include in the log message.
        """
        # Convert the object to a string if it exists, using JSON if possible
        if obj is not None:
            try:
                obj_str = json.dumps(
                    obj, default=str
                )  # Use default=str to handle non-serializable objects
            except TypeError:
                obj_str = repr(obj)  # Fallback to repr if JSON serialization fails
            message = f"{message} | {obj_str}"

        # Remove any newlines from the message
        if type(message) == str:
            message = message.replace("\n", "\\n")

        try:
            # Log the message at the specified level
            if level == logging.INFO:
                self.logger.info(message)
            elif level == logging.WARNING:
                self.logger.warning(message)
            elif level == logging.ERROR:
                self.logger.error(message)
            elif level == logging.DEBUG:
                self.logger.debug(message)
        except IOError as e:
            # Fallback to printing to console if the log file is locked
            print(f"ERROR: Could not write to log file. Reason: {e}")

    # --------------------------
    # System Utils
    # --------------------------
    def get_log_dir(self):
        """
        Retrieve the log folder path from the configuration file.

        The method first checks if the log directory is overridden in environment
        variables. If not, it retrieves the path from the JSON configuration file.

        Returns:
            Optional[str]: The log folder path, or None if not found.
        """
        key = "log_dir"
        config_file = "src/config/cfg.json"

        # Allow overriding config values with environment variables
        env_key = f"__CONFIG_OVERRIDE_{key}"

        # Check if the key exists in environment variables
        if env_key in os.environ:
            return os.getenv(env_key)

        # Otherwise, load from JSON config file
        if os.path.exists(config_file):
            with open(config_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data.get(key)

        return None


# ----------------------------
# Generic Log Function Export
# ----------------------------
def _log(message, obj=None, level="INFO"):
    """
    A generic function to log a message at the specified level.

    Args:
        message (str): The log message.
        obj (Any): An optional object to include in the log message.
        level (str): The log level (e.g., "INFO", "WARNING", "ERROR", "DEBUG").
    """

    # Add global logger if not already defined
    global logger

    if "logger" not in globals() or logger is None:
        logger = SimpleLogger()

    return logger.log(message=message, obj=obj, level=level)
