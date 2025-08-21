import os
import json
from lib.LocalCache import cache_handler

from typing import Any, Union
from services.log.Logger import _log

DEFAULT_CACHE_TTL = 60

class AppData:
    """
    A class to handle data storage and retrieval from configuration files and environment variables.

    This class provides methods for managing configuration data, API keys, and various types of
    application data (e.g., trip data, attractions). It supports CRUD operations on JSON files
    and interacts with environment variables for secure storage of API keys.
    """

    def __init__(self):
        """
        Initialize the AppData class.

        No specific initialization is required as the class mainly consists of static methods.
        """
        pass

    # --------------------------
    # System Utils
    # --------------------------

    def get_config(self, key: str) -> Any:
        """
        Retrieve configuration data from the config JSON file, with the option
        to override values using environment variables.

        Args:
            key (str): The specific key in the configuration file.

        Returns:
            Any: The configuration value, or None if the key does not exist.
        """
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

        return None  # Return None if the key is not found in either place

    def get_api_key(self, key: str) -> str:
        """
        Retrieve an API key from environment variables.
        This allows mapping service names to their corresponding API keys.
        It will fallback to the environment variable with the same name as the key.

        Args:
            key (str): The key name of the API service (e.g., 'googlemaps').

        Returns:
            str: The corresponding API key, or None if not found.

        Note:
            This method uses a predefined mapping of service names to environment variable names.
            Make sure the corresponding environment variables are set before calling this method.
        """
        key_map = {
            "scraperapi": "SCRAPER_API_KEY",
            "fastapi": "FASTAPI_KEYS",
            "huggingface": "HUGGINGFACE_API_KEY",
            "googlegemini": "GEMINY_API_KEY",
            "openai": "OPENAI_API_KEY",
            "coinbase": "COINBASE_API_KEY"
        }
        
        # If the key is not found we can try a fallback by looking for an
        # environment variable with the same name as the key.
        env_key = f"{key.upper()}"
        if env_key in os.environ:
            return os.getenv(env_key)

        try:
            if not key_map.get(key):
                raise ValueError("API key not found.")
        except ValueError as e:
            _log(f"Error retrieving API key: {e}", level="ERROR")
            return None

        return os.getenv(key_map.get(key))
    
    # --------------------------
    # State Handling
    # --------------------------

    def get_state(self, key: str) -> Any:
        """
        Retrieve a value from the session state JSON file.

        Args:
            key (str): The key for the state variable to retrieve.

        Returns:
            Any: The value of the state variable, or None if the key or file does not exist.
        """
        state_file = self._get_storage_map()["session_state"]
        if os.path.exists(state_file):
            with open(state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data.get(key)
        return None

    def set_state(self, key: str, value: Any) -> bool:
        """
        Save a key-value pair to the session state JSON file.

        Args:
            key (str): The key for the state variable.
            value (Any): The value to save.

        Returns:
            bool: True if saved successfully, False otherwise.
        """
        state_file = self._get_storage_map()["session_state"]
        data = {}
        if os.path.exists(state_file):
            with open(state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        
        data[key] = value
        
        return self._save_file(state_file, data)

    # --------------------------
    # File Operations
    # --------------------------

    def _save_file(self, file_path: str, data: Union[str, dict]) -> bool:
        """
        Save data to a file, handling both dictionary and string inputs.

        Args:
            file_path (str): The file path to save the data to.
            data (Union[str, dict]): The data to save, either as a JSON string or a dictionary.

        Returns:
            bool: True if saved successfully, False otherwise.
        """
        folder = os.path.dirname(file_path)
        if not os.path.exists(folder):
            try:
                os.makedirs(folder)
            except OSError as e:
                _log(f"Error creating directory {folder}: {e}", level="ERROR")
                return False

        try:
            if isinstance(data, dict):
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=4)
            elif isinstance(data, str):
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(data)
            else:
                _log(f"Unsupported data type: {type(data)}", level="ERROR")
                return False
            return True
        except Exception as e:
            _log(f"Error saving data to file: {e}", level="ERROR")
            return False

    def _delete_file(self, file_path: str) -> bool:
        """
        Delete a file.

        Args:
            file_path (str): The file path to delete.

        Returns:
            bool: True if deleted successfully, False otherwise.
        """
        if os.path.exists(file_path):
            os.remove(file_path)
            return True
        return False

    # --------------------------
    # Utils
    # --------------------------

    def _get_storage_map(self) -> dict:
        """
        Define the storage map based on configuration.

        Returns:
            dict: A dictionary with storage directory mappings.
        """
        temp_storage_dir = self.get_config("temp_storage_dir")
        permanent_storage_dir = self.get_config("permanent_storage_dir")
        return {
            "image_cache": f"{temp_storage_dir}/_image-cache",
            "session_state": f"{temp_storage_dir}/_session-state.json",
        }

    def get_assets_dir(self) -> str:
        """
        Get the directory path for storing assets.

        Returns:
            str: The directory path for storing assets.
        """
        return self.get_config("assets_dir")

    def sanitize_id(self, id: str) -> str:
        """
        Sanitize the ID to ensure it's valid and safe to use in filenames.

        Args:
            id (str): The ID to sanitize.

        Returns:
            str: The sanitized ID.

        Raises:
            ValueError: If the ID is invalid.
        """
        try:
            if not id:
                raise ValueError("ID is empty.")
            if not isinstance(id, str):
                raise ValueError("ID is not a string.")
            if len(id) < 5:
                raise ValueError("ID is too short.")
            if len(id) > 50:
                raise ValueError("ID is too long.")

            id = id.lower().replace(" ", "_")
            id = "".join(c for c in id if c.isalnum() or c == "-" or c == "_")

            return id

        except ValueError as e:
            raise ValueError(f"Invalid ID: {e}")
