import os
import json
import time
from typing import Any, Union

from lib.LocalCache import cache_handler
from services.logger.Logger import _log

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

    def get_api_key(self, key: str, default: str = "") -> str:
        """
        Retrieve an API key from environment variables.
        Falls back to the provided default if not found.
        """
        key_map = {
            "fastapi": "FASTAPI_KEYS",
            "huggingface": "HUGGINGFACE_API_KEY",
            "googlegemini": "GEMINI_API_KEY",  # fixed typo "GEMINY"
            "openai": "OPENAI_API_KEY"
        }

        # Try direct env var match
        env_key = key.upper()
        if env_key in os.environ:
            return os.environ[env_key]

        # Try mapped env var
        mapped_env = key_map.get(key)
        if mapped_env and mapped_env in os.environ:
            return os.environ[mapped_env]

        # Fallback to default
        return default
    
    # --------------------------
    # App State Handling
    # --------------------------

    def get_state(self, key: str) -> Any:
        """
        Retrieve a value from the session state JSON file.

        Args:
            key (str): The key for the state variable to retrieve.

        Returns:
            Any: The value of the state variable, or None if the key doesn't exist or is expired.
        """
        state_file = self._get_storage_map()["session_state"]
        if os.path.exists(state_file):
            with open(state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                entry = data.get(key)
                if entry is None:
                    return None

                # Handle TTL expiration
                expires_at = entry.get("expires_at")
                if expires_at is not None and time.time() > expires_at:
                    # Expired -> remove and resave file
                    data.pop(key, None)
                    self._save_file(state_file, data)
                    return None

                return entry.get("value")
        return None

    def set_state(self, key: str, value: Any, ttl: int = None) -> bool:
        """
        Save a key-value pair to the session state JSON file with optional TTL.

        Args:
            key (str): The key for the state variable.
            value (Any): The value to save.
            ttl (int, optional): Time-to-live in seconds. If None, data never expires.

        Returns:
            bool: True if saved successfully, False otherwise.
        """
        if not key or not isinstance(key, str):
            _log(f"Invalid key: {key}", level="ERROR")
            return False

        # If value is none, clear the state
        if value is None:
            return self.clear_state(key)

        state_file = self._get_storage_map()["session_state"]
        data = {}
        if os.path.exists(state_file):
            with open(state_file, "r", encoding="utf-8") as f:
                data = json.load(f)

        expires_at = time.time() + ttl if ttl is not None else None
        data[key] = {"value": value, "expires_at": expires_at}

        return self._save_file(state_file, data)
    
    def clear_state(self, key: str) -> bool:
        """
        Clear a specific key from the session state JSON file.

        Args:
            key (str): The key for the state variable to clear.

        Returns:
            bool: True if cleared successfully, False otherwise.
        """
        state_file = self._get_storage_map()["session_state"]
        if os.path.exists(state_file):
            with open(state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if key in data:
                    del data[key]
                    return self._save_file(state_file, data)
        return False

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
            "permanent_data": f"{permanent_storage_dir}/data"
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
