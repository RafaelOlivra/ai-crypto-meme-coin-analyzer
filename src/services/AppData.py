import os
import json
import streamlit as st

from typing import Any, Union

from src.services.log.Logger import _log


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
    # CRUD Operations
    # --------------------------

    def save(
        self, type: str, id: str, json: Union[str, dict], replace: bool = False
    ) -> bool:
        """
        Save arbitrary data to a file.

        Args:
            type (str): The type of data being saved.
            id (str): A unique identifier for the data.
            json (Union[str, dict]): The data to be saved, either as a JSON string or a dictionary.
            replace (bool, optional): Whether to replace existing data. Defaults to False.

        Returns:
            bool: True if the data was successfully saved, False otherwise.

        Raises:
            ValueError: If the id is invalid.
        """
        if not type or not id or not json:
            return False

        id = self.sanitize_id(id)
        if not id:
            return False

        folder_map = self._get_storage_map()
        save_path = folder_map.get(type)
        file_path = f"{save_path}/{id}.json"

        if os.path.exists(file_path) and not replace:
            return False

        return self._save_file(file_path, json)

    @st.cache_data(ttl=10)
    def get(_self, type: str, id: str) -> dict:
        """
        Retrieve data from a file.

        This method is cached using Streamlit's caching mechanism with a TTL of 10 seconds.

        Args:
            type (str): The type of data to retrieve.
            id (str): The unique identifier for the data.

        Returns:
            dict: The retrieved JSON data as a dictionary, or None if not found.

        Raises:
            ValueError: If the id is invalid.
        """
        if not type or not id:
            return None

        id = _self.sanitize_id(id)
        if not id:
            return None

        folder_map = _self._get_storage_map()
        save_path = folder_map.get(type)
        file_path = f"{save_path}/{id}.json"

        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                    if isinstance(data, str):
                        data = json.loads(data)
                except Exception as e:
                    _log(f"Error loading data from {file_path}: {e}", level="ERROR")
                    data = None
            return data
        return None

    def get_all(self, type: str, limit: int = 0) -> list:
        """
        Retrieve all data of a specific type.

        Args:
            type (str): The type of data to retrieve.

        Returns:
            list: A list of all data items of the specified type.
        """
        if not type:
            return []

        folder_map = self._get_storage_map()
        save_path = folder_map.get(type)

        if not os.path.exists(save_path):
            return []

        files = os.listdir(save_path)
        data = []

        for file in files:
            # Limit the number of items to retrieve
            if limit and len(data) >= limit:
                break

            id = file.replace(".json", "")
            data.append(self.get(type, id))

        return data

    def get_all_ids(self, type: str) -> list[str]:
        """
        Retrieve all data IDs of a specific type.

        Args:
            type (str): The type of data to retrieve IDs for.

        Returns:
            list: A list of all IDs for the specified data type.
        """
        if not type:
            return []

        folder_map = self._get_storage_map()
        save_path = folder_map.get(type)

        if not os.path.exists(save_path):
            return []

        files = os.listdir(save_path)
        return [file.replace(".json", "") for file in files]

    def update(self, type: str, id: str, key: str, value: str) -> bool:
        """
        Update a specific key in a file.

        Args:
            type (str): The type of data to update.
            id (str): The unique identifier for the data.
            key (str): The key to update in the data.
            value (str): The new value for the key.

        Returns:
            bool: True if the update was successful, False otherwise.

        Raises:
            ValueError: If the id is invalid.
        """
        if not type or not id or not key:
            return False

        data = self.get(type, id)
        if not data:
            return False

        data[key] = value
        return self.save(type, id, data, replace=True)

    def delete(self, type: str, id: str) -> bool:
        """
        Delete data from a file.

        Args:
            type (str): The type of data to delete.
            id (str): The unique identifier for the data to delete.

        Returns:
            bool: True if the deletion was successful, False otherwise.

        Raises:
            ValueError: If the id is invalid.
        """
        if not type or not id:
            return False

        id = self.sanitize_id(id)
        if not id:
            return False

        folder_map = self._get_storage_map()
        save_path = folder_map.get(type)
        file_path = f"{save_path}/{id}.json"

        return self._delete_file(file_path)

    @st.cache_data(ttl=600)
    def count(_self, type: str) -> int:
        """
        Count the number of items of a specific type.

        Args:
            type (str): The type of data to count.

        Returns:
            int: The number of items of the specified type.
        """
        return len(_self.get_all_ids(type))

    # --------------------------
    # File Operations
    # --------------------------

    def _save_file(self, file_path: str, json: Union[str, dict]) -> bool:
        """
        Save data to a file.

        Args:
            file_path (str): The file path to save the data to.
            json (Union[str, dict]): The data to save, either as a JSON string or a dictionary.

        Returns:
            bool: True if saved successfully, False otherwise.
        """
        folder = os.path.dirname(file_path)
        if not os.path.exists(folder):
            os.makedirs(folder)

        if isinstance(json, dict):
            json = json.dumps(json, indent=4)

        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(json)
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
