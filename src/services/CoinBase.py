import os
import json
import requests
import streamlit as st

from typing import Any, Union

from src.services.log.Logger import _log
from services.AppData import AppData


class CoinBase:
    """
       A base class for handling coin-related operations.
    """

    def __init__(self, api_key=None):
        # Set the API key, either from the environment or directly from the parameter
        self.api_key = api_key or AppData().get_api_key("openweathermap")
        if not self.api_key:
            raise ValueError("API key is required for OpenWeatherMap")

    # --------------------------
    # Data
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

    # --------------------------
    # Utils
    # --------------------------

    @st.cache_data(ttl=86400)
    def _fetch_json(_self, url: str):
        """
        Fetches JSON data from the specified URL.

        Args:
            url (str): The URL to fetch the JSON data from.

        Returns:
            dict: The JSON data as a dictionary.

        """
        response = requests.get(url)
        response.raise_for_status()
        return response.json()