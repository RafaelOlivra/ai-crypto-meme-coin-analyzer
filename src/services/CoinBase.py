import requests

from typing import Any, Union, Optional, Dict, List

from services.log.Logger import _log
from services.AppData import AppData
from lib.LocalCache import cache_handler

DEFAULT_CACHE_TTL = 86400

class CoinBase:
    """
    A base class for handling coin-related operations.
    """

    def __init__(self, api_key=None):
        # Set the API key, either from the environment or directly from the parameter
        self.api_key = api_key or AppData().get_api_key("coinbase")
        if not self.api_key:
            raise ValueError("API key is required for Coinbase")

        self.base_url = ""
        self.session = requests.Session()

    @cache_handler.cache(ttl_s=DEFAULT_CACHE_TTL)
    def _fetch(_self, url: str, method: str = "get", params: Optional[dict] = None, data: Optional[Any] = None, headers: Optional[dict] = None):
        """
        Fetches data from the specified URL using a common API call.
        
        This method handles both GET and POST requests and includes headers.
        """
        if not url.startswith("http"):
            url = _self.base_url + url

        # Add auth headers
        if headers is None:
            headers = {}
        headers["X-API-KEY"] = _self.api_key
        headers["Content-Type"] = "application/json"

        if method.lower() == "get":
            response = _self.session.get(url, params=params, headers=headers)
        elif method.lower() == "post":
            response = _self.session.post(url, data=data, headers=headers)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        response.raise_for_status()
        return response.json()