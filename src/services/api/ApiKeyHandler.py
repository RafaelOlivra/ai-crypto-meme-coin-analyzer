from fastapi import HTTPException, Security
from fastapi.security.api_key import APIKeyHeader

from typing import Dict

from services.AppData import AppData

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


class ApiKeyHandler:
    """
    API key handler class to manage API keys for FastAPI.
    """

    def __init__(self):
        self.header = api_key_header
        pass

    def parse_keys(self, keys: str) -> list[dict[str, int]]:
        """
        Parse raw API keys string to a list of dictionaries containing token and user_id.

        Args:
            keys (str): The API keys string formatted as ABCDEFGHIJKLMNOPQRSTUVWXYZ012345:user_id, ...

        Returns:
            list: A list of dictionaries containing a list of tokens and user_ids
        """
        keys = keys.split(",")
        keys_list = []
        for key in keys:
            key = self.parse_key(key)
            if key:
                keys_list.append(key)
        return keys_list

    def parse_key(self, key: str) -> dict[str, int]:
        """
        Parse API key formatted as ABCDEFGHIJKLMNOPQRSTUVWXYZ012345:user_id

        Args:
            key (str): The API key formatted as ABCDEFGHIJKLMNOPQRSTUVWXYZ012345:user

        Returns:
            dict: A dictionary containing the token and user_id
        """
        key_parts = key.split(":")

        if len(key_parts) != 2:
            return None

        token, user_id = key_parts

        # Clean invalid characters from token
        _token = "".join(filter(str.isalnum, token))

        # Validate token and user_id
        if _token and len(_token) == 32 and user_id.isnumeric():
            return {_token: int(user_id)}

    def get_available_keys(self) -> list[dict[str, int]]:
        """
        Get available API keys from AppData.

        Returns:
            list: A list of dictionaries containing a list of tokens and user_ids
        """
        keys = self._get_raw_keys()
        if not keys:
            raise HTTPException(status_code=500, detail="No valid API keys available")
        keys = self.parse_keys(keys)
        return keys

    def validate_key(self, api_key: str = Security(api_key_header)) -> str:
        """
        Validate API key

        Args:
            api_key (str): The API key to validate

        Returns:
            str: The valid API key if it exists

        Raises:
            HTTPException: If the API key is invalid
        """
        keys = self.get_available_keys()
        keys = self._join_keys(keys)
        if not keys:
            raise HTTPException(status_code=500, detail="No valid API keys available")
        if api_key not in keys:
            raise HTTPException(status_code=403, detail="Invalid API key")
        return api_key

    def get_user_id(self, api_key: str) -> int:
        """
        Get user_id from API key.

        Args:
            api_key (str): The API key to get the user_id from

        Returns:
            int: The user_id
        """
        keys = self.parse_key(api_key)
        return list(keys.values())[0]

    def _get_raw_keys(self):
        """
        Get raw API keys from AppData.

        Returns:
            str: The raw API keys string
        """
        return AppData().get_api_key("fastapi")

    def _join_keys(self, keys: list[Dict[str, int]]) -> list[str]:
        """
        Join API keys into a list of strings

        Args:
            keys (list): The list of dictionaries containing tokens and user_ids

        Returns:
            list: A list of strings containing tokens and user_ids
        """
        return [":".join([k, str(v)]) for key in keys for k, v in key.items()]
