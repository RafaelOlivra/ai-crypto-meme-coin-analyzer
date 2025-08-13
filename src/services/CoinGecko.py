import os
import json
import requests
import streamlit as st

from typing import Any, Union, Optional, Dict, List

class CoinGecko:
    """
    A class for interacting with the CoinGecko API.
    """

    def __init__(self):
        self.base_url = "https://api.coingecko.com/api/v3"
        self.session = requests.Session()
        
    def get_coin_market_data(self, vs_currency: str = "usd", category: Optional[str] = None, per_page: int = 250, page: int = 1) -> List[Dict]:
        """
        Get coin market data from CoinGecko.

        Args:
            vs_currency (str): The currency to compare against (e.g., 'usd').
            category (str): Optional category ID to filter coins.
            per_page (int): Number of results per page.
            page (int): Page number.

        Returns:
            list: List of coin market data.
        """
        params = {
            "vs_currency": vs_currency,
            "per_page": per_page,
            "page": page
        }
        if category:
            params["category"] = category

        return self._fetch_json("/coins/markets", params=params)

    def get_solana_meme_coins_market_data(self, vs_currency: str = "usd", per_page: int = 250, page: int = 1) -> List[Dict]:
        """
        Get Solana Meme Coins market data.

        Args:
            vs_currency (str): The currency to compare against (e.g., 'usd').
            per_page (int): Number of results per page.
            page (int): Page number.

        Returns:
            list: List of Solana meme coins.
        """
        # Get category list
        categories = self._fetch_json("/coins/categories/list")
        category_id = None
        for cat in categories:
            if cat["name"].strip().lower() == "solana meme":
                category_id = cat["category_id"]
                break

        if not category_id:
            raise ValueError("Category 'Solana Meme Coins' not found in CoinGecko.")

        return self.get_coin_market_data(vs_currency=vs_currency, category=category_id, per_page=per_page, page=page)

    def get_coin_details(self, coin_id: str, localization: bool = False, sparkline: bool = False) -> Dict:
        """
        Get detailed information for a single coin from CoinGecko.

        Args:
            coin_id (str): The CoinGecko API ID of the coin (e.g., 'bitcoin').
            localization (bool): Set to True to include localized data. Defaults to False.
            sparkline (bool): Set to True to include a 7-day sparkline. Defaults to False.

        Returns:
            dict: A dictionary containing the coin's detailed information.
        """
        params = {
            "localization": "true" if localization else "false",
            "sparkline": "true" if sparkline else "false"
        }

        return self._fetch_json(f"/coins/{coin_id}", params=params)

    @st.cache_data(ttl=86400)
    def _fetch_json(_self, url: str, params: Optional[dict] = None):
        """
        Fetches JSON data from the specified URL.
        """
        if not url.startswith("http"):
            url = _self.base_url + url

        response = _self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()