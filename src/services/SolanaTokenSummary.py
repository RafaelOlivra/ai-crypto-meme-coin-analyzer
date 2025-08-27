import time
import random
import requests
import pandas as pd
from requests.exceptions import RequestException
from datetime import datetime, timezone
from typing import Optional, Any, List

# Add aiohttp and asyncio for asynchronous operations
import aiohttp
import asyncio

# Assuming these modules exist in your project
from services.AppData import AppData
from lib.LocalCache import cache_handler
from lib.Utils import Utils
from services.logger.Logger import _log

DEFAULT_CACHE_TTL = 300
RPC_CACHE_TTL = 2
MINUTE_IN_SECONDS = 60
DAYS_IN_SECONDS = 24 * 60 * 60

class SolanaTokenSummary:
    """
    SolanaTokenSummary is a class designed to retrieve and aggregate comprehensive
    data about Solana tokens from various sources, including Solana's own RPC nodes
    and third-party APIs like Birdeye, Dexscreener, Solscan, and RugCheck.
    """
    def __init__(self, rpc_endpoints: Optional[list] = None):
        """
        Initializes the SolanaTokenSummary instance.

        Args:
            rpc_endpoints (Optional[list], optional): A list of RPC endpoint URLs.
                Can also be a comma-separated string. Defaults to the value
                from AppData().get_env_var("RPC_NODE_ENDPOINTS").
        """
        # Multiple RPC endpoints can be provided as a comma-separated string or a list.
        endpoints = rpc_endpoints or AppData().get_env_var(
            "RPC_NODE_ENDPOINTS",
            "https://api.mainnet-beta.solana.com"
        )
        if isinstance(endpoints, str):
            self.rpc_endpoints = [e.strip() for e in endpoints.split(",")]
        elif isinstance(endpoints, list):
            self.rpc_endpoints = endpoints
        else:
            raise ValueError("rpc_endpoints must be a URL string or list")
        
        # Synchronous session for standard methods
        self.session = requests.Session()
        
        # Asynchronous session for high-performance methods
        self._async_session = None

        self.birdeye_api_key = AppData().get_api_key("birdeye_api_key")
        self.solscan_api_key = AppData().get_api_key("solscan_api_key")
        self.instance_id = Utils.hash(self.rpc_endpoints) # For caching

    # --------------------------
    # Solana RPC info
    # --------------------------

    @cache_handler.cache(ttl_s=DAYS_IN_SECONDS)
    def _rpc_get_mint_info(self, mint_address: str) -> Optional[dict]:
        """
        Retrieves mint account information from a Solana RPC node.

        Args:
            mint_address (str): The public key of the token's mint address.

        Returns:
            Optional[dict]: A dictionary containing the parsed mint account
                info, or None if the data is not found.
        """
        data = self._rpc_fetch("getAccountInfo", [mint_address, {"encoding": "jsonParsed"}])
        try:
            return data["result"]["value"]["data"]["parsed"]["info"]
        except (KeyError, TypeError):
            return None
        
    @cache_handler.cache(ttl_s=MINUTE_IN_SECONDS)
    def _rpc_get_token_supply(self, mint_address: str) -> int:
        """
        Retrieves the total token supply for a given mint address.

        Args:
            mint_address (str): The public key of the token's mint address.

        Returns:
            int: The total token supply. Returns 0 if the data is not found.
        """
        data = self._rpc_fetch("getTokenSupply", [mint_address])
        try:
            return int(data["result"]["value"]["uiAmount"])
        except (KeyError, TypeError):
            return 0
    
    @cache_handler.cache(ttl_s=MINUTE_IN_SECONDS)
    def _rpc_get_largest_accounts(self, mint_address: str) -> List[dict]:
        """
        Retrieves the largest token holders for a given mint address.

        Args:
            mint_address (str): The public key of the token's mint address.

        Returns:
            List[dict]: A list of dictionaries, where each dictionary
                represents a token holder. Returns an empty list on failure.
        """
        data = self._rpc_fetch("getTokenLargestAccounts", [mint_address])
        try:
            return data["result"]["value"]
        except (KeyError, TypeError):
            return []
        
    def _rpc_check_nomint(self, mint_info: dict) -> bool:
        """
        Checks if the mint authority for a token has been revoked.

        Args:
            mint_info (dict): The parsed mint information.

        Returns:
            bool: True if the mint authority is None, indicating it has been
                revoked (no more tokens can be minted).
        """
        return mint_info.get("mintAuthority") is None
    
    @cache_handler.cache(ttl_s=DAYS_IN_SECONDS)
    def _rpc_estimate_wallet_age(self, wallet_address: str) -> int:
        """
        Estimates the wallet age for a single wallet address by finding
        its first transaction.

        Args:
            wallet_address (str): The wallet address to check.

        Returns:
            int: The estimated age of the wallet in days, or -1 if not found.
        """
        try:
            age = self._rpc_estimate_wallet_ages([wallet_address])[0]
        except IndexError:
            age = -1
        return age
    

    @cache_handler.cache(ttl_s=DAYS_IN_SECONDS)
    def _rpc_estimate_wallet_ages(self, wallet_addresses: List[str]) -> List[int]:
        """
        Estimates the age for a list of wallets concurrently by finding their
        first transaction. This function uses asyncio to perform the checks
        in parallel.

        Args:
            wallet_addresses (List[str]): The wallet addresses to check.

        Returns:
            List[int]: A list of estimated ages for each wallet in days, or -1
                for wallets where the age could not be determined.
        """
        tasks = [
            self._rpc_estimate_wallet_age_async(wallet)
            for wallet in wallet_addresses
        ]
        results = asyncio.run(self._rpc_run_async_tasks(tasks))
        return results

    @cache_handler.cache(ttl_s=DAYS_IN_SECONDS)
    async def _rpc_estimate_wallet_age_async(self, wallet_address: str) -> int:
        """
        Asynchronously gets the wallet age (days since first transaction) by
        paginating through transaction signatures.

        Args:
            wallet_address (str): The wallet address to check.

        Returns:
            int: The estimated age of the wallet in days, or -1 if not found.
        """
        before: Optional[str] = None
        oldest_sig: Optional[str] = None

        # Page through until we hit the oldest tx
        while True:
            data = await self._rpc_fetch_async(
                "getSignaturesForAddress",
                [wallet_address, {"limit": 1000, "before": before}]
            )
            signatures = data.get("result", [])
            if not signatures:
                break

            oldest_sig = signatures[-1]["signature"]
            before = oldest_sig  # paginate further

            # If less than requested limit, we've reached the end
            if len(signatures) < 1000:
                break

        if not oldest_sig:
            return -1

        # Fetch transaction details to get blockTime
        tx_data = await self._rpc_fetch_async("getTransaction", [oldest_sig, {"encoding": "json"}])
        tx = tx_data.get("result", {})
        block_time = tx.get("blockTime")

        if block_time is None:
            return -1

        first_tx_time = datetime.fromtimestamp(block_time, tz=timezone.utc)
        now = datetime.now(timezone.utc)
        age_days = (now - first_tx_time).days

        return age_days
    
    @cache_handler.cache(ttl_s=RPC_CACHE_TTL)
    def _rpc_fetch(self, method: str, params: list) -> dict:
        """
        Fetches data from a random Solana RPC endpoint with retry logic.
        This is a synchronous version for compatibility.

        Args:
            method (str): The RPC method to call (e.g., 'getAccountInfo').
            params (list): The parameters for the RPC method.

        Returns:
            dict: The JSON response from the RPC endpoint, or an empty
                dictionary on failure.
        """
        max_retries = 3
        for attempt in range(max_retries):
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params
            }

            # Pick a random endpoint for this attempt
            rpc_url = random.choice(self.rpc_endpoints)

            try:
                response = self.session.post(rpc_url, json=payload, timeout=10)
                response.raise_for_status()
                return response.json()
            except RequestException as e:
                _log(
                    f"Solana RPC fetch error from {rpc_url} "
                    f"on attempt {attempt + 1}/{max_retries}: {e}",
                    level="ERROR"
                )
                if attempt < max_retries - 1:
                    _log("Retrying in 3 seconds with another endpoint...", level="INFO")
                    time.sleep(3)

        _log(f"All {max_retries} attempts failed for method {method}.", level="ERROR")
        return {}
    
    @cache_handler.cache(ttl_s=RPC_CACHE_TTL)
    async def _rpc_fetch_async(self, method: str, params: list) -> dict:
        """
        Fetches data from multiple Solana RPC endpoints concurrently.
        This asynchronous version is designed for high performance. It
        sends requests to all available endpoints and returns the result
        from the first one that responds successfully.

        Args:
            method (str): The RPC method to call.
            params (list): The parameters to pass to the RPC method.

        Returns:
            dict: The JSON response from the first successful RPC call,
                or an empty dictionary on failure.
        """
        tasks = []
        async with aiohttp.ClientSession() as session:
            for rpc_url in self.rpc_endpoints:
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": method,
                    "params": params
                }
                tasks.append(
                    asyncio.create_task(
                        session.post(rpc_url, json=payload, timeout=10)
                    )
                )

            done, pending = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED,
                timeout=10
            )
            
            # Cancel all other pending tasks immediately
            for task in pending:
                task.cancel()
            
            # Check for a successful response and return it
            for task in done:
                try:
                    response = await task
                    response.raise_for_status()
                    return await response.json()
                except Exception:
                    continue
        
        _log(f"All async attempts failed for method {method}.", level="ERROR")
        return {}
    
    async def _rpc_run_async_tasks(self, tasks: List) -> List[int]:
        """
        Helper to run a list of async tasks and handle exceptions.

        Args:
            tasks (List): A list of async tasks to run.

        Returns:
            List[int]: A list of results from the tasks. If a task fails,
                the result is -1.
        """
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle exceptions gracefully
        task_results = []
        for result in results:
            if isinstance(result, Exception):
                _log(f"Error processing wallet: {result}", level="ERROR")
                task_results.append(-1)
            else:
                task_results.append(result)
        return task_results

    # --------------------------
    # RUG CHECK Info
    # --------------------------
    
    def _rugcheck_get_token_info(self, mint_address: str) -> Optional[dict]:
        """
        Retrieves the main token report from the RugCheck API.

        Args:
            mint_address (str): The token's mint address.

        Returns:
            Optional[dict]: The token's full report, or None if not found.
        """
        return self._rugcheck_fetch(mint_address)

    def _rugcheck_check_mint_authority(self, mint_address: str) -> bool:
        """
        Checks if the mint authority exists for a token using the RugCheck API.

        Args:
            mint_address (str): The token's mint address.

        Returns:
            bool: True if a mint authority is found, False otherwise.
        """
        token_info = self._rugcheck_get_token_info(mint_address)
        if not token_info:
            return False
        return token_info.get("token", {}).get("mintAuthority") is not None

    def _rugcheck_get_token_risks(self, mint_address: str) -> list[str]:
        """
        Gets a list of identified risks for a token from the RugCheck API.

        Args:
            mint_address (str): The token's mint address.

        Returns:
            list[str]: A list of risk names. Returns an empty list on failure.
        """
        token_info = self._rugcheck_get_token_info(mint_address)
        if not token_info:
            return []
        risks = token_info.get("risks", [])
        return [risk["name"] for risk in risks]

    def _rugcheck_check_is_mutable(self, mint_address: str) -> bool:
        """
        Checks if the token's metadata is mutable according to RugCheck.

        Args:
            mint_address (str): The token's mint address.

        Returns:
            bool: True if the metadata is mutable, False otherwise.
        """
        token_info = self._rugcheck_get_token_info(mint_address)
        if not token_info:
            return False
        return token_info.get("tokenMeta", {}).get("isMutable") is not None

    def _rugcheck_check_freeze_authority(self, mint_address: str) -> bool:
        """
        Checks if a freeze authority exists for the token using the RugCheck API.

        Args:
            mint_address (str): The token's mint address.

        Returns:
            bool: True if a freeze authority is found, False otherwise.
        """
        token_info = self._rugcheck_get_token_info(mint_address)
        if not token_info:
            return False
        return token_info.get("token", {}).get("freezeAuthority") is not None

    def _rugcheck_get_market_data(self, mint_address: str, pair_address: str) -> Optional[dict]:
        """
        Retrieves market-specific data for a token-pair from RugCheck.

        Args:
            mint_address (str): The token's mint address.
            pair_address (str): The liquidity pool pair address.

        Returns:
            Optional[dict]: The market data for the specified pair, or None if not found.
        """
        data = self._rugcheck_fetch(mint_address)
        markets = data.get("markets", {})
        if not markets:
            return None
        for market in markets:
            if market.get("pubkey") == pair_address:
                return market
        return None
    
    def _rugcheck_get_liquidity_locked(self, mint_address: str, pair_address: str) -> bool:
        """
        Checks the amount of liquidity locked for a given token pair.

        Args:
            mint_address (str): The token's mint address.
            pair_address (str): The liquidity pool pair address.

        Returns:
            bool: The amount of locked liquidity, in tokens. Returns 0 if none is found.
        """
        market_data = self._rugcheck_get_market_data(mint_address, pair_address)
        if not market_data:
            return False
        return market_data.get("lp", {}).get("lpLocked", 0)
    
    def _rugcheck_is_liquidity_locked(self, mint_address: str, pair_address: str) -> bool:
        """
        Checks if liquidity is locked for a given token pair.

        Args:
            mint_address (str): The token's mint address.
            pair_address (str): The liquidity pool pair address.

        Returns:
            bool: True if liquidity is locked, False otherwise.
        """
        return self._rugcheck_get_liquidity_locked(mint_address, pair_address) > 1

    @cache_handler.cache(ttl_s=DEFAULT_CACHE_TTL, invalidate_if_return = {})
    def _rugcheck_fetch(self, mint_address: str) -> dict:
        """
        Fetches a token report from the RugCheck API.

        Args:
            mint_address (str): The token's mint address.

        Returns:
            dict: The JSON response from the API, or an empty dictionary on error.
        """
        url = f"https://api.rugcheck.xyz/v1/tokens/{mint_address}/report"
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            _log(f"RugCheck fetch error: {e}", level="ERROR")
            return {}

    # --------------------------
    # Dexscreener Info
    # --------------------------

    def _dexscreener_get_token_info(self, mint_address: str) -> Optional[dict]:
        """
        Retrieves all pairs associated with a token from Dexscreener.

        Args:
            mint_address (str): The token's mint address.

        Returns:
            Optional[dict]: A list of all pairs found for the token.
        """
        data = self._dexscreener_fetch(mint_address)
        return data.get("pairs", [])

    def _dexscreener_get_token_pair_info(self, mint_address: str, pair_address: str) -> Optional[dict]:
        """
        Retrieves specific pair information from Dexscreener.

        Args:
            mint_address (str): The token's mint address.
            pair_address (str): The liquidity pool pair address.

        Returns:
            Optional[dict]: The pair information, or None if not found.
        """
        pairs = self._dexscreener_get_token_info(mint_address)
        if not pairs:
            return None
        for pair in pairs:
            if pair.get("pairAddress") == pair_address:
                return pair
        return None
    
    @cache_handler.cache(ttl_s=DEFAULT_CACHE_TTL, invalidate_if_return = {})
    def _dexscreener_fetch(self, mint_address: str) -> dict:
        """
        Fetches token data from the Dexscreener API.

        Args:
            mint_address (str): The token's mint address.

        Returns:
            dict: The JSON response from the API, or an empty dictionary on error.
        """
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint_address}"
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            _log(f"Dexscreener fetch error: {e}", level="ERROR")
            return {}

    # --------------------------
    # Birdeye Info
    # --------------------------
    
    def _birdeye_get_token_security(self, mint_address: str) -> Optional[dict]:
        """
        Get the token security information from the Birdeye API.
        @see https://docs.birdeye.so/reference/get-defi-token_security

        Args:
            mint_address (str): The mint address of the token.

        Returns:
            Optional[dict]: The security data, or None on failure.
        """
        data = self._birdeye_fetch("defi/token_security", {"address": mint_address})
        return data.get("data") if data.get("success") else None

    def _birdeye_get_token_overview(self, mint_address: str) -> Optional[dict]:
        """
        Get the token overview information from the Birdeye API.
        @see https://docs.birdeye.so/reference/get-defi-token_overview

        Args:
            mint_address (str): The mint address of the token.
        
        Returns:
            Optional[dict]: The overview data, or None on failure.
        """
        data = self._birdeye_fetch("defi/token_overview", {"address": mint_address})
        return data.get("data") if data.get("success") else None

    def _birdeye_get_pair_overview(self, pair_address: str) -> Optional[dict]:
        """
        Get the overview information for a specific trading pair from the Birdeye API.
        @see https://docs.birdeye.so/reference/get-defi-v3-pair-overview-single
        
        Args:
            pair_address (str): The liquidity pool / pair address.
            
        Returns:
            Optional[dict]: The pair overview data, or None on failure.
        """
        data = self._birdeye_fetch(
            "defi/v3/pair/overview/single",
            {"address": pair_address, "ui_amount_mode": "scaled"}
        )
        if not data.get("success") or not data.get("data"):
            return None
        return data["data"]

    def _birdeye_get_wallet_overview(self, wallet_address: str) -> Optional[dict]:
        """
        Get the wallet overview information for a specific wallet from the Birdeye API.
        @see https://docs.birdeye.so/reference/get-wallet-v2-net-worth-details
        
        Args:
            wallet_address (str): The public key of the wallet.
        
        Returns:
            Optional[dict]: The wallet overview data, or None on failure.
        """
        data = self._birdeye_fetch(
            "wallet/v2/net-worth-details",
            {
                "wallet": wallet_address,
                "limit": 1
            }
        )
        if not data.get("success") or not data.get("data"):
            return None
        return data["data"]

    def _birdeye_get_token_supply(self, mint_address: str) -> float:
        """
        Get the token supply information from the Birdeye API.
        
        Args:
            mint_address (str): The mint address of the token.
            
        Returns:
            float: The total token supply.
        """
        be_token_security = self._birdeye_get_token_security(mint_address)
        if not be_token_security:
            return 0
        return float(be_token_security.get("totalSupply", 0) or 0)
    
    @cache_handler.cache(ttl_s=DEFAULT_CACHE_TTL)
    def _birdeye_fetch(self, method: str, params: dict) -> dict:
        """
        Fetches data from the Birdeye API with authentication.
        
        Args:
            method (str): The API method/endpoint (e.g., "defi/token_security").
            params (dict): The query parameters for the request.
            
        Returns:
            dict: The JSON response, or an empty dictionary on error.
        """
        url = f"https://public-api.birdeye.so/{method}"
        headers = {
            "x-chain": "solana",
            "accept": "application/json",
            "x-api-key": self.birdeye_api_key
        }
        try:
            response = self.session.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            _log(f"Birdeye fetch error: {e}", level="ERROR")
            return {}
        
    # --------------------------
    # Solscan Info
    # --------------------------
    
    @cache_handler.cache(ttl_s=DAYS_IN_SECONDS)
    def _solscan_estimate_wallet_age(self, wallet_address: str) -> Optional[int]:
        """
        Estimates the wallet age based on its metadata from Solscan.
        
        Args:
            wallet_address (str): The wallet address.

        Returns:
            Optional[int]: The estimated wallet age in days, or None if not found.
        """
        metadata = self._solscan_get_wallet_metadata(wallet_address)
        if not metadata:
            return None

        # Age is present on active_age
        active_age = metadata.get("active_age", 0)
        return active_age

    @cache_handler.cache(ttl_s=MINUTE_IN_SECONDS * 2)
    def _solscan_get_wallet_metadata(self, wallet_address: str) -> Optional[dict]:
        """
        Gets account metadata from the Solscan Pro API.

        Args:
            wallet_address (str): The wallet address.

        Returns:
            Optional[dict]: Wallet metadata, or None if not found.
        """
        data = self._solscan_fetch(
            "account/metadata",
            {"address": wallet_address}
        )
        if not data:
            return None
        return data.get("data", data)

    @cache_handler.cache(ttl_s=DEFAULT_CACHE_TTL)
    def _solscan_get_wallet_created_pools(self,
            wallet_address: str,
            page: int = 1,
            page_size: int = 100
        ) -> Optional[List[dict]]:
        """
        Gets the pools created by a wallet from the Solscan Pro API.

        Args:
            wallet_address (str): The wallet address.
            page (int): The page number to retrieve.    
            page_size (int): The number of results to return per page.

        Returns:
            Optional[List[dict]]: A list of pools created by the wallet, or None if not found.
        """
        data = self._solscan_fetch(
            "account/defi/activities",
            {
                "address": wallet_address,
                "sort_by": "block_time",
                "sort_order": "desc",
                "activity_type[]": "ACTIVITY_POOL_CREATE",
                "page": page,
                "page_size": page_size
            }
        )
        if not data:
            return None

        return data.get("data", data)

    @cache_handler.cache(ttl_s=DEFAULT_CACHE_TTL)
    def _solscan_fetch(self, method: str, params: dict = None) -> dict:
        """
        Fetches data from the Solscan Pro API with authentication.
        @see https://pro-api.solscan.io/pro-api-docs/v2.0

        Args:
            method (str): The API method/endpoint (relative to base URL).
            params (dict, optional): Query parameters.

        Returns:
            dict: The JSON response, or {} on error.
        """
        url = f"https://pro-api.solscan.io/v2.0/{method}"
        headers = {
            "accept": "application/json",
            "token": self.solscan_api_key
        }
        params = params or {}

        try:
            response = self.session.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            _log(f"Solscan fetch error: {e}", level="ERROR")
            return {}

    # --------------------------
    # Aggregated Info
    # --------------------------
    
    @cache_handler.cache(ttl_s=DEFAULT_CACHE_TTL)
    def get_token_summary(
        self, 
        mint_address: str, 
        pair_address: str
    ) -> dict[str, Any]:
        """
        Retrieves a comprehensive summary of a Solana token by aggregating data
        from multiple sources (Birdeye, Dexscreener, RugCheck, Solscan).

        Args:
            mint_address (str): The mint address of the token to analyze.
            pair_address (str): The liquidity pool / pair address for price and
                volume information.

        Returns:
            dict: A dictionary containing various security, liquidity, market,
                and creator wallet metrics.
        """
        
        # ================
        # Birdeye data
        # ================
        be_token_security = self._birdeye_get_token_security(mint_address)
        be_token_overview = self._birdeye_get_token_overview(mint_address)
        be_pool_overview = self._birdeye_get_pair_overview(pair_address)

        be_creator_address = be_token_security.get("creatorAddress", "")
        be_wallet_overview = self._birdeye_get_wallet_overview(be_creator_address)

        be_total_token_supply = be_token_security.get("totalSupply", 0)
        
        be_metadata = be_token_overview.get("extensions", {})
        be_token_description = be_metadata.get("description", "") if be_metadata else ""
        be_token_meta = {}
        if be_metadata:
            be_token_meta = {
                "website": be_metadata.get("website", ""),
                "twitter": be_metadata.get("twitter", ""),
                "discord": be_metadata.get("discord", ""),
            }
            be_token_meta = {k: v for k, v in be_token_meta.items() if v}

        be_token_price_usd = be_token_overview.get("price", 0)
        be_lp_liquidity_usd = be_pool_overview.get("liquidity", 0)
        be_lp_liquidity_tokens = be_lp_liquidity_usd / be_token_price_usd if be_token_price_usd else 0

        # ================
        # Dexscreener data
        # ================
        dexscreener_pair_info = self._dexscreener_get_token_pair_info(mint_address, pair_address) or {}

        # Parse values
        dex_liquidity = dexscreener_pair_info.get("liquidity", {})
        dex_liquidity_usd = float(dex_liquidity.get("usd") or 0)
        dex_lp_tokens = float(dex_liquidity.get("base") or 0)
        dex_price_change = dexscreener_pair_info.get("priceChange", {})
        dex_token_market_cap_usd = float(dexscreener_pair_info.get("marketCap", 0))

        # ================
        # RUG CHECK data
        # ================
        rc_token_info = self._rugcheck_get_token_info(mint_address)
        rc_pair_info = self._rugcheck_get_market_data(mint_address, pair_address)
        
        token_symbol = rc_token_info.get("tokenMeta", {}).get("symbol", "")
        rc_score = rc_token_info.get("score_normalised", 0)
        rc_risks = self._rugcheck_get_token_risks(mint_address)
        rc_mint_authority = self._rugcheck_check_mint_authority(mint_address)
        rc_is_mutable = self._rugcheck_check_is_mutable(mint_address)
        rc_is_freezable = self._rugcheck_check_freeze_authority(mint_address)
        rc_lp_locked = self._rugcheck_get_liquidity_locked(mint_address, pair_address)
        
        rc_pair_lp_info = rc_pair_info.get("lp", {})

        rc_pool_token_supply = rc_pair_lp_info.get("tokenSupply", 0)
        rc_pool_tokens_locked = rc_pair_lp_info.get("lpLocked", 0)

        # ================
        # SolScan data
        # ================
        wallet_metadata = self._solscan_get_wallet_metadata(be_creator_address)
        wallet_funded_by = wallet_metadata.get("funded_by", {}).get("funded_by", "UNKNOWN")
        wallet_age = self._solscan_estimate_wallet_age(be_creator_address)
        creator_created_pools = self._solscan_get_wallet_created_pools(be_creator_address)

        # -- Aggregate response
        return {
            "token_symbol": token_symbol,
            "mint_address": mint_address,
            "pair_address": pair_address,
            "description": be_token_description,

            # ================
            # RUG CHECK
            # ================
            "rc_risk_score": rc_score,
            "rc_risks_desc": rc_risks,
            "rc_mint_authority": rc_mint_authority,
            "rc_is_mutable": rc_is_mutable,
            "rc_is_freezable": rc_is_freezable,
            "rc_liquidity_locked_tokens": rc_lp_locked,
            "rc_is_liquidity_locked": True if rc_lp_locked else False,
            "rc_pool_tokens_locked": rc_pool_tokens_locked,

            # ================
            # SolScan
            # ================
            "ss_creator_wallet_funded_by": wallet_funded_by,
            "ss_creator_wallet_age_days": wallet_age,
            "ss_creator_pools_created": len(creator_created_pools) if creator_created_pools else 0,

            # ================
            # Birdeye
            # ================
            
            # Security
            "be_top10_holder_percentage": round(float(be_token_security.get("top10HolderPercent", 0)) * 100, 2), #  Pool
            "be_token_creation_tx": be_token_security.get("creationTx"),
            "be_token_creation_time": Utils.to_date_string(be_token_security.get("creationTime")),
            "be_token_mint_tx": be_token_security.get("mintTx"),
            "be_token_mint_date": Utils.to_date_string(be_token_security.get("mintTime")),
            "be_token_total_supply": be_total_token_supply,
            "be_token_holders": be_token_overview.get("holder"),
            "be_mutable_metadata": be_token_security.get("mutableMetadata"),
            "be_freezeable": be_token_security.get("freezeable") is not None,
            "be_freeze_authority": be_token_security.get("freezeAuthority") is not None,
            "be_non_transferable": bool(be_token_security.get("nonTransferable")), # https://solana.com/pt/developers/guides/token-extensions/non-transferable
            "be_fake_token": bool(be_token_security.get("fakeToken")),
            "be_pre_market_holder": be_token_security.get("preMarketHolder"),
            "be_has_transfer_tax": bool(be_token_security.get("transferFeeEnable")),

            # Creator info
            "be_creator_percentage": float(be_token_security.get("creatorPercentage", 0) or 0),
            "be_creator_address": be_token_security.get("creatorAddress"),
            "be_creator_net_worth_usd": float(be_wallet_overview.get("net_worth", 0) or 0),
            
            # Extensions
            "be_metadata": be_token_meta,

            # Pair / Market info
            "be_pool_source": be_pool_overview.get("source") or 0,
            "be_token_price_usd": be_token_price_usd,
            "be_pool_creation_time": Utils.to_date_string(be_pool_overview.get("created_at", "")),
            "be_liquidity_pool_usd": be_lp_liquidity_usd,
            # "be_price_usd": be_overview.get("price"),
            "be_traded_volume_24h_usd": be_pool_overview.get("volume_24h"),
            "be_unique_traders_24h": be_pool_overview.get("unique_wallet_24h"),
            # "be_mc_usd": be_token_overview.get("marketCap"),
            # "be_fdv": be_token_overview.get("fdv"),

            # ================
            # Dexscreener data
            # ================
            
            "dex_price_usd": dexscreener_pair_info.get("priceUsd"),
            "dex_liquidity_pool_usd": dex_liquidity_usd,
            "dex_unlocked_liquidity_pool_tokens": dex_lp_tokens,
            "dex_fdv": float(dexscreener_pair_info.get("fdv") or 0),
            "dex_mc_usd": dex_token_market_cap_usd,

            "cl_unlocked_lp_token_supply_percentage": round((dex_lp_tokens / be_total_token_supply * 100), 2),
            
            # Volume momentum
            "dex_volume_h24": dexscreener_pair_info.get("volume", {}).get("h24"),
            "dex_volume_h6": dexscreener_pair_info.get("volume", {}).get("h6"),
            "dex_volume_h1": dexscreener_pair_info.get("volume", {}).get("h1"),
            "dex_volume_m5": dexscreener_pair_info.get("volume", {}).get("m5"),

            # Price momentum
            "dex_price_change_h6": dex_price_change.get("h6"),
            "dex_price_change_h24": dex_price_change.get("h24"),
        }

    def get_token_summary_df(
        self, 
        mint_address: str, 
        pair_address: str
    ) -> pd.DataFrame:
        """
        Retrieves the token summary and returns it as a pandas DataFrame.

        This method is a wrapper around `get_token_summary` to format the
        output for easier data manipulation and analysis.

        Args:
            mint_address (str): The mint address of the token.
            pair_address (str): The pair / liquidity pool address.

        Returns:
            pd.DataFrame: A DataFrame with a single row containing the token
                summary info. If an error occurs, it returns a DataFrame
                with an 'error' column.
        """
        status = self.get_token_summary(mint_address, pair_address)
        if "error" in status:
            return pd.DataFrame({"error": [status["error"]]})

        # Wrap the status dict into a single-row DataFrame
        df = pd.DataFrame([status])

        # Lowercase all column names
        df.columns = [c.lower() for c in df.columns]

        return df