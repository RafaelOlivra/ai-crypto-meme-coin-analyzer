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
    Retrieves Solana token summary from multiple sources.
    This class supports both synchronous and asynchronous RPC calls.
    """
    def __init__(self, rpc_endpoints: Optional[list] = None):

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
        data = self._rpc_fetch("getAccountInfo", [mint_address, {"encoding": "jsonParsed"}])
        try:
            return data["result"]["value"]["data"]["parsed"]["info"]
        except (KeyError, TypeError):
            return None
        
    @cache_handler.cache(ttl_s=MINUTE_IN_SECONDS)
    def _rpc_get_token_supply(self, mint_address: str) -> int:
        data = self._rpc_fetch("getTokenSupply", [mint_address])
        try:
            return int(data["result"]["value"]["uiAmount"])
        except (KeyError, TypeError):
            return 0
    
    @cache_handler.cache(ttl_s=MINUTE_IN_SECONDS)
    def _rpc_get_largest_accounts(self, mint_address: str) -> List[dict]:
        data = self._rpc_fetch("getTokenLargestAccounts", [mint_address])
        try:
            return data["result"]["value"]
        except (KeyError, TypeError):
            return []
        
    def _rpc_check_nomint(self, mint_info: dict) -> bool:
        return mint_info.get("mintAuthority") is None
    
    @cache_handler.cache(ttl_s=DAYS_IN_SECONDS)
    def _rpc_estimate_wallet_age(self, wallet_address: str) -> int:
        """
        Estimate the wallet age for a single wallet address.

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
        Estimate the age for a list of wallets concurrently.
        This function creates tasks and runs the asyncio event loop.

        Args:
            wallet_addresses (List[str]): The wallet addresses to check.

        Returns:
            List[int]: A list of estimated ages for each wallet in days, or -1 if not found.
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
        Asynchronously get the wallet age (days since first transaction).

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
        (Synchronous version for compatibility)
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
        (Asynchronous version for performance)
        
        Args:
            method (str): The RPC method to call.
            params (list): The parameters to pass to the RPC method.
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
        Helper to run the main async tasks.

        Args:
            tasks (List): A list of async tasks to run.

        Returns:
            List[int]: A list of results or -1 for failed tasks.
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
        return self._rugcheck_fetch(mint_address)

    def _rugcheck_check_mint_authority(self, mint_address: str) -> bool:
        token_info = self._rugcheck_get_token_info(mint_address)
        if not token_info:
            return False
        return token_info.get("token", {}).get("mintAuthority") is not None

    def _rugcheck_get_token_risks(self, mint_address: str) -> list[str]:
        token_info = self._rugcheck_get_token_info(mint_address)
        if not token_info:
            return []
        risks = token_info.get("risks", [])
        return [risk["name"] for risk in risks]

    def _rugcheck_check_is_mutable(self, mint_address: str) -> bool:
        token_info = self._rugcheck_get_token_info(mint_address)
        if not token_info:
            return False
        return token_info.get("tokenMeta", {}).get("isMutable") is not None

    def _rugcheck_check_freeze_authority(self, mint_address: str) -> bool:
        token_info = self._rugcheck_get_token_info(mint_address)
        if not token_info:
            return False
        return token_info.get("token", {}).get("freezeAuthority") is not None

    def _rugcheck_get_market_data(self, mint_address: str, pair_address: str) -> Optional[dict]:
        data = self._rugcheck_fetch(mint_address)
        markets = data.get("markets", {})
        if not markets:
            return None
        for market in markets:
            if market.get("pubkey") == pair_address:
                return market
        return None
    
    def _rugcheck_get_liquidity_locked(self, mint_address: str, pair_address: str) ->bool:
        market_data = self._rugcheck_get_market_data(mint_address, pair_address)
        if not market_data:
            return False
        return market_data.get("lp", {}).get("lpLocked", 0)
    
    def _rugcheck_is_liquidity_locked(self, mint_address: str, pair_address: str) ->bool:
        return self._rugcheck_get_liquidity_locked(mint_address, pair_address) > 1

    @cache_handler.cache(ttl_s=DEFAULT_CACHE_TTL)
    def _rugcheck_fetch(self, mint_address: str) -> dict:
        url = f"https://api.rugcheck.xyz/v1/tokens/{mint_address}/report"
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            _log(f"RugCheck fetch error: {e}", level="ERROR")
            return {}

    # --------------------------
    # Dexscreener Info
    # --------------------------

    def _dexscreener_get_token_info(self, mint_address: str) -> Optional[dict]:
        data = self._dexscreener_fetch(mint_address)
        return data.get("pairs", [])

    def _dexscreener_get_token_pair_info(self, mint_address: str, pair_address: str) -> Optional[dict]:
        pairs = self._dexscreener_get_token_info(mint_address)
        if not pairs:
            return None
        for pair in pairs:
            if pair.get("pairAddress") == pair_address:
                return pair
        return None
    
    @cache_handler.cache(ttl_s=DEFAULT_CACHE_TTL)
    def _dexscreener_fetch(self, mint_address: str) -> dict:
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
        """
        data = self._birdeye_fetch("defi/token_security", {"address": mint_address})
        return data.get("data") if data.get("success") else None

    def _birdeye_get_token_supply(self, mint_address: str) -> float:
        """
        Get the token supply information from the Birdeye API.
        """
        be_token_security = self._birdeye_get_token_security(mint_address)
        if not be_token_security:
            return 0
        return float(be_token_security.get("totalSupply", 0) or 0)

    def _birdeye_get_pair_overview(self, pair_address: str) -> Optional[dict]:
        """
        Get the overview information for a specific trading pair from the Birdeye API.
        @see https://docs.birdeye.so/reference/get-defi-v3-pair-overview-single
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

    @cache_handler.cache(ttl_s=DEFAULT_CACHE_TTL)
    def _birdeye_fetch(self, method: str, params: dict) -> dict:
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
    def _solscan_estimate_wallet_age(self,  wallet_address: str) -> Optional[int]:
        """
        Estimate the wallet age based on its metadata.
        
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
        Get account metadata from Solscan.

        Args:
            wallet_address (str): The wallet address.

        Returns:
            Optional[dict]: Wallet metadata or None if not found.
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
        Get the pools created by a wallet.

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
        Fetch data from the Solscan Pro API.
        @see https://pro-api.solscan.io/pro-api-docs/v2.0

        Args:
            method (str): The API method/endpoint (relative to base URL).
            params (dict, optional): Query parameters.

        Returns:
            dict: The JSON response, or {} if error.
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
    
    # @cache_handler.cache(ttl_s=DEFAULT_CACHE_TTL)
    def get_token_summary(
        self, 
        mint_address: str, 
        pair_address: str
    ) -> dict[str, Any]:
        """
        Retrieve token summary and security overview from Birdeye & Dexscreener.

        Args:
            mint_address (str): The mint address of the token to analyze.
            pair_address (str): The liquidity pool / pair address for price & volume info.

        Returns:
            dict: A dictionary containing token security, liquidity, 
                price, holder concentration, and extra data.
        """
        
        # -- Birdeye data
        be_token_security = self._birdeye_get_token_security(mint_address)
        if not be_token_security:
            return {"error": "Token security info not found"}

        be_overview = self._birdeye_get_pair_overview(pair_address)
        if not be_overview:
            return {"error": "Pair overview not found"}

        creator_address = be_token_security.get("creatorAddress", "")
        be_wallet_overview = self._birdeye_get_wallet_overview(creator_address)
        if not be_wallet_overview:
            return {"error": "Wallet overview not found"}

        # Calculate BurntPercent-like metric
        be_total_token_supply = float(be_token_security.get("totalSupply", 0) or 0)
        be_top10 = float(be_token_security.get("top10HolderBalance", 0) or 0)
        be_creator = float(be_token_security.get("creatorBalance", 0) or 0)
        be_owner = float(be_token_security.get("ownerBalance", 0) or 0)
        be_held = be_top10 + be_creator + be_owner
        be_top_holders_percent = round(max((be_total_token_supply - be_held) / be_total_token_supply * 100, 0), 2) if be_total_token_supply > 0 else 0.0
        
        # -- Dexscreener data
        dexscreener_pair_info = self._dexscreener_get_token_pair_info(mint_address, pair_address) or {}

        # Parse Dexscreener values safely
        dex_liquidity = dexscreener_pair_info.get("liquidity", {})
        dex_liquidity_usd = float(dex_liquidity.get("usd") or 0)
        dex_liquidity_tokens = float(dex_liquidity.get("base") or 0)
        dex_price_change = dexscreener_pair_info.get("priceChange", {})
        dex_pair_market_cap = dexscreener_pair_info.get("marketCap", {})

        # Add liquidity ratio checks
        fdv = float(dexscreener_pair_info.get("fdv") or 0)
        liquidity_usd_dex = float(dex_liquidity.get("usd") or 0)

        # Token age
        pair_created_at = dexscreener_pair_info.get("pairCreatedAt")

        # -- RUG CHECK
        rc_token_info = self._rugcheck_get_token_info(mint_address)
        rc_pair_info = self._rugcheck_get_market_data(mint_address, pair_address)
        
        token_symbol = rc_token_info.get("tokenMeta", {}).get("symbol", "")
        rc_score = rc_token_info.get("score_normalised", 0)
        rc_risks = self._rugcheck_get_token_risks(mint_address)
        rc_mint_authority = self._rugcheck_check_mint_authority(mint_address)
        rc_is_mutable = self._rugcheck_check_is_mutable(mint_address)
        rc_is_freezable = self._rugcheck_check_freeze_authority(mint_address)
        rc_lp_locked = self._rugcheck_get_liquidity_locked(mint_address, pair_address)

        rc_pool_token_supply = rc_pair_info.get("lp", {}).get("tokenSupply", 0)
        _log(f"RC Pool Token Supply", rc_pair_info)
        rc_total_token_holders = rc_token_info.get("totalHolders", 0)

        # -- Solscan
        wallet_metadata = self._solscan_get_wallet_metadata(creator_address)
        wallet_funded_by = wallet_metadata.get("funded_by", {}).get("funded_by", "UNKNOWN")
        wallet_age = self._solscan_estimate_wallet_age(creator_address)
        creator_created_pools = self._solscan_get_wallet_created_pools(creator_address)

        # -- Aggregate response
        return {
            "token_symbol": token_symbol,
            "mint_address": mint_address,
            "pair_address": pair_address,

            #-- RUG CHECK data
            "rc_risk_score": rc_score,
            "rc_risks_desc": rc_risks,
            "rc_mint_authority": rc_mint_authority,
            "rc_is_mutable": rc_is_mutable,
            "rc_is_freezeable": rc_is_freezable,
            "rc_liquidity_locked_tokens": rc_lp_locked,
            "rc_is_liquidity_locked": True if rc_lp_locked else False,
            "rc_total_token_holders": rc_total_token_holders,
            "rc_pool_token_supply": rc_pool_token_supply,

            # -- Solscan
            "ss_creator_wallet_funded_by": wallet_funded_by,
            "ss_creator_wallet_age_days": wallet_age,
            "ss_creator_pools_created": len(creator_created_pools) if creator_created_pools else 0,

            # -- Birdeye
            
            # Security & Creator info (Birdeye)
            "be_top10_holders_plus_creator_percentage": be_top_holders_percent,
            "be_top10_holder_percentage": round(float(be_token_security.get("top10HolderPercent", 0)) * 100, 2), #  Pool
            "be_token_creation_tx": be_token_security.get("creationTx"),
            "be_token_creation_date": Utils.to_date_string(be_token_security.get("creationTime")),
            "be_token_mint_tx": be_token_security.get("mintTx"),
            "be_token_mint_date": Utils.to_date_string(be_token_security.get("mintTime")),
            "be_token_total_supply": be_total_token_supply,
            "be_mutable_metadata": be_token_security.get("mutableMetadata"),
            "be_freezeable": be_token_security.get("freezeable") is not None,
            "be_freeze_authority": be_token_security.get("freezeAuthority") is not None,
            "be_non_transferable": bool(be_token_security.get("nonTransferable")), # https://solana.com/pt/developers/guides/token-extensions/non-transferable
            "be_is_NFT": bool(be_token_security.get("fakeToken")),
            "be_pre_market_holder": be_token_security.get("preMarketHolder"),
            "be_has_transfer_tax": bool(be_token_security.get("transferFeeEnable")),

            # Creator info
            "be_creator_percentage": float(be_token_security.get("creatorPercentage", 0) or 0),
            "be_creator_address": be_token_security.get("creatorAddress"),
            "be_creator_net_worth_usd": float(be_wallet_overview.get("net_worth", 0) or 0),

            # Pair / Market info
            "be_liquidity_pool_usd": be_overview.get("liquidity"),
            "be_price_usd": be_overview.get("price"),
            "be_traded_volume_24h_usd": be_overview.get("volume_24h"),
            "be_unique_traders_24h": be_overview.get("unique_wallet_24h"),

            # -- Dexscreener
            
            # "dex_price_usd": dexscreener_info.get("priceUsd"),
            # "dex_liquidity_pool_usd": dex_liquidity_usd,
            "dex_liquidity_pool_tokens": dex_liquidity_tokens,
            "dex_fdv": fdv,
            "dex_current_pool_mc": dex_pair_market_cap,
            # "dex_liq_fdv_ratio": liq_fdv_ratio,
            "dex_pool_age_days": Utils.get_days_since(int(pair_created_at / 1000)),

            "cl_pool_supply_token_percentage": round((dex_liquidity_tokens / be_total_token_supply * 100), 2),
            
            # Volume & Transaction momentum
            "dex_volume_h24": dexscreener_pair_info.get("volume", {}).get("h24"),
            "dex_volume_h6": dexscreener_pair_info.get("volume", {}).get("h6"),
            "dex_volume_h1": dexscreener_pair_info.get("volume", {}).get("h1"),
            "dex_volume_m5": dexscreener_pair_info.get("volume", {}).get("m5"),

            # "dex_txns_m5": dex_txns.get("m5"),
            # "dex_txns_h1": dex_txns.get("h1"),
            # "dex_txns_h6": dex_txns.get("h6"),
            # "dex_txns_h24": dex_txns.get("h24"),

            # Price momentum
            "dex_price_change_h6": dex_price_change.get("h6"),
            "dex_price_change_h24": dex_price_change.get("h24"),

            # Optional metadata
            "dex_socials": dexscreener_pair_info.get("info", {}).get("socials"),
            "dex_websites": dexscreener_pair_info.get("info", {}).get("websites")
        }

    def get_token_summary_df(
        self, 
        mint_address: str, 
        pair_address: str
    ) -> pd.DataFrame:
        """
        Get token summary as a pandas DataFrame.

        Args:
            mint_address (str): The mint address of the token.
            pair_address (str): The pair / liquidity pool address.

        Returns:
            pd.DataFrame: DataFrame with a single row containing token summary info.
                        If an error occurs, returns a DataFrame with an 'error' column.
        """
        status = self.get_token_summary(mint_address, pair_address)
        if "error" in status:
            return pd.DataFrame({"error": [status["error"]]})

        # Wrap the status dict into a single-row DataFrame
        df = pd.DataFrame([status])

        # Lowercase all column names
        df.columns = [c.lower() for c in df.columns]

        return df
