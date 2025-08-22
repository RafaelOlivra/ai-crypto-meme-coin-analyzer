import time
import requests
import pandas as pd
from requests.exceptions import RequestException
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, Dict, Any, List

from services.AppData import AppData
from lib.LocalCache import cache_handler
from lib.Utils import Utils
from services.log.Logger import _log

DEFAULT_CACHE_TTL = 300
MINUTE_IN_SECONDS = 60
DAYS_IN_SECONDS = 24 * 60 * 60

class SolanaTokenSummary:
    """
    Retrieves Solana token summary from multiple sources.
    """

    def __init__(self, rpc_url=None):
        self.rpc_url = rpc_url or "https://api.mainnet-beta.solana.com"
        self.session = requests.Session()
        self.birdeye_api_key = AppData().get_api_key("birdeye_api_key")
        self.helius_api_key = AppData().get_api_key("helius_api_key")
        self.solscan_api_key = AppData().get_api_key("solscan_api_key")

    # --------------------------
    # Solana RPC info
    # --------------------------

    def _rpc_get_mint_info(self, mint_address: str) -> Optional[dict]:
        data = self._rpc_fetch("getAccountInfo", [mint_address, {"encoding": "jsonParsed"}])
        try:
            return data["result"]["value"]["data"]["parsed"]["info"]
        except (KeyError, TypeError):
            return None

    def _rpc_get_token_supply(self, mint_address: str) -> Decimal:
        data = self._rpc_fetch("getTokenSupply", [mint_address])
        try:
            return Decimal(data["result"]["value"]["uiAmount"])
        except (KeyError, TypeError):
            return Decimal(0)

    def _rpc_get_largest_accounts(self, mint_address: str) -> List[dict]:
        data = self._rpc_fetch("getTokenLargestAccounts", [mint_address])
        try:
            return data["result"]["value"]
        except (KeyError, TypeError):
            return []

    @cache_handler.cache(ttl_s=DAYS_IN_SECONDS)
    def _rpc_estimate_wallet_age(self, wallet_address: str) -> Dict[str, Any]:
        """
        Get the wallet age (days since first transaction).
        Returns a dict with first_tx_time (datetime) and age_days (int).
        """
        before: Optional[str] = None
        oldest_sig: Optional[str] = None

        # Page through until we hit the oldest tx
        while True:
            data = self._rpc_fetch(
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
            return {"error": "No transactions found for this wallet: " + wallet_address}

        # Fetch transaction details to get blockTime
        tx_data = self._rpc_fetch("getTransaction", [oldest_sig, {"encoding": "json"}])
        tx = tx_data.get("result", {})
        block_time = tx.get("blockTime")

        if block_time is None:
            return {"error": "Could not fetch block time."}

        first_tx_time = datetime.fromtimestamp(block_time, tz=timezone.utc)
        now = datetime.now(timezone.utc)
        age_days = (now - first_tx_time).days

        return {
            "first_tx_time": first_tx_time,
            "age_days": age_days
        }

    def _rpc_check_nomint(self, mint_info: dict) -> bool:
        return mint_info.get("mintAuthority") is None
    
    @cache_handler.cache(ttl_s=DEFAULT_CACHE_TTL)
    def _rpc_fetch(self, method: str, params: list) -> dict:
        """
        Fetches data from the Solana RPC endpoint with retry logic.
        """
        max_retries = 3
        for attempt in range(max_retries):
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params
            }
            try:
                response = self.session.post(self.rpc_url, json=payload, timeout=10) # Added a timeout
                response.raise_for_status()
                return response.json()
            except RequestException as e:
                _log(f"Solana RPC fetch error on attempt {attempt + 1}/{max_retries}: {e}", level="ERROR")
                if attempt < max_retries - 1:
                    _log("Retrying in 3 seconds...", level="INFO")
                    time.sleep(3) # Delay for 3 seconds before retrying

        _log(f"All {max_retries} attempts failed for method {method}.", level="ERROR")
        return {}
    
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
        @see https://public-api.birdeye.so/wallet/v2/net-worth-details
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
    # Helius Info
    # --------------------------

    def _helius_get_wallet_tx(self, wallet_address: str, limit: int = 10) -> Optional[List[dict]]:
        """
        Get the transaction of a wallet.

        Args:
            wallet_address (str): The wallet address.

        Returns:
            Optional[dict]: The first transaction object, or None if not found/error.
        """
        data = self._helius_fetch(
            f"v0/addresses/{wallet_address}/transactions",
            {
                "limit": limit
            }
        )

        # Invert the data to get the earliest transaction first
        data = list(reversed(data))

        if not data or not isinstance(data, list) or len(data) == 0:
            return None

        return data
    
    @cache_handler.cache(ttl_s=DEFAULT_CACHE_TTL)
    def _helius_fetch(self, method: str, params: dict = None) -> dict:
        """
        Fetch data from the Helius API.

        Args:
            method (str): The API method/endpoint (relative to base URL).
            params (dict, optional): Query parameters.

        Returns:
            dict: The JSON response, or {} if error.
        @see https://docs.helius.dev/solana-apis
        """
        url = f"https://api.helius.xyz/{method}"
        headers = {"accept": "application/json"}
        params = params or {}
        params["api-key"] = self.helius_api_key

        try:
            response = self.session.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            _log(f"Helius fetch error: {e}", level="ERROR")
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
        
        # Age should be guessed by funded_by[block_time]
        fund_block_time_timestamp = metadata.get("funded_by", {}).get("block_time")
        age = Utils.get_days_since(fund_block_time_timestamp)
        return age

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
    def _solscan_fetch(self, method: str, params: dict = None) -> dict:
        """
        Fetch data from the Solscan Pro API.

        Args:
            method (str): The API method/endpoint (relative to base URL).
            params (dict, optional): Query parameters.

        Returns:
            dict: The JSON response, or {} if error.
        @see https://pro-api.solscan.io/pro-api-docs/v2.0
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
        Retrieve token summary and security overview from Birdeye & Dexscreener.

        Args:
            mint_address (str): The mint address of the token to analyze.
            pair_address (str): The liquidity pool / pair address for price & volume info.

        Returns:
            dict: A dictionary containing token security, liquidity, 
                price, holder concentration, and extra Dexscreener data.
        """
        
        # -- Birdeye data
        be_security = self._birdeye_get_token_security(mint_address)
        if not be_security:
            return {"error": "Token security info not found"}

        be_overview = self._birdeye_get_pair_overview(pair_address)
        if not be_overview:
            return {"error": "Pair overview not found"}

        creator_address = be_security.get("creatorAddress", "")
        be_wallet_overview = self._birdeye_get_wallet_overview(creator_address)
        if not be_wallet_overview:
            return {"error": "Wallet overview not found"}

        # Calculate BurntPercent-like metric
        be_total_supply = float(be_security.get("totalSupply", 0) or 0)
        be_top10 = float(be_security.get("top10HolderBalance", 0) or 0)
        be_creator = float(be_security.get("creatorBalance", 0) or 0)
        be_owner = float(be_security.get("ownerBalance", 0) or 0)
        be_held = be_top10 + be_creator + be_owner
        be_top_holders_percent = round(max((be_total_supply - be_held) / be_total_supply * 100, 0), 2) if be_total_supply > 0 else 0.0
        
        # -- Dexscreener data
        dexscreener_info = self._dexscreener_get_token_pair_info(mint_address, pair_address) or {}

        # Parse Dexscreener values safely
        dex_liquidity = dexscreener_info.get("liquidity", {})
        dex_txns = dexscreener_info.get("txns", {})
        dex_price_change = dexscreener_info.get("priceChange", {})

        # Add liquidity ratio checks
        fdv = float(dexscreener_info.get("fdv") or 0)
        liquidity_usd_dex = float(dex_liquidity.get("usd") or 0)
        liq_fdv_ratio = round((liquidity_usd_dex / fdv * 100), 2) if fdv > 0 else None

        # Token age
        pair_created_at = dexscreener_info.get("pairCreatedAt")

        # -- RUG CHECK
        score = self._rugcheck_get_token_info(mint_address).get("score_normalised", 0)
        risks = self._rugcheck_get_token_risks(mint_address)
        nomint = self._rugcheck_check_mint_authority(mint_address)
        is_mutable = self._rugcheck_check_is_mutable(mint_address)
        is_frozen = self._rugcheck_check_freeze_authority(mint_address)
        lp_locked = self._rugcheck_get_liquidity_locked(mint_address, pair_address)

        # -- Solscan
        wallet_metadata = self._solscan_get_wallet_metadata(creator_address)
        wallet_funded_by = wallet_metadata.get("funded_by", {}).get("funded_by", "UNKNOWN")
        wallet_age = self._solscan_estimate_wallet_age(creator_address)

        # -- Aggregate response
        return {
            "mint_address": mint_address,
            "pair_address": pair_address,
            # **concentration,

            #-- RUG CHECK data
            "rc_risk_score": score,
            "rc_risks": risks,
            "rc_no_mint": nomint,
            "rc_is_mutable": is_mutable,
            "rc_is_frozen": is_frozen,
            "rc_liquidity_locked": lp_locked,

            # -- Solscan
            "ss_creator_wallet_funded_by": wallet_funded_by,
            "ss_creator_wallet_age_days": wallet_age,

            # -- Birdeye
            
            # Security & Creator info (Birdeye)
            "be_top10_holders_plus_creator_percentage": be_top_holders_percent,
            "be_creation_tx": be_security.get("creationTx"),
            "be_creation_time": be_security.get("creationTime"),
            "be_mint_tx": be_security.get("mintTx"),
            "be_mint_time": be_security.get("mintTime"),
            "be_total_supply": be_total_supply,
            "be_mutable_metadata": be_security.get("mutableMetadata"),
            "be_freezeable": be_security.get("freezeable") is not None,
            "be_freeze_authority": be_security.get("freezeAuthority") is not None,
            "be_top10_holder_percentage": round(float(be_security.get("top10HolderPercent", 0)) * 100, 2),
            "be_non_transferable": be_security.get("nonTransferable"),
            "be_fake_token": be_security.get("fakeToken"),
            "be_is_true_token": be_security.get("isTrueToken"),
            "be_pre_market_holder": be_security.get("preMarketHolder"),
            "be_transfer_fee_enable": be_security.get("transferFeeEnable"),

            # Creator Info
            "be_creator_percentage": float(be_security.get("creatorPercentage", 0) or 0),
            "be_creator_address": be_security.get("creatorAddress"),
            "be_creator_net_worth_usd": float(be_wallet_overview.get("net_worth", 0) or 0),

            # Pair / Market info
            "be_liquidity_usd": be_overview.get("liquidity"),
            "be_price_usd": be_overview.get("price"),
            "be_volume_24h_usd": be_overview.get("volume_24h"),
            "be_unique_wallets_24h": be_overview.get("unique_wallet_24h"),

            # -- Dexscreener
            # Extras
            "dex_price_usd": dexscreener_info.get("priceUsd"),
            "dex_liquidity_usd": liquidity_usd_dex,
            "dex_liquidity_base": dex_liquidity.get("base"),
            "dex_liquidity_quote": dex_liquidity.get("quote"),
            "dex_fdv": fdv,
            "dex_marketcap": dexscreener_info.get("marketCap"),
            "dex_liq_fdv_ratio": liq_fdv_ratio,
            "dex_pair_age": pair_created_at,

            # Volume & Transaction Momentum
            "dex_volume_h24": dexscreener_info.get("volume", {}).get("h24"),
            "dex_volume_h6": dexscreener_info.get("volume", {}).get("h6"),
            "dex_volume_h1": dexscreener_info.get("volume", {}).get("h1"),
            "dex_volume_m5": dexscreener_info.get("volume", {}).get("m5"),

            "dex_txns_m5": dex_txns.get("m5"),
            "dex_txns_h1": dex_txns.get("h1"),
            "dex_txns_h6": dex_txns.get("h6"),
            "dex_txns_h24": dex_txns.get("h24"),

            # Price momentum
            "dex_price_change_h6": dex_price_change.get("h6"),
            "dex_price_change_h24": dex_price_change.get("h24"),

            # Optional metadata
            "dex_socials": dexscreener_info.get("info", {}).get("socials"),
            "dex_websites": dexscreener_info.get("info", {}).get("websites")
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
