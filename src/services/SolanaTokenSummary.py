import json
import requests
import pandas as pd
from decimal import Decimal
from typing import Optional, Dict, Any, List

from services.AppData import AppData
from lib.LocalCache import cache_handler
from services.log.Logger import _log

DEFAULT_CACHE_TTL = 30

class SolanaTokenSummary:
    """
    Retrieves Solana token summary from multiple sources.
    """

    def __init__(self, rpc_url=None):
        self.rpc_url = rpc_url or "https://api.mainnet-beta.solana.com"
        self.session = requests.Session()
        self.birdeye_api_key = AppData().get_api_key("birdeye_api_key")

    @cache_handler.cache(ttl_s=DEFAULT_CACHE_TTL)
    def _fetch_solana_rpc(self, method: str, params: list) -> dict:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params
        }
        try:
            response = self.session.post(self.rpc_url, json=payload)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            _log(f"Solana RPC fetch error: {e}", level="ERROR")
            return {}

    @cache_handler.cache(ttl_s=DEFAULT_CACHE_TTL)
    def _fetch_birdeye_api(self, method: str, params: dict) -> dict:
        url = f"https://public-api.birdeye.so/{method}"
        headers = {
            "x-chain": "solana",
            "accept": "application/json",
            "x-api-key": self.birdeye_api_key
        }
        try:
            response = self.session.get(url, headers=headers, params=params)
            _log(f"Birdeye API response for {method}:", response.json(), level="DEBUG")
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            _log(f"Birdeye fetch error: {e}", level="ERROR")
            return {}

    @cache_handler.cache(ttl_s=DEFAULT_CACHE_TTL)
    def _fetch_dexscreener_api(self, mint_address: str) -> dict:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint_address}"
        try:
            response = self.session.get(url, timeout=10)
            _log(f"Dexscreener", response.json(), level="DEBUG")
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            _log(f"Dexscreener fetch error: {e}", level="ERROR")
            return {}

    def _get_mint_info(self, mint_address: str) -> Optional[dict]:
        data = self._fetch_solana_rpc("getAccountInfo", [mint_address, {"encoding": "jsonParsed"}])
        try:
            return data["result"]["value"]["data"]["parsed"]["info"]
        except (KeyError, TypeError):
            return None

    def _get_token_supply(self, mint_address: str) -> Decimal:
        data = self._fetch_solana_rpc("getTokenSupply", [mint_address])
        try:
            return Decimal(data["result"]["value"]["uiAmount"])
        except (KeyError, TypeError):
            return Decimal(0)

    def _get_largest_accounts(self, mint_address: str) -> List[dict]:
        data = self._fetch_solana_rpc("getTokenLargestAccounts", [mint_address])
        try:
            return data["result"]["value"]
        except (KeyError, TypeError):
            return []

    def _check_nomint(self, mint_info: dict) -> bool:
        return mint_info.get("mintAuthority") is None

    def _check_burn_percentage(self, mint_address: str, supply: Decimal) -> Decimal:
        if supply <= 0:
            return Decimal(0)

        burn_wallets = {
            "1nc1nerator11111111111111111111111111111111",
            "11111111111111111111111111111111"
        }

        largest_accounts = self._get_largest_accounts(mint_address)
        burnt_amount = Decimal(0)

        for acc in largest_accounts:
            if acc["address"] in burn_wallets:
                burnt_amount += Decimal(acc["uiAmount"])

        return (burnt_amount / supply) * 100

    def _calculate_holder_concentration(self, mint_address: str, supply: Decimal) -> Dict[str, Decimal]:
        if supply <= 0:
            return {"Top1HolderPercent": Decimal(0), "Top5HolderPercent": Decimal(0)}

        burn_wallets = {
            "1nc1nerator11111111111111111111111111111111",
            "11111111111111111111111111111111"
        }

        largest_accounts = [
            acc for acc in self._get_largest_accounts(mint_address)
            if acc["address"] not in burn_wallets
        ]

        top1 = Decimal(largest_accounts[0]["uiAmount"]) / supply * 100 if largest_accounts else Decimal(0)
        top5_sum = sum(Decimal(acc["uiAmount"]) for acc in largest_accounts[:5])
        top5 = (top5_sum / supply) * 100

        return {
            "top1_holder_percent": round(top1, 2),
            "top5_holder_percent": round(top5, 2)
        }

    # --------------------------
    # Dexscreener Info
    # --------------------------

    def _get_dexscreener_token_info(self, mint_address: str) -> Optional[dict]:
        data = self._fetch_dexscreener_api(mint_address)
        return data.get("pairs", [])

    def _get_dexscreener_token_pair_info(self, mint_address: str, pair_address: str) -> Optional[dict]:
        pairs = self._get_dexscreener_token_info(mint_address)
        if not pairs:
            return None
        for pair in pairs:
            if pair.get("pairAddress") == pair_address:
                return pair
        return None

    # --------------------------
    # Birdeye Info
    # --------------------------
    
    def _get_birdeye_token_security(self, mint_address: str) -> Optional[dict]:
        """
        Get the token security information from the Birdeye API.
        @see https://docs.birdeye.so/reference/get-defi-token_security
        """
        data = self._fetch_birdeye_api("defi/token_security", {"address": mint_address})
        return data.get("data") if data.get("success") else None

    def _get_birdeye_pair_overview(self, pair_address: str) -> Optional[dict]:
        """
        Get the overview information for a specific trading pair from the Birdeye API.
        @see https://docs.birdeye.so/reference/get-defi-v3-pair-overview-single
        """
        data = self._fetch_birdeye_api(
            "defi/v3/pair/overview/single",
            {"address": pair_address, "ui_amount_mode": "scaled"}
        )
        if not data.get("success") or not data.get("data"):
            return None
        return data["data"]
    
    
    # --------------------------
    # Aggregated Info
    # --------------------------

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
            dict: A dictionary containing token security, burn info, liquidity, 
                price, holder concentration, and extra Dexscreener data.
        """
        # -- Solana network data
        mint_info = self._get_mint_info(mint_address)
        if not mint_info:
            return {"error": "Mint address not found"}

        supply = self._get_token_supply(mint_address)
        nomint = self._check_nomint(mint_info)
        concentration = self._calculate_holder_concentration(mint_address, supply)
        
        # -- Birdeye data
        be_security = self._get_birdeye_token_security(mint_address)
        if not be_security:
            return {"error": "Token security info not found"}

        be_overview = self._get_birdeye_pair_overview(pair_address)
        if not be_overview:
            return {"error": "Pair overview not found"}
        
        # Calculate BurntPercent-like metric
        be_total_supply = float(be_security.get("totalSupply", 0))
        be_top10 = float(be_security.get("top10HolderBalance", 0))
        be_creator = float(be_security.get("creatorBalance", 0))
        be_owner = float(be_security.get("ownerBalance") or 0)
        be_held = be_top10 + be_creator + be_owner
        be_top_holders_percent = round(max((be_total_supply - be_held) / be_total_supply * 100, 0), 2) if be_total_supply > 0 else 0.0
        
        # -- Dexscreener data
        dexscreener_info = self._get_dexscreener_token_pair_info(mint_address, pair_address) or {}

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

        
        # -- Aggregate response
        return {
            # Basic token info
            "no_mint": nomint,
            # **concentration,

            # Security & creator info (Birdeye)
            "be_top_holders_percent": be_top_holders_percent,
            "be_creator_address": be_security.get("creatorAddress"),
            "be_creation_tx": be_security.get("creationTx"),
            "be_creation_time": be_security.get("creationTime"),
            "be_mint_tx": be_security.get("mintTx"),
            "be_mint_time": be_security.get("mintTime"),
            "be_total_supply": be_total_supply,
            "be_mutable_metadata": be_security.get("mutableMetadata"),
            "be_freeze_authority": be_security.get("freezeAuthority") is not None,
            "be_top10_holder_percent": round(float(be_security.get("top10HolderPercent", 0)) * 100, 2),
            "be_creator_percentage": float(be_security.get("creatorPercentage", 0)),
            "be_non_transferable": be_security.get("nonTransferable"),
            "be_fake_token": be_security.get("fakeToken"),
            "be_is_true_token": be_security.get("isTrueToken"),
            "be_pre_market_holder": be_security.get("preMarketHolder"),
            "be_transfer_fee_enable": be_security.get("transferFeeEnable"),

            # Birdeye Pair / Market info
            "be_liquidity_usd": be_overview.get("liquidity"),
            "be_price_usd": be_overview.get("price"),
            "be_volume_24h_usd": be_overview.get("volume_24h"),
            "be_unique_wallets_24h": be_overview.get("unique_wallet_24h"),

            
            # Dexscreener extras
            "dex_price_usd": dexscreener_info.get("priceUsd"),
            "dex_liquidity_usd": liquidity_usd_dex,
            "dex_liquidity_base": dex_liquidity.get("base"),
            "dex_liquidity_quote": dex_liquidity.get("quote"),
            "dex_fdv": fdv,
            "dex_marketcap": dexscreener_info.get("marketCap"),
            "dex_liq_fdv_ratio": liq_fdv_ratio,
            "dex_pair_age": pair_created_at,

            # Volume & transaction momentum
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
