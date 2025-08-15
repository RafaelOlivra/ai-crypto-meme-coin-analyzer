import json
import requests
from decimal import Decimal
from typing import Optional, Dict, Any, List

from services.AppData import AppData
from lib.LocalCache import cache_handler
from src.services.log.Logger import _log


class SolanaTokenStatus:
    """
    Retrieves Solana token status:
    - NoMint (mint authority revoked)
    - Blacklist & scam status (via Birdeye — kept for reference, not used here)
    - Burn percentage (supply in burn wallets)
    - Liquidity & price (via Dexscreener)
    - Top holders concentration
    """

    def __init__(self, rpc_url=None):
        self.rpc_url = rpc_url or "https://api.mainnet-beta.solana.com"
        self.session = requests.Session()
        self.birdeye_api_key = AppData().get_api_key("birdeye_api_key")

    @cache_handler.cache(ttl_s=1800)
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

    # @cache_handler.cache(ttl_s=60)
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

    @cache_handler.cache(ttl_s=60)
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
            "Top1HolderPercent": round(top1, 2),
            "Top5HolderPercent": round(top5, 2)
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
        data = self._fetch_birdeye_api("defi/token_security", {"address": mint_address})
        return data.get("data") if data.get("success") else None

    def _get_birdeye_liquidity(self, pair_address: str) -> Optional[dict]:
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
    
    def get_status(self, mint_address: str, pair_address: str = None) -> Dict[str, Any]:
        mint_info = self._get_mint_info(mint_address)
        if not mint_info:
            return {"error": "Mint address not found"}

        supply = self._get_token_supply(mint_address)
        burnt_percent = self._check_burn_percentage(mint_address, supply)
        nomint = self._check_nomint(mint_info)

        # Dexscreener liquidity & price data
        if pair_address:
            main_pair = self._get_dexscreener_token_pair_info(mint_address, pair_address)
        else:
            pairs = self._get_dexscreener_token_info(mint_address)
            main_pair = pairs[0] if pairs else None

        liquidity_usd = main_pair.get("liquidity", {}).get("usd") if main_pair else None
        price_usd = main_pair.get("priceUsd") if main_pair else None
        volume_24h_usd = main_pair.get("volume", {}).get("h24") if main_pair else None

        # Holder concentration
        concentration = self._calculate_holder_concentration(mint_address, supply)

        return {
            "NoMint": nomint,
            "BurntPercent": float(round(burnt_percent, 2)),
            "FreezeAuthority": mint_info.get("freezeAuthority") is not None,
            "DEXPaid": liquidity_usd is not None,
            "LiquidityUSD": liquidity_usd,
            "PriceUSD": price_usd,
            "Volume24hUSD": volume_24h_usd,
            "UniqueWallets24h": None,
            **concentration
        }
