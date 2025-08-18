import datetime
from services.log.Logger import _log
import pytest
import time

from services.SolanaTokenStatus import SolanaTokenStatus

def test_get_mint_info():
    solana = SolanaTokenStatus()
    mint_info = solana._get_mint_info("2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv")
    assert isinstance(mint_info, dict)
    assert "mintAuthority" in mint_info
    assert "supply" in mint_info

def test_get_birdeye_token_security():
    solana = SolanaTokenStatus()
    security_info = solana._get_birdeye_token_security("2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv")
    assert isinstance(security_info, dict)
    assert "freezeAuthority" in security_info
    assert "nonTransferable" in security_info
    assert "isTrueToken" in security_info

def test_get_dexscreener_token_pair_info():
    solana = SolanaTokenStatus()
    dex_info = solana._get_dexscreener_token_pair_info(
        "3B5wuUrMEi5yATD7on46hKfej3pfmd7t1RKgrsN3pump", # BILLY
        "9uWW4C36HiCTGr6pZW9VFhr9vdXktZ8NA8jVnzQU35pJ" # GMGN Pool
    )
    assert isinstance(dex_info, dict)
    assert "liquidity" in dex_info
    assert "priceNative" in dex_info
    assert "priceUsd" in dex_info
    assert "volume" in dex_info

def test_get_status():
    solana = SolanaTokenStatus()
    status = solana.get_token_summary(
        "3B5wuUrMEi5yATD7on46hKfej3pfmd7t1RKgrsN3pump", # BILLY
        "9uWW4C36HiCTGr6pZW9VFhr9vdXktZ8NA8jVnzQU35pJ" # GMGN Pool
    )
    _log("Status for BILLY:", status)
    assert isinstance(status, dict)
    assert "NoMint" in status
    assert "FreezeAuthority" in status
