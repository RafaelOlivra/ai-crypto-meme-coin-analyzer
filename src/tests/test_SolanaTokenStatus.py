import datetime
from services.log.Logger import _log
import pytest
import time

from services.SolanaTokenSummary import SolanaTokenSummary

## Solana RPC

def test_get_mint_info():
    solana = SolanaTokenSummary()
    mint_info = solana._get_mint_info("2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv")
    assert isinstance(mint_info, dict)
    assert "mintAuthority" in mint_info
    assert "supply" in mint_info

def test_get_wallet_age():
    solana = SolanaTokenSummary()
    wallet_age = solana.get_wallet_age("2QfBNK2WDwSLoUQRb1zAnp3KM12N9hQ8q6ApwUMnWW2T")
    assert isinstance(wallet_age, dict)
    _log("Wallet age:", wallet_age)
    assert "first_tx_time" in wallet_age
    assert "age_days" in wallet_age
    assert wallet_age["age_days"] > 18

## Birdeye

def test_get_birdeye_token_security():
    solana = SolanaTokenSummary()
    security_info = solana._get_birdeye_token_security("2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv")
    assert isinstance(security_info, dict)
    assert "freezeAuthority" in security_info
    assert "nonTransferable" in security_info
    assert "isTrueToken" in security_info

def test_get_birdeye_wallet_overview():
    solana = SolanaTokenSummary()
    wallet_info = solana._get_birdeye_wallet_overview("GixMsyA2jeAoUEQkF2vZD77DdGGh7FFyW8qsezetyEs3")
    _log("Wallet overview:", wallet_info)
    assert isinstance(wallet_info, dict)
    assert "net_worth" in wallet_info

## Dexscreener

def test_get_dexscreener_token_pair_info():
    solana = SolanaTokenSummary()
    dex_info = solana._get_dexscreener_token_pair_info(
        "3B5wuUrMEi5yATD7on46hKfej3pfmd7t1RKgrsN3pump", # BILLY
        "9uWW4C36HiCTGr6pZW9VFhr9vdXktZ8NA8jVnzQU35pJ" # GMGN Pool
    )
    assert isinstance(dex_info, dict)
    assert "liquidity" in dex_info
    assert "priceNative" in dex_info
    assert "priceUsd" in dex_info
    assert "volume" in dex_info

## Aggregator

def test_get_token_summary():
    solana = SolanaTokenSummary()
    status = solana.get_token_summary(
        "3B5wuUrMEi5yATD7on46hKfej3pfmd7t1RKgrsN3pump", # BILLY
        "9uWW4C36HiCTGr6pZW9VFhr9vdXktZ8NA8jVnzQU35pJ" # GMGN Pool
    )
    _log("Status for BILLY:", status)
    assert isinstance(status, dict)
    assert "no_mint" in status
    assert "be_freeze_authority" in status
