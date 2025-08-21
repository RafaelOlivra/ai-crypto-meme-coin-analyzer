import datetime
import pytest
import time

from services.BitQuerySolana import BitQuerySolana
from services.log.Logger import _log

def test_get_access_token():
    bitquery = BitQuerySolana()
    access_token = bitquery._get_access_token()
    _log("BitQuery access token fetched successfully.", access_token)
    assert isinstance(access_token, str)
    assert len(access_token) == 94
    time.sleep(1)

def test_get_mint_address_by_name():
    bitquery = BitQuerySolana()
    mint_address = bitquery.get_mint_address_by_name('Pudgy Penguins')
    assert isinstance(mint_address, str)
    assert mint_address == "2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv"
    time.sleep(1)
    
def test_get_latest_tokens():
    bitquery = BitQuerySolana()
    latest_tokens = bitquery.get_latest_tokens()
    _log("Latest tokens fetched successfully.", latest_tokens)
    assert isinstance(latest_tokens, list)
    assert len(latest_tokens) > 0

def test_get_recent_coin_trades_for_all_pools():
    bitquery = BitQuerySolana()
    mint_address = '2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv' # Pudgy Penguins
    transactions = bitquery.get_recent_coin_tx_for_all_pools(mint_address, limit=3)
    _log("Pudgy Penguins recent transactions fetched successfully.", transactions)
    assert isinstance(transactions, list)
    assert len(transactions) == 3
    
def test_get_token_pair_summary():
    bitquery = BitQuerySolana()
    mint_address = "J921djbXknTwmazepWsSbuwqjqqPsXA84FbGwormpump" # BILLY
    pair_address = "4hxRUetaPGfN5KuRXvpmZWdNruXiWHX3XhX3mNwbj2AA" # PUMP.FUN
    time = "2025-08-14T15:30:39Z"
    summary = bitquery.get_token_pair_summary(mint_address, pair_address, time=time)
    assert isinstance(summary, dict)
    assert summary['Trade']['Currency']['MintAddress'] == mint_address
    assert summary['Trade']['Market']['MarketAddress'] == pair_address
    assert "sell_volume" in summary
    assert "sell_volume_5min" in summary
    
def test_get_recent_token_pair_trades():
    bitquery = BitQuerySolana()
    mint_address = "J921djbXknTwmazepWsSbuwqjqqPsXA84FbGwormpump" # BILLY
    pair_address = "4hxRUetaPGfN5KuRXvpmZWdNruXiWHX3XhX3mNwbj2AA" # PUMP.FUN
    trades = bitquery.get_recent_pair_tx(mint_address, pair_address)
    # _log("GMGN recent token trades fetched successfully.", trades)
    _log(trades[0]['Transaction']['FeeInUSD'])
    assert isinstance(trades, list)
    assert len(trades) > 0
    assert float(trades[0]['Transaction']['FeeInUSD']) > 0
    
def test_get_liquidity_pool():
    bitquery = BitQuerySolana()
    pair_address = "9uWW4C36HiCTGr6pZW9VFhr9vdXktZ8NA8jVnzQU35pJ"  # BILLY-GMGN
    liquidity_pool = bitquery.get_liquidity_pool_for_pair(pair_address)
    _log("GMGN liquidity pool fetched successfully.", liquidity_pool)
    assert isinstance(liquidity_pool, dict)
    assert liquidity_pool['Pool']['Market']['MarketAddress'] == pair_address
    assert "Quote" in liquidity_pool['Pool']
    assert "Base" in liquidity_pool['Pool']
    
def test_get_wallet_age():
    solana = BitQuerySolana()
    wallet_age = solana.estimate_wallet_age("2QfBNK2WDwSLoUQRb1zAnp3KM12N9hQ8q6ApwUMnWW2T")
    assert isinstance(wallet_age, int)
    _log("Wallet age:", wallet_age)
    assert wallet_age >= 0
    
def test_get_wallet_age_multiple():
    solana = BitQuerySolana()
    wallet_age = solana.estimate_wallets_age([
        "2QfBNK2WDwSLoUQRb1zAnp3KM12N9hQ8q6ApwUMnWW2T",
        "5wEyeeTwzaqdkgSw1TfeNbhWzppjWFv35aW8tk3vyS2x"
    ])
    assert isinstance(wallet_age, list)
    _log("Wallet age:", wallet_age)
    assert all(age >= 0 for age in wallet_age)