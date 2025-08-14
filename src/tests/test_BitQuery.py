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
    _log("Pudgy Penguins mint address fetched successfully.", mint_address)
    assert isinstance(mint_address, str)
    assert mint_address == "2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv"
    time.sleep(1)
    
def test_get_recent_coin_transactions():
    bitquery = BitQuerySolana()
    mint_address = '2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv'
    transactions = bitquery.get_recent_coin_transactions(mint_address, limit=3)
    _log("Pudgy Penguins recent transactions fetched successfully.", transactions)
    assert isinstance(transactions, list)
    assert len(transactions) == 3
    
def test_get_gmgn_coin_summary():
    bitquery = BitQuerySolana()
    token = "8WCyzpzgo78S651NuHKiCYnjucQX8Etndp2cWeNZnPXh" # Pudgy Penguins
    pair_address = "2e8zmWPrfKXFB9dDf7foECz4aLbmHTAucid8yCRPoyyA"
    summary = bitquery.get_gmgn_token_summary(token, pair_address)
    _log("GMGN token summary fetched successfully.", summary)
    assert isinstance(summary, dict)
    assert summary['Trade']['Currency']['MintAddress'] == token
    assert summary['Trade']['Market']['MarketAddress'] == pair_address
    assert "sell_volume" in summary
    assert "sell_volume_5min" in summary