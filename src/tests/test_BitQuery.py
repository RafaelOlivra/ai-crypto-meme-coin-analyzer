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
    mint_address = bitquery.get_mint_address_by_name('doge')
    _log("Doge coin mint address fetched successfully.", mint_address)
    assert isinstance(mint_address, str)
    assert mint_address == "9AiGiG5NPomL6QDmHQPKUYusBuBHk3LCQ7Scgxbpump"
    time.sleep(1)
    
def test_get_recent_coin_transactions():
    bitquery = BitQuerySolana()
    mint_address = '9AiGiG5NPomL6QDmHQPKUYusBuBHk3LCQ7Scgxbpump' # Doge
    transactions = bitquery.get_recent_coin_transactions(mint_address, limit=3)
    _log("Recent transactions fetched successfully.", transactions)
    assert isinstance(transactions, list)
    assert len(transactions) == 3