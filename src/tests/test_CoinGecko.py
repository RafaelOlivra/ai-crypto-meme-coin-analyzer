import pytest
import time

from services.CoinGecko import CoinGecko
from services.log.Logger import _log

@pytest.mark.skip(reason="API limit exceeded")
def test_get_coin_market_data():
    coingecko = CoinGecko()
    data = coingecko.get_coin_market_data(vs_currency="usd")
    _log("Coin data fetched successfully.", data)
    assert isinstance(data, list)
    assert len(data) > 0
    assert "id" in data[0]
    assert "current_price" in data[0]
    time.sleep(1)

@pytest.mark.skip(reason="API limit exceeded")
def test_get_coin_solana_meme_coins_market_data():
    coingecko = CoinGecko()
    data = coingecko.get_solana_meme_coins_market_data(vs_currency="usd")
    _log("Solana meme coins data fetched successfully.", data)
    assert isinstance(data, list)
    assert len(data) > 0
    assert "id" in data[0]
    time.sleep(1)

def test_get_coin_details():
    coingecko = CoinGecko()
    data = coingecko.get_coin_details('doge')
    _log("Meme coin details fetched successfully.", data)
    assert isinstance(data, dict)
    assert data.get("id") == "doge"
    time.sleep(1)