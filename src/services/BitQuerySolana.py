import os
import json
import requests
import streamlit as st

from typing import Any, Union, Optional, Dict, List

from src.services.log.Logger import _log
from services.AppData import AppData
from lib.Utils import Utils
from lib.LocalCache import cache_handler

class BitQuerySolana:
    """
    A base class for handling Solana coin-related operations via BitQuery.
    """

    def __init__(self, api_key=None):
        # Set the API key, either from the environment or directly from the parameter
        self.client_id = api_key or AppData().get_api_key("bitquery_client_id")
        self.client_secret = api_key or AppData().get_api_key("bitquery_client_secret")

        self.base_url = "https://graphql.bitquery.io/"
        self.oauth_url = "https://oauth2.bitquery.io/oauth2/token"
        self.eap_url = "https://streaming.bitquery.io/eap"
        self.session = requests.Session()

    @cache_handler.cache(ttl_s=1)
    def get_recent_coin_transactions(self, mint_address: str, limit: int = 10) -> List[Dict]:
        """
        Get the most recent transactions for a Solana coin.

        Args:
            mint_address (str): The mint address of the coin (contract_address).
            limit (int): The number of recent transactions to retrieve.

        Returns:
            list: A list of recent transaction data.
        """
        
        query = """
        query ($mintAddress: String!, $limit: Int!) {
          Solana(network: solana) {
            Transfers(
              where: { Transfer: { Currency: { MintAddress: { is: $mintAddress}}}},
              limit: {count: $limit}
            ) {
              Transfer {
                Amount
                AmountInUSD
                Currency {
                  MintAddress
                  Name
                  Symbol
                }
                Sender {
                  Address
                }
                Receiver {
                  Address
                }
              }
              Block {
                Time
              }
              Transaction {
                Signature
              }
            }
          }
        }
        """

        variables = {
            "mintAddress": mint_address,
            "limit": limit
        }

        payload = {
            "query": query,
            "variables": variables
        }

        response_data = self._fetch(
            url=self.eap_url, 
            method="post", 
            data=json.dumps(payload),
        )
        
        try:
            return response_data["data"]["Solana"]["Transfers"]
        except (KeyError, TypeError) as e:
            _log(f"Error parsing BitQuerySolana response: {e}", level="ERROR")
            return []

    @cache_handler.cache(ttl_s=3600)
    def get_mint_address_by_name(self, coin_name: str) -> Optional[str]:
        """
        Get the mint address for a Solana coin based on its name or symbol.
        """
        query = """
        query ($coinName: String!) {
          Solana {
            DEXTrades(
              orderBy: { descending: Block_Time }
              limit: { count: 1 }
              limitBy: { by: Trade_Buy_Currency_MintAddress, count: 1 }
              where: { Trade: { Buy: { Currency: { Name: { is: $coinName } } } } }
            ) {
              Trade {
                Buy {
                  Currency {
                    Name
                    Symbol
                    MintAddress
                  }
                }
              }
            }
          }
        }
        """
        
        variables = {
            "coinName": coin_name,
        }

        payload = {
            "query": query,
            "variables": variables
        }
        
        response_data = self._fetch(
            url=self.eap_url,
            method="post",
            data=json.dumps(payload),
        )

        try:
            # The query returns a list of DEXTrades, so we access the first one
            # and then get the MintAddress from the Trade.
            mint_address = response_data["data"]["Solana"]["DEXTrades"][0]["Trade"]["Buy"]["Currency"]["MintAddress"]
            return mint_address
        except (KeyError, TypeError, IndexError) as e:
            _log(f"Error parsing BitQuery response or coin not found: {e}", level="ERROR")
            return None
    
    @cache_handler.cache(ttl_s=3600)
    def get_gmgn_token_summary(self, token: str, pair_address: str, side_token: str = "So11111111111111111111111111111111111111112"):
        """
        Retrieve a trading summary for a specific GMGN token in a given market.

        In the context of GMGN (a Solana-based token analytics/trading platform):
        - `token` is the **mint address** of the token you want to analyze (the "base token").
        - `pair_address` is the **mint address of the liquidity pool or market pair**
          in which the token is traded.
        - `side_token` is the **mint address of the counter or quote token** 
          that the base token is traded against (e.g., USDC, SOL). 
          By default, this is set to wrapped SOL (`So11111111111111111111111111111111111111112`).

        Args:
            token (str): Mint address of the GMGN token to analyze (base token).
            pair_address (str): Mint address of the specific market pair/liquidity pool.
            side_token (str): Mint address of the quote/counter token (default: wrapped SOL).

        Returns:
            dict: A dictionary containing the token's summary statistics, such as price,
                  volume, liquidity, and other market indicators.
        """
        query = """
          query Q($token: String!, $side_token: String!, $pair_address: String!, $time_5min_ago: DateTime!, $time_1h_ago: DateTime!) {
          Solana(dataset: realtime) {
            DEXTradeByTokens(
              where: { Transaction: { Result: { Success: true } }, Trade: { Currency: { MintAddress: { is: $token }}, Side: {Currency: {MintAddress: {is: $side_token}}}, Market: {MarketAddress: {is: $pair_address}}}, Block: {Time: {since: $time_1h_ago}}}
            ) {
              Trade {
                Currency {
                  Name
                  MintAddress
                  Symbol
                }
                start: PriceInUSD(minimum: Block_Time)
                min5: PriceInUSD(
                  minimum: Block_Time
                  if: {Block: {Time: {after: $time_5min_ago}}}
                )
                end: PriceInUSD(maximum: Block_Time)
                Dex {
                  ProtocolName
                  ProtocolFamily
                  ProgramAddress
                }
                Market {
                  MarketAddress
                }
                Side {
                  Currency {
                    Symbol
                    Name
                    MintAddress
                  }
                }
              }
              makers: count(distinct: Transaction_Signer)
              makers_5min: count(
                distinct: Transaction_Signer
                if: {Block: {Time: {after: $time_5min_ago}}}
              )
              buyers: count(
                distinct: Transaction_Signer
                if: {Trade: {Side: {Type: {is: buy}}}}
              )
              buyers_5min: count(
                distinct: Transaction_Signer
                if: {Trade: {Side: {Type: {is: buy}}}, Block: {Time: {after: $time_5min_ago}}}
              )
              sellers: count(
                distinct: Transaction_Signer
                if: {Trade: {Side: {Type: {is: sell}}}}
              )
              sellers_5min: count(
                distinct: Transaction_Signer
                if: {Trade: {Side: {Type: {is: sell}}}, Block: {Time: {after: $time_5min_ago}}}
              )
              trades: count
              trades_5min: count(if: {Block: {Time: {after: $time_5min_ago}}})
              traded_volume: sum(of: Trade_Side_AmountInUSD)
              traded_volume_5min: sum(
                of: Trade_Side_AmountInUSD
                if: {Block: {Time: {after: $time_5min_ago}}}
              )
              buy_volume: sum(
                of: Trade_Side_AmountInUSD
                if: {Trade: {Side: {Type: {is: buy}}}}
              )
              buy_volume_5min: sum(
                of: Trade_Side_AmountInUSD
                if: {Trade: {Side: {Type: {is: buy}}}, Block: {Time: {after: $time_5min_ago}}}
              )
              sell_volume: sum(
                of: Trade_Side_AmountInUSD
                if: {Trade: {Side: {Type: {is: sell}}}}
              )
              sell_volume_5min: sum(
                of: Trade_Side_AmountInUSD
                if: {Trade: {Side: {Type: {is: sell}}}, Block: {Time: {after: $time_5min_ago}}}
              )
              buys: count(if: {Trade: {Side: {Type: {is: buy}}}})
              buys_5min: count(
                if: {Trade: {Side: {Type: {is: buy}}}, Block: {Time: {after: $time_5min_ago}}}
              )
              sells: count(if: {Trade: {Side: {Type: {is: sell}}}})
              sells_5min: count(
                if: {Trade: {Side: {Type: {is: sell}}}, Block: {Time: {after: $time_5min_ago}}}
              )
            }
          }
        }
        """
        variables = {
          "token": token,
          "side_token": side_token,
          "pair_address": pair_address,
          "time_5min_ago": Utils.formatted_date(delta_seconds=-300) + 'Z',
          "time_1h_ago": Utils.formatted_date(delta_seconds=-3600) + 'Z'
        }

        _log("Variables", variables)

        payload = {
          "query": query,
          "variables": variables
        }
        
        response_data = self._fetch(
            url=self.eap_url, 
            method="post", 
            data=json.dumps(payload),
        )
        
        try:
            _log("BitQuery", response_data)
            return response_data["data"]["Solana"]["DEXTradeByTokens"][0]
        except (KeyError, TypeError) as e:
            _log(f"Error parsing BitQuerySolana response: {e}", level="ERROR")
            return []

    # --------------------------
    # Utils
    # --------------------------
         
    def _get_access_token(self):
      """
      Generates an OAuth2 access token for BitQuery.
      """

      if not self.client_id or not self.client_secret:
          raise ValueError("Client ID and secret are required for token generation.")
          
      payload = {
          "grant_type": "client_credentials",
          "client_id": self.client_id,
          "client_secret": self.client_secret,
          "scope": "api"
      }
      
      headers = {
          "Content-Type": "application/x-www-form-urlencoded"
      }

      try:
          response = self.session.post(self.oauth_url, data=payload, headers=headers)
          response.raise_for_status()
          access_token_data = response.json()
          return access_token_data.get("access_token")
      except requests.exceptions.RequestException as e:
          _log(f"Error generating BitQuery access token: {e}", level="ERROR")
          return None
          
    def _fetch(_self, url: str, method: str = "get", params: Optional[dict] = None, data: Optional[Any] = None, headers: Optional[dict] = None):
        """
        Fetches data from the specified URL using a common API call.
        
        This method handles both GET and POST requests and includes headers.
        """
        if not url.startswith("http"):
            url = _self.base_url + url

        if headers is None:
            headers = {}
        
        # Generate and use the OAuth2 access token
        access_token = _self._get_access_token()
        if not access_token:
            raise RuntimeError("Failed to obtain BitQuery access token.")
            
        headers["Authorization"] = f"Bearer {access_token}"
        headers["Content-Type"] = "application/json"
        
        if method.lower() == "get":
            response = _self.session.get(url, params=params, headers=headers)
        elif method.lower() == "post":
            response = _self.session.post(url, data=data, headers=headers)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        response.raise_for_status()
        return response.json()