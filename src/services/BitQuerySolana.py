import json
from time import time
from numpy import record
import requests
import pandas as pd

from typing import Any, Union, Optional, Dict, List

from services.log.Logger import _log
from services.AppData import AppData
from lib.Utils import Utils
from lib.LocalCache import cache_handler

DEFAULT_CACHE_TTL = 5
DAYS_IN_SECONDS = 24 * 60 * 60
YEARS_IN_SECONDS = 365 * DAYS_IN_SECONDS

class BitQuerySolana:
    """
    A base class for handling Solana coin-related operations via BitQuery.
    """
    
    def __init__(self, api_key=None):
        self.client_id = api_key or AppData().get_api_key("bitquery_client_id")
        self.client_secret = api_key or AppData().get_api_key("bitquery_client_secret")

        self.apiv1 = "https://graphql.bitquery.io/"
        self.oauth_url = "https://oauth2.bitquery.io/oauth2/token"
        self.eap_url = "https://streaming.bitquery.io/eap"
        self.session = requests.Session()
        self.IS_QUERYING = False
        
    # --------------------------
    # Api
    # --------------------------
    
    # Coin Info
    
    @cache_handler.cache(ttl_s=YEARS_IN_SECONDS)
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
    
    @cache_handler.cache(ttl_s=1)
    def get_latest_tokens(
          self,
          platform: str = "pump.fun",
          min_liquidity: float = 0.0,
          max_liquidity: float = 1_000_000_000.0,
          limit: int = 5
        ) -> List[Dict]:
        """
        Get the most recent tokens created on a given platform (e.g., pump.fun).

        Args:
            platform (str): The DEX/program name address (default: pump.fun).
            min_liquidity (float): Minimum liquidity in USD.
            max_liquidity (float): Maximum liquidity in USD.
            limit (int): Number of recent tokens to retrieve.

        Returns:
            list: A list of recent token creation data.
        """

        platform_map = {
            "pump.fun": "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
        }

        # Use the platform map to get the correct address or fallback to the provided platform address
        platform_address = platform_map.get(platform, platform)

        query = """
        query (
          $platformAddress: String!,
          $minLiquidity: String!,
          $maxLiquidity: String!,
          $limit: Int!
        ) {
          Solana {
            DEXPools(
              limit: { count: $limit }
              orderBy: { descending: Block_Time }
              where: {
                Pool: {
                  Dex: { ProgramAddress: { is: $platformAddress } },
                  Quote: {
                    PostAmountInUSD: { 
                      ge: $minLiquidity, 
                      le: $maxLiquidity 
                    }
                  }
                }
                Transaction: {
                  Result: { Success: true }
                }
              }
            ) {
              Pool {
                Market {
                  MarketAddress
                  BaseCurrency {
                    Name
                    Symbol
                    MintAddress
                    Uri
                    Decimals
                  }
                  QuoteCurrency {
                    Name
                    Symbol
                    MintAddress
                  }
                }
                Base {
                  PostAmount
                }
                Quote {
                  PostAmount
                  PostAmountInUSD
                  PriceInUSD
                }
              }
            }
          }
        }
        """

        variables = {
            "platformAddress": platform_address,
            "minLiquidity": min_liquidity,
            "maxLiquidity": max_liquidity,
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
            return response_data["data"]["Solana"]["DEXPools"]
        except (KeyError, TypeError) as e:
            _log(f"Error parsing BitQuery response: {e}", level="ERROR")
            return []

    # Token Trades
    
    @cache_handler.cache(ttl_s=1)
    def get_recent_coin_tx_for_all_pools(
        self,
        mint_address: str,
        limit: int = 1000000000
      ) -> List[Dict]:
        """
        Get the most recent transactions for a Solana coin.

        Args:
            mint_address (str): The mint address of the coin (contract_address).
            pair_address (str): Mint address of the specific market pair/liquidity pool.
            limit (int): The number of recent transactions to retrieve.

        Returns:
            list: A list of recent transaction data.
        """
        
        query = """
        query ($mintAddress: String!, $limit: Int!) {
          Solana(network: solana) {
            DEXTradeByTokens(
              where: {
                Trade: { 
                  Currency: { MintAddress: {is: $mintAddress} }
                },
                Transaction: {Result: {Success: true}}
              },
              limit: {count: $limit},
              orderBy: { descending: Block_Time }
            ) {
              Trade {
                Amount
                AmountInUSD
                PriceInUSD
                Currency {
                  Symbol
                }
                Market {
                  MarketAddress
                }
                Side {
                  Amount
                  Currency {
                    Symbol
                  }
                  Type
                }
              }
              Block {
                Time
              }
              Transaction {
                Fee
                FeeInUSD
                FeePayer
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
            return response_data["data"]["Solana"]["DEXTradeByTokens"]
        except (KeyError, TypeError) as e:
            _log(f"Error parsing BitQuery response: {e}", level="ERROR")
            return []
   
    @cache_handler.cache(ttl_s=60)
    def get_recent_pair_tx(
          self,
          mint_address: str,
          pair_address: str,
          limit: int = 1000000000
        ):
        """
        Get recent trades for the token.

        Args:
            mint_address (str): Mint address of the token to analyze (base token).
            pair_address (str): Mint address of the specific market pair/liquidity pool.

        Returns:
            dict: A dictionary containing the token's summary statistics, such as price,
                  volume, liquidity, and other market indicators.
        """
        query = """
        query ($mintAddress: String!, $pairAddress: String!, $limit: Int!) {
          Solana {
              DEXTradeByTokens(
                  where: {
                      Trade: {
                          Market: { MarketAddress: { is: $pairAddress } }
                          Currency: { MintAddress: { is: $mintAddress } }
                          Dex: { ProgramAddress: {} }
                      }
                      Transaction: { Result: { Success: true } }
                  },
                  limit: { count: $limit }
              ) {
                  Block {
                      Time
                      Hash
                  }
                  Trade {
                      Market {
                        MarketAddress
                      }
                      Currency {
                          Symbol
                      }
                      Amount
                      PriceAgainstSideCurrency: Price
                      PriceInUSD
                      Side {
                          Currency {
                              Symbol
                          }
                          Amount
                          Type
                      }
                  }
                  Transaction {
                      Maker: Signer
                      Fee
                      FeeInUSD
                      FeePayer
                  }
              }
          }
        }
        """
        
        variables = {
          "mintAddress": mint_address,
          "pairAddress": pair_address,
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
            return response_data["data"]["Solana"]["DEXTradeByTokens"]
        except (KeyError, TypeError) as e:
            _log(f"Error parsing BitQuery response: {e}", level="ERROR")
            return []
    
    @cache_handler.cache(ttl_s=60)
    def get_recent_pair_tx_df(
          self,
          mint_address: str,
          pair_address: str,
          limit: int = 3000 # Max allowed is 1000000000
        ) -> pd.DataFrame:
        """
        Get recent trades for the token as a DataFrame.
        
        Args:
            mint_address (str): Mint address of the token to analyze (base token).
            pair_address (str): Mint address of the specific market pair/liquidity pool.

        Returns:
            pd.DataFrame: A DataFrame containing the token's recent trade data.
        """
        trades = self.get_recent_pair_tx(
            mint_address=mint_address,
            pair_address=pair_address,
            limit=limit
        )

        def flatten_trade_record(record):
            flat = {}

            # Block info
            block = record.get("Block", {})
            for k, v in block.items():
                flat[f"block_{k}"] = v

            # Trade info
            trade = record.get("Trade", {})
            for k, v in trade.items():
                if isinstance(v, dict):
                    for sub_k, sub_v in v.items():
                        # Side.Currency nested dict
                        if sub_k == "Side" and isinstance(sub_v, dict):
                            side = sub_v
                            for side_k, side_v in side.items():
                                if isinstance(side_v, dict):
                                    for subsub_k, subsub_v in side_v.items():
                                        flat[f"trade_side_{subsub_k}"] = subsub_v
                                else:
                                    flat[f"trade_side_{side_k}"] = side_v
                        elif isinstance(sub_v, dict):
                            for subsub_k, subsub_v in sub_v.items():
                                flat[f"trade_{k}_{subsub_k}"] = subsub_v
                        else:
                            flat[f"trade_{k}_{sub_k}"] = sub_v
                else:
                    flat[f"trade_{k}"] = v

            # Transaction info
            transaction = record.get("Transaction", {})
            for k, v in transaction.items():
                flat[f"transaction_{k}"] = v

            # Lowercase all column names
            flat = {k.lower(): v for k, v in flat.items()}

            return flat

        # Flatten all records and create DataFrame
        df = pd.DataFrame([flatten_trade_record(r) for r in trades])

        # Convert numeric columns from strings to float where possible
        for col in df.columns:
            try:
                df[col] = df[col].astype(float)
            except (ValueError, TypeError):
                pass

        # Add bq_ prefix to all columns
        df = df.rename(columns=lambda x: f"bq_{x}" if not x.startswith("bq_") else x)

        return df

    # Summary

    @cache_handler.cache(ttl_s=DEFAULT_CACHE_TTL)
    def get_token_pair_24h_summary(
          self,
          mint_address: str,
          pair_address: str,
          time: int = 0
        ) -> Optional[Dict]:
        """
        Retrieve a 24-hour trading summary for a specific token in a given market.

        In the context of (a Solana-based token analytics/trading platform):
        - `mint_address` is the **mint address** of the token you want to analyze (the "base token").
        - `pair_address` is the **mint address of the liquidity pool or market pair**
          in which the token is traded.

        Args:
            mint_address (str): Mint address of the token to analyze (base token).
            pair_address (str): Mint address of the specific market pair/liquidity pool.
            time (int): The specific time to base the query on (default: 0 = Current time).

        Returns:
            dict: A dictionary containing the token's summary statistics, such as price,
                  volume, liquidity, and other market indicators.
        """
        query = """
        query ($mintAddress: String!, $pairAddress: String!, $time5minAgo: DateTime!, $time24hAgo: DateTime!) {
          Solana(dataset: realtime) {
            DEXTradeByTokens(
              where: {
                Transaction: {
                  Result: { Success: true }
                },
                Trade: {
                  Currency: { MintAddress: { is: $mintAddress } },
                  Market: { MarketAddress: { is: $pairAddress } }
                },
                Block: { Time: { since: $time24hAgo } }
              }
            ) {
              Trade {
                Currency {
                  MintAddress
                  Symbol
                  UpdateAuthority
                  IsMutable
                  Fungible
                  Wrapped
                }
                start: PriceInUSD(minimum: Block_Time)
                min5: PriceInUSD(
                  minimum: Block_Time
                  if: {Block: {Time: {after: $time5minAgo}}}
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
                    MintAddress
                  }
                }
              }
              makers_24h: count(distinct: Transaction_Signer)
              makers_5min: count(
                distinct: Transaction_Signer
                if: {Block: {Time: {after: $time5minAgo}}}
              )
              buyers_24h: count(
                distinct: Transaction_Signer
                if: {Trade: {Side: {Type: {is: buy}}}}
              )
              buyers_5min: count(
                distinct: Transaction_Signer
                if: {Trade: {Side: {Type: {is: buy}}}, Block: {Time: {after: $time5minAgo}}}
              )
              sellers_24h: count(
                distinct: Transaction_Signer
                if: {Trade: {Side: {Type: {is: sell}}}}
              )
              sellers_5min: count(
                distinct: Transaction_Signer
                if: {Trade: {Side: {Type: {is: sell}}}, Block: {Time: {after: $time5minAgo}}}
              )
              trades_24h: count
              trades_5min: count(if: {Block: {Time: {after: $time5minAgo}}})
              traded_volume_24h: sum(of: Trade_Side_AmountInUSD)
              traded_volume_5min: sum(
                of: Trade_Side_AmountInUSD
                if: {Block: {Time: {after: $time5minAgo}}}
              )
              buy_volume_24h: sum(
                of: Trade_Side_AmountInUSD
                if: {Trade: {Side: {Type: {is: buy}}}}
              )
              buy_volume_5min: sum(
                of: Trade_Side_AmountInUSD
                if: {Trade: {Side: {Type: {is: buy}}}, Block: {Time: {after: $time5minAgo}}}
              )
              sell_volume_24h: sum(
                of: Trade_Side_AmountInUSD
                if: {Trade: {Side: {Type: {is: sell}}}}
              )
              sell_volume_5min: sum(
                of: Trade_Side_AmountInUSD
                if: {Trade: {Side: {Type: {is: sell}}}, Block: {Time: {after: $time5minAgo}}}
              )
              buys_24h: count(if: {Trade: {Side: {Type: {is: buy}}}})
              buys_5min: count(
                if: {Trade: {Side: {Type: {is: buy}}}, Block: {Time: {after: $time5minAgo}}}
              )
              sells_24h: count(if: {Trade: {Side: {Type: {is: sell}}}})
              sells_5min: count(
                if: {Trade: {Side: {Type: {is: sell}}}, Block: {Time: {after: $time5minAgo}}}
              )
            }
          }
        }
        """
        
        variables = {
          "mintAddress": mint_address,
          "pairAddress": pair_address,
          "time5minAgo": Utils.formatted_date(time, delta_seconds=-300),
          "time24hAgo": Utils.formatted_date(time, delta_seconds=-86400)
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
            response_data = response_data["data"]["Solana"]["DEXTradeByTokens"]
            return len(response_data) > 0 and response_data[0] or None
        except (KeyError, TypeError) as e:
            _log(f"Error parsing BitQuery response: {e}", level="ERROR")
            return None

    def get_token_pair_24h_summary_df(
          self,
          mint_address: str,
          pair_address: str,
          time: int = 0
        ) -> pd.DataFrame:
        """
        Get a summary DataFrame for the token.

        Args:
            mint_address (str): Mint address of the token to analyze (base token).
            pair_address (str): Mint address of the specific market pair/liquidity pool.
            time (int): The specific time to base the query on (default: 0 = Current time).

        Returns:
            pd.DataFrame: A DataFrame containing the token's summary statistics.
        """
        summary = self.get_token_pair_24h_summary(mint_address, pair_address, time)
        if not summary:
            return pd.DataFrame()

        # Flatten the Trade section
        flat = {}
        trade = summary.get("Trade", {})
        for key, val in trade.items():
            if isinstance(val, dict):
                # Nested dict: flatten further
                for sub_key, sub_val in val.items():
                    flat[f"trade_{key}_{sub_key}"] = sub_val
            else:
                flat[f"trade_{key}"] = val
        
        # Add top-level fields (like buy_volume, sellers, etc.)
        for key, val in summary.items():
            if key != "Trade":
                flat[key] = val
        
        # Lowercase all columns
        flat = {k.lower(): v for k, v in flat.items()}
          
        # Adapt to a pandas Dataframe
        df = pd.DataFrame([flat])
        
        # Add bq_ prefix to all columns
        df = df.rename(columns=lambda x: f"bq_{x}" if not x.startswith("bq_") else x)
        
        return df
    
    # Wallet Info
           
    @cache_handler.cache(ttl_s=DAYS_IN_SECONDS)
    def estimate_wallet_age(self, wallet_address: str) -> Optional[int]:
        """
        Get the wallet age (based on the first transaction) for a Solana wallet.

        Args:
            wallet_address (str): The Solana wallet address to check.

        Returns:
            Optional[int]: The age of the wallet in days, or None if not found.
        """
        try:
            age = self.estimate_wallets_age([wallet_address])
            return age.get(wallet_address)
        except (KeyError, TypeError, IndexError) as e:
            _log(f"Error parsing BitQuery response or wallet not found: {e}", level="ERROR")
            return None
          
    @cache_handler.cache(ttl_s=DAYS_IN_SECONDS)
    def estimate_wallets_age(self, wallet_addresses: list[str]) -> dict[str, Optional[int]]:
        """
        Get the wallet age (based on the first transaction) for multiple Solana wallets.

        Args:
            wallet_addresses (list[str]): List of Solana wallet addresses.

        Returns:
            dict[str, Optional[int]]: Mapping of wallet address -> wallet age in days (or None if not found).
        """

        # Format addresses for GraphQL
        addresses_str = ", ".join([f'"{addr}"' for addr in wallet_addresses])

        query = """
        {
          solana {
            transfers(
              receiverAddress: {in: [ADDRESSES]}
            ) {
              minimum(of: time)
              receiver {
                address
              }
            }
          }
        }
        """
        query = query.replace("ADDRESSES", addresses_str)

        payload = {
            "query": query,
        }

        response_data = self._fetch(
            url=self.apiv1,
            method="post",
            data=json.dumps(payload),
        )

        results: dict[str, Optional[int]] = {}

        try:
            transfers = response_data["data"]["solana"]["transfers"]
            for tx in transfers:
                block_date = tx["minimum"]
                wallet_address = tx["receiver"]["address"]

                if block_date:
                    age = Utils.get_days_since(block_date, format="%Y-%m-%d %H:%M:%S %Z")
                    results[wallet_address] = age
                else:
                    results[wallet_address] = None

        except (KeyError, TypeError) as e:
            _log(f"Error parsing BitQuery response: {e}", level="ERROR")
            # fallback: mark all wallets as None
            results = {addr: None for addr in wallet_addresses}

        return results
    
    # Liquidity
    
    @cache_handler.cache(ttl_s=DEFAULT_CACHE_TTL)
    def get_liquidity_pool_for_pair(
          self,
          pair_address: str,
          time: Optional[str] = None
        ):
        """
        Retrieve the liquidity pool information for a specific market pair.

        Args:
            pair_address (str): The mint address of the market pair to query.
            time (Optional[str]): The specific time to base the query on (default: None).

        Returns:
            dict: The liquidity pool information for the specified market pair.
        """

        # Use current time if not provided
        if not time:
          time = Utils.formatted_date()
          
        query = """
        query ($time: DateTime!, $pairAddress: String!) {
            Solana {
                DEXPools(
                    where: {
                        Pool: {
                            Market: { MarketAddress: { is: $pairAddress }}
                        }
                        Block: { Time: { before: $time } }
                    }
                  limit: {count: 1}
                ) {
                    Pool {
                        Market {
                            MarketAddress
                        }
                        Quote {
                          Price
                          PriceInUSD
                          PostAmount # Liquidity Pool at the time
                          PostAmountInUSD
                        }
                        Base {
                          Price
                        }
                    }
                    Block {
                        Time
                    }
                }
            }
        }
        """
        
        variables = {
          "pairAddress": pair_address,
          "time": Utils.formatted_date(time)
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
            return response_data["data"]["Solana"]["DEXPools"][0]
        except (KeyError, TypeError) as e:
            _log(f"Error parsing BitQuery response: {e}", level="ERROR")
            return []
    
    # Market Cap
         
    @cache_handler.cache(ttl_s=DEFAULT_CACHE_TTL)
    def get_market_cap(
          self,
          mint_address: str,
          times: list[str|int],
        ) -> dict:
        """
        Retrieve the market capitalization for a specific token.

        Args:
            mint_address (str): The mint address of the token to query.
            times (list[str|int]): A list of timestamps to query for the market cap.

        Returns:
            dict: The market capitalization information for the specified token.
        """
        if type(times) is int:
            times = [times]

        # Use current time if not provided
        if not times:
          times = [Utils.formatted_date()]
      
        template = """
        SLUG: TokenSupplyUpdates(
          where: {
            TokenSupplyUpdate: {Currency: {MintAddress: {is: "MINT_ADDRESS"}}},
            Block: {Time: {till: "TIME"}}
          }
          limit: {count: 1}
          orderBy: {ascending: Block_Time}
        ) {
          TokenSupplyUpdate {
            MarketCap: PostBalanceInUSD
            Supply: PostBalance
            Currency {
              Symbol
              Name
            }
          }
          Block {
            Time
          }
        }
        """
        
        base_query = ""
        for time in times:
            query_part = template.replace("SLUG", Utils.time_slugify(time)).replace("MINT_ADDRESS", mint_address).replace("TIME", Utils.formatted_date(time))
            base_query += query_part + "\n"

        query = """
        query {
            Solana {
              """ + base_query + """
            }
        }
        """

        payload = {
          "query": query
        }
        
        response_data = self._fetch(
            url=self.eap_url, 
            method="post", 
            data=json.dumps(payload),
        )
        
        try:
            mc = {}
            response = response_data["data"]["Solana"]
            for t in times:
                slug = Utils.time_slugify(t)
                if slug in response and len(response[slug]) > 0:
                    market_cap = response[slug][0]["TokenSupplyUpdate"]["MarketCap"]
                    mc[t] = float(market_cap) if market_cap else 0.0
            return mc

        except (KeyError, TypeError) as e:
            _log(f"Error parsing BitQuery response: {e}", level="ERROR")
            return {}
          
    @cache_handler.cache(ttl_s=DEFAULT_CACHE_TTL)
    def get_market_cap_df(
          self,
          mint_address: str,
          times: list[str|int],
        ) -> pd.DataFrame:
        """
        Retrieve the market capitalization for a specific token.

        Args:
            mint_address (str): The mint address of the token to query.
            times (list[str|int]): A list of timestamps to query for the market cap.

        Returns:
            dict: The market capitalization information for the specified token.
        """
        mc = self.get_market_cap(mint_address, times)
        df = pd.DataFrame(list(mc.items()), columns=["bq_block_time", "bq_estimated_market_cap"])
        return df

    
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
          
    def _fetch(self, url: str, method: str = "get", params: Optional[dict] = None, data: Optional[Any] = None, headers: Optional[dict] = None):
        """
        Fetches data from the specified URL using a common API call.
        
        This method handles both GET and POST requests and includes headers.
        """
        if self.IS_QUERYING:
            _log("Query is already in progress", level="WARNING")
            return {"error": "Query is already in progress"}

        if not url.startswith("http"):
            url = self.apiv1 + url

        if headers is None:
            headers = {}
        
        # Generate and use the OAuth2 access token
        access_token = self._get_access_token()
        if not access_token:
            raise RuntimeError("Failed to obtain BitQuery access token.")
            
        headers["Authorization"] = f"Bearer {access_token}"
        headers["Content-Type"] = "application/json"
        
        if method.lower() == "get":
            self.IS_QUERYING = True
            response = self.session.get(url, params=params, headers=headers)
        elif method.lower() == "post":
            self.IS_QUERYING = True
            response = self.session.post(url, data=data, headers=headers)
        else:
            self.IS_QUERYING = False
            raise ValueError(f"Unsupported HTTP method: {method}")

        self.IS_QUERYING = False
        response.raise_for_status()
        return response.json()