import os
import json
import pandas as pd
from requests.exceptions import RequestException
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, Dict, Any, List

from services.AppData import AppData
from services.BitQuerySolana import BitQuerySolana
from services.SolanaTokenSummary import SolanaTokenSummary

from lib.LocalCache import cache_handler
from lib.Utils import Utils

DEFAULT_CACHE_TTL = 300
MINUTE_IN_SECONDS = 60
DAYS_IN_SECONDS = 24 * 60 * 60

class CoinTrainingDataPrep:
    """
    Parser for preparing training data for a given coin pair on Solana.
    """
    def __init__(self):
        self.app_data = AppData()
        self.bitquery = BitQuerySolana()
        self.solana = SolanaTokenSummary()

    @cache_handler.cache(ttl_s=2)
    def get_raw_pair_training_data(self, mint_address: str, pair_address: str, save: bool = False) -> Optional[dict]:
        """
        Get raw training data for a token pair.

        Args:
            mint_address (str): The mint address of the token.
            pair_address (str): The pair address of the token.
            save (bool): Whether to save the data to a file.

        Returns:
            Optional[dict]: The raw training data for the token pair.
        """

        # -- Get Solana token summary
        df_sol_summary = self.solana.get_token_summary_df(mint_address, pair_address)
        
        # Convert known JSON cells to key: value, key: value
        cells_to_convert = ['dex_socials', 'dex_websites', 'be_metadata']
        for cell in cells_to_convert:
            if cell in df_sol_summary.columns:
                df_sol_summary[cell] = df_sol_summary[cell].apply(Utils.flatten_json_to_string)
                df_sol_summary[cell] = df_sol_summary[cell].str.replace("type: ", "")
                df_sol_summary[cell] = df_sol_summary[cell].str.replace("label: ", "")
                df_sol_summary[cell] = df_sol_summary[cell].str.replace("url: ", "")
        
        # Convert any other json cells to string
        df_sol_summary = df_sol_summary.map(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
        
        # -- Add BitQuery data
        
        # summary (Not needed anymore)
        # df_bitquery_summary = self.bitquery.get_token_pair_24h_summary_df(mint_address, pair_address)

        # recent transactions
        df_bitquery_transactions = self.bitquery.get_recent_pair_tx_df(mint_address, pair_address)
        
        # -- Add processed fields
        
        # add wallets age
        tx_wallets = df_bitquery_transactions['bq_transaction_maker'].unique().tolist()
        tx_ages = self.bitquery.estimate_wallets_age(tx_wallets)
        df_bitquery_transactions['bq_transaction_maker_age_days'] = df_bitquery_transactions['bq_transaction_maker'].map(tx_ages)
        df_bitquery_transactions['bq_transaction_maker_age_days'] = df_bitquery_transactions['bq_transaction_maker_age_days'].replace({-1: 0})

        # add market cap
        be_total_supply = self.solana._birdeye_get_token_supply(mint_address)
        df_bitquery_transactions['bq_market_cap'] = df_bitquery_transactions['bq_trade_priceinusd'] * be_total_supply

        # -- Merge DataFrames
        
        # (Not needed anymore)
        # df_sol_summary = df_sol_summary.merge(df_bitquery_summary, how="cross")

        # Add context_ prefix to all columns
        df_sol_summary = df_sol_summary.rename(columns=lambda x: f"context_{x}" if x != "context" else x)

        # -- Add Current Transactions
        df_merged = df_sol_summary.merge(df_bitquery_transactions, how="cross")

        # -- Remove unwanted columns
        cols_to_remove = [
            "context_rc_liquidity_locked",
            "context_rc_is_mutable",
            "context_rc_is_freezeable",
            "context_rc_liquidity_locked_tokens",

            "context_be_top10_holders_plus_creator_percentage",
            "context_be_pre_market_holder",
            "context_be_creation_tx",
            "context_be_creator_percentage",
            "context_be_token_creation_tx",
            "context_be_price_usd",
            "context_be_mint_tx",
            "context_be_token_mint_tx",
            "context_be_mint_timestamp",
            "context_be_mint_date",
            "context_be_token_mint_date",
            "context_be_traded_volume_24h_usd",
            "context_be_unique_traders_24h",
            "context_be_unique_traders_24h",
            
            "context_dex_fdv",
            "context_dex_volume_h24",
            "context_dex_volume_h6",
            "context_dex_volume_h1",
            "context_dex_volume_m5",
            "context_dex_price_change_h6",
            "context_dex_price_change_h24",
            "context_dex_price_usd",
            "context_dex_liquidity_pool_usd",
            "context_dex_txns_m5",
            "context_dex_txns_h1",
            "context_dex_txns_h6",
            "context_dex_txns_h24",

            "context_bq_trade_currency_wrapped",
            "context_bq_trade_currency_symbol",
            "context_bq_trade_currency_ismutable",
            "context_bq_trade_currency_mintaddress",
            "context_bq_trade_currency_updateauthority",
            "context_bq_trade_side_currency",
            "context_bq_trade_end",
            "context_bq_trade_start",
            "context_bq_trade_dex_programaddress",
            "context_bq_trade_dex_protocolfamily",
            "context_bq_trade_market_marketaddress",
            "context_bq_trade_priceagainstsidecurrency",
            "context_bq_trade_min5",
            "context_bq_buyers",
            "context_bq_buys",
            "context_bq_buys_24h",
            "context_bq_buy_volume",
            "context_bq_buy_volume_5min",
            "context_bq_buy_volume_24h",
            "context_bq_buys_5min",
            "context_bq_buyers_5min",
            "context_bq_buyers_24h",
            "context_bq_makers",
            "context_bq_makers_24h",
            "context_bq_makers_5min",
            "context_bq_sell_volume",
            "context_bq_sell_volume_5min",
            "context_bq_sell_volume_24h",
            "context_bq_sellers",
            "context_bq_sellers_5min",
            "context_bq_sellers_24h",
            "context_bq_sells",
            "context_bq_sells_5min",
            "context_bq_sells_24h",
            "context_bq_traded_volume",
            "context_bq_traded_volume_5min",
            "context_bq_traded_volume_24h",
            "context_bq_trades",
            "context_bq_trades_5min",
            "context_bq_trades_24h",
            
            "bq_market_marketaddress",
            "bq_trade_market_marketaddress",
            "bq_trade_priceagainstsidecurrency",
            "bq_transaction_feepayer",
        ]
        df_merged = df_merged.drop(columns=cols_to_remove, errors='ignore')

        # -- Rename Columns
        cols_to_rename = {
            "context_be_creation_date": "context_be_token_creation_date",
            "context_bq_transaction_fee": "context_bq_transaction_fee_sol",
            "bq_trade_amount": "bq_trade_amount_token",
            "bq_transaction_fee": "bq_transaction_fee_side",
            "bq_market_cap": "bq_mc_usd",
        }
        df_merged = df_merged.rename(columns=cols_to_rename)

        # Standardize token symbol
        df_merged["context_token_symbol"] = df_merged["bq_trade_currency_symbol"]
        
        # -- Set Data Types

        # Specific Integer Columns
        int_cols = [
            "context_rc_lp_tokens_locked",
        ]
        for col in int_cols:
            if col in df_merged.columns:
                df_merged[col] = pd.to_numeric(df_merged[col], downcast="integer", errors="coerce")

        # Numeric Columns
        num_cols = [
            "bq_trade_amount_token",
            "bq_trade_priceinusd",
            "bq_transaction_feeinusd",
            "bq_mc_usd",
            "context_be_token_price_usd",
            "context_be_token_total_supply",
            "context_be_creator_net_worth_usd",
            "context_be_token_holders",
            "context_be_top10_holder_percentage",
            "context_be_liquidity_pool_usd",
            "context_dex_mc_usd"
        ]
        for col in num_cols:
            if col in df_merged.columns:
                df_merged[col] = pd.to_numeric(df_merged[col], errors="coerce")

        # Datetime Columns
        dt_cols = [
            "context_be_token_creation_time",
            "context_be_pool_creation_time",
            "bq_block_time"
        ]
        for col in dt_cols:
            if col in df_merged.columns:
                df_merged[col] = pd.to_datetime(df_merged[col], errors="coerce")

        # -- Store Data
        if save:
            coin_name = df_merged['bq_trade_currency_symbol'].iloc[0]
            store_time = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            self.store_data(df_merged, f"ctd_{coin_name}_{pair_address}_{store_time}.parquet")

        return df_merged
    
    def store_data(self, data: pd.DataFrame, filename: str):
        """
        Store the DataFrame to a parquet file.

        Args:
            data (pd.DataFrame): The DataFrame to store.
            filename (str): The filename to store the DataFrame as.
        """
        storage_dir = self.app_data.get_config("permanent_storage_dir")
        if not os.path.exists(storage_dir):
            os.makedirs(storage_dir)

        data.to_parquet(os.path.join(storage_dir, filename), index=False)
