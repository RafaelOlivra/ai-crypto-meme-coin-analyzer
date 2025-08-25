from turtle import pd
import streamlit as st
import json
from dotenv import load_dotenv, find_dotenv

from services.AppData import AppData
from services.BitQuerySolana import BitQuerySolana
from services.SolanaTokenSummary import SolanaTokenSummary
from services.CoinTrainingDataParser import CoinTrainingDataParser

# --------------------------
# Configurations
# ---------------------------

# Load environment variables
load_dotenv(find_dotenv("../.env"))


# --------------------------
# Page
# ---------------------------
def Home():
    
    app_data = AppData()
    bitquery = BitQuerySolana()
    solana = SolanaTokenSummary()

    assets_dir = app_data.get_assets_dir()

    # Set page title
    st.set_page_config(page_title="Home", page_icon="🐸", layout="wide")

    # Styles
    with open(f"{assets_dir}style.css") as css:
        st.markdown(f"<style>{css.read()}</style>", unsafe_allow_html=True)
    
    st.title("🐸 Meme Coin Analyzer")
    
    col1, col2 = st.columns([8, 2])
    
    with col2:
        if st.button("Refresh Tokens", use_container_width=True):
            app_data.clear_state("latest_tokens")

    with col1:
        # Get latest meme coins from BitQuery
        coins = app_data.get_state("latest_tokens")
        if not coins:
            coins = {}
            meme_coins = bitquery.get_latest_tokens(platform="pump.fun", min_liquidity=10000, limit=20)

            # Temporary dict to keep best version per mint
            best_coins = {}

            for coin in meme_coins:
                details = coin["Pool"]
                base = details['Market']['BaseCurrency']
                mint = base['MintAddress']
                post_amount = float(details['Base']['PostAmount'])

                # If mint not seen yet, or current has higher PostAmount, update it
                if mint not in best_coins or post_amount > best_coins[mint]['post_amount']:
                    best_coins[mint] = {
                        "name": base['Name'],
                        "symbol": base['Symbol'],
                        "mint": mint,
                        "pair": details['Market']['MarketAddress'],
                        "post_amount": post_amount
                    }

            # Build final dict for session state
            for mint, data in best_coins.items():
                coins[data["name"] + " (" + data["symbol"] + ")"] = {
                    "mint": data["mint"],
                    "pair": data["pair"]
                }
            app_data.set_state("latest_tokens", coins, ttl=60)

        addresses = {
            "BILLY": {
                "mint": "3B5wuUrMEi5yATD7on46hKfej3pfmd7t1RKgrsN3pump",
                "pair": "9uWW4C36HiCTGr6pZW9VFhr9vdXktZ8NA8jVnzQU35pJ"
            },
            "INN0": {
                "mint": "E8uL1V5kxzgMSiTczapzocaneGwBreGdKL6SzW2Fpump",
                "pair": "4CjK8NS1EAu3DpJUMBCV1CbPEGLsZMSem5R5ctZ7mSV4"
            },
            "PENGU": {
                "mint": "2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv",
                "pair": "C6ELogyx2aAd4FfMS9YcVQ284sPvD454hNaFGj7WFYYh"
            }
        }

        # Merge with existing addresses
        addresses.update(coins)

        # Add coin selector
        current_latest_token = app_data.get_state("current_latest_token")
        index = list(addresses.keys()).index(current_latest_token) if current_latest_token in addresses else 0
        current_latest_token = st.selectbox("Select a token", options=list(addresses.keys()), index=index)
        app_data.set_state("current_latest_token", current_latest_token)

    token = addresses[current_latest_token]["mint"]
    pair_address = addresses[current_latest_token]["pair"]

    st.markdown("### Token Summary (BirdEye + Dexscreener)")
    df_sol_status = solana.get_token_summary_df(token, pair_address)

    # Convert any json cells to string
    df_sol_status = df_sol_status.applymap(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

    st.dataframe(df_sol_status.T.rename_axis("Agg Token Summary"), use_container_width=True)

    st.markdown("### Token Summary (BitQuery)")
    df_bitquery_summary = bitquery.get_token_pair_24h_summary_df(token, pair_address)
    st.dataframe(df_bitquery_summary.T.rename_axis("BitQuery Summary"), use_container_width=True)

    st.markdown("### Recent Trades (BitQuery)")
    df_bitquery_recent_transactions = bitquery.get_recent_pair_tx_df(token, pair_address)
    st.dataframe(df_bitquery_recent_transactions, use_container_width=True)

    st.markdown("### Raw Training DataFrame")

    df_raw_training_data = CoinTrainingDataParser().get_raw_pair_training_data(token, pair_address)
    st.dataframe(df_raw_training_data, use_container_width=True)

# --------------------------
# INIT
# --------------------------
if __name__ == "__main__":
    Home()
