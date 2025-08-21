import streamlit as st
import json
from dotenv import load_dotenv, find_dotenv

from services.AppData import AppData
from services.BitQuerySolana import BitQuerySolana
from services.SolanaTokenSummary import SolanaTokenSummary
from services.CoinGecko import CoinGecko

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

    # Set logo
    assets_dir = app_data.get_assets_dir()
    # st.logo(f"{assets_dir}logo.svg", size="large")

    # Set page title
    st.set_page_config(page_title="Home", page_icon="🐸", layout="wide")

    # Styles
    with open(f"{assets_dir}style.css") as css:
        st.markdown(f"<style>{css.read()}</style>", unsafe_allow_html=True)

    st.title("🐸 Meme Coin Analyzer")
    
    # Get latest meme coins from BitQuery
    coins = app_data.get_state("latest_tokens")
    if not coins:
        coins = {}
        meme_coins = bitquery.get_latest_tokens(platform="pump.fun", min_liquidity=10000, limit=10)

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
    df_status = solana.get_token_summary_df(token, pair_address)

    # Convert any json cells to string
    df_status = df_status.applymap(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

    st.dataframe(df_status.T.rename_axis("Agg Token Summary"), use_container_width=True)

    st.markdown("### Token Summary (BitQuery)")
    df_summary = bitquery.get_token_pair_summary_df(token, pair_address)
    st.dataframe(df_summary.T.rename_axis("BitQuery Summary"), use_container_width=True)

    st.markdown("### Recent Trades (BitQuery)")
    df_recent_transactions = bitquery.get_recent_pair_tx_df(token, pair_address)
    st.dataframe(df_recent_transactions, use_container_width=True)

    st.markdown("### Processed DataFrame")
    # Get transaction_maker_age_days for every transaction_maker
    df_recent_transactions['transaction_maker_age_days'] = df_recent_transactions['transaction_maker'].apply(solana._solscan_estimate_wallet_age)

    # Merge the token status df (Since it is 1 row, copy it for each transaction)
    df_status = df_status.merge(df_summary, how="cross")
    df_recent_transactions = df_status.merge(df_recent_transactions, how="cross")

    st.dataframe(df_recent_transactions, use_container_width=True)


    # col1, col2 = st.columns(2)
    # with col1:
    #     with st.container(border=True):
    #         st.write(
    #             """
    #             ### 🏠 BOX 1
    #             Description
    #         """
    #         )

    # with col2:
    #     with st.container(border=True):
    #         st.write(
    #             """
    #             ### ✏️ BOX 2
    #             Description
    #         """
    #         )


# --------------------------
# INIT
# --------------------------
if __name__ == "__main__":
    Home()
