import streamlit as st
import json
import pandas as pd
import matplotlib.pyplot as plt

from dotenv import load_dotenv, find_dotenv

from services.AppData import AppData
from services.BitQuerySolana import BitQuerySolana
from services.SolanaTokenSummary import SolanaTokenSummary
from services.CoinTrainingDataPrep import CoinTrainingDataPrep

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

    # Set page title
    st.set_page_config(page_title="Home", page_icon="🐸", layout="wide")
    
    st.title("🐸 Meme Coin Analyzer Concept")
    
    col_token_selector, col_refresh, sep, col_custom_pair = st.columns([4, 1, .5, 4])
    
    with col_refresh:
        st.write("######")
        if st.button("Refresh", use_container_width=True):
            app_data.clear_state("latest_tokens")

    with col_token_selector:
        # Get latest meme coins from BitQuery
        coins = app_data.get_state("latest_tokens")
        if not coins:
            coins = {}
            meme_coins = bitquery.get_latest_tokens(platform="pump.fun", min_liquidity=10000, limit=20)

            # Filter for "best" coins
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

        # Static addresses for testing
        addresses = {
            "BILLY": {
                "mint": "3B5wuUrMEi5yATD7on46hKfej3pfmd7t1RKgrsN3pump",
                "pair": "9uWW4C36HiCTGr6pZW9VFhr9vdXktZ8NA8jVnzQU35pJ"
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

    sep.write()
    
    with col_custom_pair:
        pair_address = st.text_input("Or Inform Pair (Pool) Address", value=app_data.get_state("custom_pair_address") or "")
        if pair_address != "":
            app_data.set_state("custom_pair_address", pair_address)
            token = solana._birdeye_get_mint_from_pair(pair_address)
        else:
            token = addresses[current_latest_token]["mint"]
            pair_address = addresses[current_latest_token]["pair"]
            
    if not token or not pair_address:
        st.error("Token or Pair address is missing or invalid.")
        return
    
    df_sol_summary = solana.get_token_summary_df(token, pair_address)
    token_name = df_sol_summary.loc[0]['token_symbol']
    st.markdown(f"# ℹ️ Token Overview: {token_name}")

    st.markdown("### Token Summary (Aggregated Sources)")
    # Convert any json cells to string
    df_sol_summary = df_sol_summary.applymap(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

    st.dataframe(df_sol_summary.T.rename_axis("Agg Token Summary"), use_container_width=True)

    st.markdown("### Token Summary (BitQuery)")
    df_bitquery_summary = bitquery.get_token_pair_24h_summary_df(token, pair_address)
    st.dataframe(df_bitquery_summary.T.rename_axis("BitQuery Summary"), use_container_width=True)

    st.markdown("### Recent Trades (BitQuery)")
    df_bitquery_recent_transactions = bitquery.get_recent_pair_tx_df(token, pair_address)
    st.dataframe(df_bitquery_recent_transactions, use_container_width=True)

    st.markdown("### Raw Training DataFrame")

    df_raw_training_data = CoinTrainingDataPrep().get_raw_pair_training_data(token, pair_address)
    st.dataframe(df_raw_training_data, use_container_width=True)
    
    
  # Convert numeric cols
    num_cols = ["bq_trade_amount_token", "bq_trade_priceinusd", "bq_transaction_feeinusd", "bq_mc_usd", "context_be_token_price_usd", "context_be_token_total_supply", "context_be_creator_net_worth_usd", "context_be_token_holders", "context_be_top10_holder_percentage", "context_be_liquidity_pool_usd", "context_dex_mc_usd"]
    for col in num_cols:
        if col in df_raw_training_data.columns:
            df_raw_training_data[col] = pd.to_numeric(df_raw_training_data[col], errors="coerce")

    # Add trade value USD
    df_raw_training_data["trade_value_usd"] = df_raw_training_data["bq_trade_amount_token"] * df_raw_training_data["bq_trade_priceinusd"]

    # ---- HEADER ----
    st.markdown("# 📊 Token Analysis")
    st.markdown(f"##### Based on {df_raw_training_data['bq_block_time'].count()} transactions")

    # ---- CONTEXT INFO ----
    st.subheader("Token Context")
    col1, col2, col3 = st.columns(3)
    col1.metric("Risk Score", df_raw_training_data["context_rc_risk_score"].iloc[0])
    col2.metric("Risk Desc", df_raw_training_data["context_rc_risks_desc"].iloc[0])
    col3.metric("Creator Wallet Age (days)", df_raw_training_data["context_ss_creator_wallet_age_days"].iloc[0])

    col4, col5, col6 = st.columns(3)
    col4.metric("Total Token Supply", f"{df_raw_training_data['context_be_token_total_supply'].iloc[0]:,.0f}")
    col5.metric("Holders", df_raw_training_data["context_be_token_holders"].iloc[0])
    col6.metric("Top10 Holder %", f"{df_raw_training_data['context_be_top10_holder_percentage'].iloc[0]:.2f}%")

    col7, col8, col9 = st.columns(3)
    col7.metric("Liquidity Pool (USD)", f"${df_raw_training_data['context_be_liquidity_pool_usd'].iloc[0]:,.2f}")
    col8.metric("Market Cap (USD)", f"${df_raw_training_data['context_dex_mc_usd'].iloc[0]:,.2f}")
    col9.metric("Creator Net Worth (USD)", f"${df_raw_training_data['context_be_creator_net_worth_usd'].iloc[0]:,.2f}")

    st.markdown("---")

    # ---- KPIs ----
    st.subheader("Trading KPIs")
    total_trades = len(df_raw_training_data)
    total_volume_token = df_raw_training_data["bq_trade_amount_token"].sum()
    total_volume_usd = df_raw_training_data["trade_value_usd"].sum()
    avg_trade_size = df_raw_training_data["bq_trade_amount_token"].mean()
    total_fees_usd = df_raw_training_data["bq_transaction_feeinusd"].sum()
    unique_traders = df_raw_training_data["bq_transaction_maker"].nunique()

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Trades", f"{total_trades:,}")
    col2.metric("Volume (Tokens)", f"{total_volume_token:,.2f}")
    col3.metric("Volume (USD)", f"${total_volume_usd:,.2f}")

    col4, col5, col6 = st.columns(3)
    col4.metric("Avg Token Trade Amount", f"{avg_trade_size:,.2f}")
    col5.metric("Total Fees (USD)", f"${total_fees_usd:,.2f}")
    col6.metric("Unique Traders", unique_traders)

    st.markdown("---")

    # ---- CHARTS ----
    st.subheader("Price Over Time (USD)")
    st.line_chart(df_raw_training_data.set_index("bq_block_time")["bq_trade_priceinusd"])

    st.subheader("Trade Volume Over Time (USD)")
    volume_time = df_raw_training_data.groupby(pd.Grouper(key="bq_block_time", freq="1min"))["trade_value_usd"].sum()
    st.line_chart(volume_time)

    st.subheader("Buy vs Sell Distribution")
    st.bar_chart(df_raw_training_data["bq_trade_side_type"].value_counts())

    st.subheader("Top 10 Traders by Volume (USD)")
    top_traders = df_raw_training_data.groupby("bq_transaction_maker")["trade_value_usd"].sum().nlargest(10)
    st.bar_chart(top_traders)


# --------------------------
# INIT
# --------------------------
if __name__ == "__main__":
    Home()
