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
# Features
# ---------------------------

def compute_metrics(combined_df):
    metrics = {}
    
    # --------------------------
    # DEV
    # --------------------------

    # Unique devs
    devs = combined_df["context_be_creator_address"].unique()
    metrics["number_of_dev_wallets"] = len(devs)

    # Dev token count
    dev_agg_pools = combined_df.groupby("context_pair_address")["context_ss_creator_pools_created"].max()
    metrics["dev_avg_pools_created"] = dev_agg_pools.mean()
    metrics["dev_total_pools_created"] = dev_agg_pools.sum()
    metrics["dev_min_pools_created"] = dev_agg_pools.min()
    metrics["dev_max_pools_created"] = dev_agg_pools.max()

    # Dev wallet age
    dev_agg_age = combined_df.groupby("context_be_creator_address")["context_ss_creator_wallet_age_days"].max()
    metrics["dev_avg_wallet_age"] = dev_agg_age.mean()
    metrics["dev_min_wallet_age"] = dev_agg_age.min()
    metrics["dev_max_wallet_age"] = dev_agg_age.max()

    # Dev net worth
    dev_agg_net_worth = combined_df.groupby("context_be_creator_address")["context_be_creator_net_worth_usd"].max()
    metrics["dev_avg_net_worth"] = dev_agg_net_worth.mean()
    metrics["dev_min_net_worth"] = dev_agg_net_worth.min()
    metrics["dev_max_net_worth"] = dev_agg_net_worth.max()

    # Dev buy/sell amounts
    dev_trades = combined_df[combined_df["bq_transaction_maker"].isin(devs)]
    dev_buy_amount = dev_trades[dev_trades["bq_trade_side_type"] == "buy"]["bq_trade_side_amount"].sum()
    dev_sell_amount = dev_trades[dev_trades["bq_trade_side_type"] == "sell"]["bq_trade_side_amount"].sum()
    metrics["dev_bought_amount"] = dev_buy_amount
    metrics["dev_sold_amount"] = dev_sell_amount

    # --------------------------
    # TX
    # --------------------------

    # Fastest & avg transaction time
    combined_df["bq_block_time"] = pd.to_datetime(combined_df["bq_block_time"])
    combined_df["context_be_token_creation_time"] = pd.to_datetime(combined_df["context_be_token_creation_time"])
    combined_df["tx_delay"] = (combined_df["bq_block_time"] - combined_df["context_be_token_creation_time"]).dt.total_seconds()

    metrics['tx_total_buys'] = (combined_df["bq_trade_side_type"] == "buy").sum()
    metrics['tx_total_sells'] = (combined_df["bq_trade_side_type"] == "sell").sum()

    metrics['tx_unique_wallets'] = combined_df["bq_transaction_maker"].nunique()
    metrics["tx_fastest_tx_time"] = combined_df["tx_delay"].min()
    metrics["tx_avg_block_time"] = combined_df["tx_delay"].mean()
    metrics["tx_avg_amount_side"] = combined_df["bq_trade_side_amount"].mean()
    metrics["tx_avg_wallet_age_days"] = combined_df["bq_transaction_maker_age_days"].mean()
    metrics["tx_min_wallet_age_days"] = combined_df["bq_transaction_maker_age_days"].min()
    metrics["tx_max_wallet_age_days"] = combined_df["bq_transaction_maker_age_days"].max()

    # --------------------------
    # POOLS
    # --------------------------
    
    # Pools
    metrics["how_many_pools"] = combined_df["context_pair_address"].nunique()

    # Pool Age in Minutes
    age_agg = combined_df.groupby("context_pair_address")["context_be_pool_creation_time"].min()
    pool_age_minutes = (pd.to_datetime("now", utc=True) - age_agg).dt.total_seconds() / 60
    metrics["pool_avg_age_mins"] = pool_age_minutes.mean()
    metrics["pool_min_age_mins"] = pool_age_minutes.min()
    metrics["pool_max_age_mins"] = pool_age_minutes.max()

    # Liquidity
    lp_agg_df = combined_df.groupby("context_pair_address")["context_be_liquidity_pool_usd"].max()
    metrics["lp_total_usd"] = lp_agg_df.sum()
    metrics["lp_avg_usd"] = lp_agg_df.mean()
    metrics["lp_min_usd"] = lp_agg_df.min()
    metrics["lp_max_usd"] = lp_agg_df.max()
    
    # Market Cap
    mc_agg_df = combined_df.groupby("context_pair_address")["bq_mc_usd"].max()
    metrics["mc_total_usd"] = mc_agg_df.sum()
    metrics["mc_avg_usd"] = mc_agg_df.mean()
    metrics["mc_min_usd"] = mc_agg_df.min()
    metrics["mc_max_usd"] = mc_agg_df.max()

    # --------------------------
    # COIN
    # --------------------------
    
    # --------------------------
    # Security
    # --------------------------
    sec_agg = combined_df.groupby("context_pair_address").first()
    metrics["freezable_tokens"] = sec_agg["context_rc_is_freezable"].sum()
    metrics["no_mint"] = (sec_agg["context_rc_mint_authority"] == False).sum()
    metrics["lp_locked"] = (sec_agg["context_rc_is_liquidity_locked"] == True).sum()
    metrics["mutable_metadata"] = sec_agg["context_be_mutable_metadata"].sum()
    metrics["non_transferable"] = sec_agg["context_be_non_transferable"].sum()
    metrics["has_transfer_tax"] = sec_agg["context_be_has_transfer_tax"].sum()

    metrics["avg_rugcheck_score"] = sec_agg["context_rc_risk_score"].mean()
    metrics["min_rugcheck_score"] = sec_agg["context_rc_risk_score"].min()
    metrics["max_rugcheck_score"] = sec_agg["context_rc_risk_score"].max()

    # Count of coins with social media
    social_media_platforms = ['twitter', 'telegram', 'discord']
    social_media_pattern = '|'.join(social_media_platforms)
    metrics["meta_social_media_count"] = sec_agg["context_be_metadata"].str.contains(
        social_media_pattern, case=False, na=False
    ).sum()

    # Count of coins with a website
    metrics["meta_website_count"] = sec_agg["context_be_metadata"].str.contains(
        "website", case=False, na=False
    ).sum()


    return metrics

# --------------------------
# Page
# ---------------------------
def Page():
    
    ct_data = CoinTrainingDataPrep()

    # Set page title
    st.set_page_config(page_title="Home", page_icon="🐸", layout="wide")
    
    st.title("🐸 Dataset Comparer")

    available_datasets = ct_data.list_available_raw_training_metadata()

    # Select Specific Datasets
    enable_picking = st.toggle("Pick Specific Datasets", value=False, key="select_all_datasets")
    if enable_picking:
        selected_datasets = st.multiselect("Select Datasets for Comparison:", options=[dataset["symbol"] + " (" + dataset["pair_address"] + ")" for dataset in available_datasets])
        # Parse the selected dataset symbols
        selected_addresses = [dataset.split(" (")[1][:-1] for dataset in selected_datasets]
        # Match the selected addresses with the available datasets
        matched_datasets = [dataset for dataset in available_datasets if dataset["pair_address"] in selected_addresses]
    else:
        matched_datasets = available_datasets
        
    # Retrieve the DataFrames for the matched datasets
    matched_dataframes = []
    for dataset in matched_datasets:
        df = ct_data.get_raw_training_df(dataset["pair_address"], dataset["filename"])
        matched_dataframes.append(df)

    # Combine all matched DataFrames into a single DataFrame
    if matched_dataframes:
        combined_df = pd.concat(matched_dataframes, ignore_index=True).drop_duplicates()
        st.write("Retrieved Data:")
        st.dataframe(combined_df)
    else:
        st.warning("No Datasets available.")
        return
    
    st.write("#### Filters:")
    
    # Filter by Market Cap (Slider)
    min_dataset_mc = combined_df["bq_mc_usd"].min()
    max_dataset_mc = combined_df["bq_mc_usd"].max()
    market_cap_range = st.slider("Select Market Cap Range (USD):", min_value=min_dataset_mc, max_value=max_dataset_mc, value=(1000.0, max_dataset_mc), step=100.00)
    combined_df = combined_df[(combined_df["bq_mc_usd"] >= market_cap_range[0]) & (combined_df["bq_mc_usd"] <= market_cap_range[1])]

    if combined_df.empty:
        st.warning("No data available after applying the filters.")
        return
    
    st.write("---")
    # Compute insights
    metrics = compute_metrics(combined_df)

    # Display in columns
    total_coins = combined_df['context_pair_address'].nunique()
    st.write(f"## Analyzing {total_coins} Coins / {combined_df.shape[0]} Transactions")
    st.write("")

    st.write("### 🔎 Dev Insights")
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Unique Dev Wallets", metrics["number_of_dev_wallets"])
    col2.metric("Avg Dev Wallet Age (days)", f"{metrics['dev_avg_wallet_age']:.2f}")
    col3.metric("Min-Max Dev Wallet Age (days)", f"{metrics['dev_min_wallet_age']} / {metrics['dev_max_wallet_age']}")
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Avg Pools Created by Dev", f"{metrics['dev_avg_pools_created']:.2f}")
    col2.metric("Min-Max Pools Created by Dev", f"{metrics['dev_min_pools_created']} / {metrics['dev_max_pools_created']}")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Avg Dev Net Worth (USD)", f"${metrics['dev_avg_net_worth']:,.2f}")
    col2.metric("Min-Max Dev Net Worth (USD)", f"${metrics['dev_min_net_worth']:,.2f} / ${metrics['dev_max_net_worth']:,.2f}")

    st.write("---")
    st.write("### 🔒 Security Insights")
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Freezable Tokens", f"{metrics['freezable_tokens']} / {total_coins}")
    col2.metric("NoMint", f"{metrics['no_mint']} / {total_coins}")
    col3.metric("LP Locked", f"{metrics['lp_locked']} / {total_coins}")
    col4.metric("Mutable Metadata", f"{metrics['mutable_metadata']} / {total_coins}")
    col1.metric("Non Transferable", f"{metrics['non_transferable']} / {total_coins}")
    col2.metric("Has Transfer Tax", f"{metrics['has_transfer_tax']} / {total_coins}")
    col3.metric("Avg Rugcheck Score", f"{metrics['avg_rugcheck_score']:.2f}")
    col4.metric("Min-Max Rugcheck Score", f"{metrics['min_rugcheck_score']} / {metrics['max_rugcheck_score']}")
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Has Social Media (Meta)", f"{metrics['meta_social_media_count']} / {total_coins}")
    col2.metric("Has Website (Meta)", f"{metrics['meta_website_count']} / {total_coins}")

    st.write("---")
    st.write("### 🪙 Pool Insights")
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Liquidity Pool (USD)", f"${metrics['lp_total_usd']:,.2f}")
    col2.metric("Avg Liquidity Pool (USD)", f"${metrics['lp_avg_usd']:,.2f}")
    col3.metric("Min-Max Liquidity Pool (USD)", f"${metrics['lp_min_usd']:,.2f} / ${metrics['lp_max_usd']:,.2f}")
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Market Cap (USD)", f"${metrics['mc_total_usd']:,.2f}")
    col2.metric("Avg Market Cap (USD)", f"${metrics['mc_avg_usd']:,.2f}")
    col3.metric("Min-Max Market Cap (USD)", f"${metrics['mc_min_usd']:,.2f} / ${metrics['mc_max_usd']:,.2f}")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Avg Pool Age (Minutes)", f"{metrics['pool_avg_age_mins'] / 60:.2f}", delta=f"{metrics['pool_avg_age_mins']:.0f} seconds")
    col2.metric("Min-Max Pool Age (Minutes)", f"{metrics['pool_min_age_mins'] / 60:.2f} / {metrics['pool_max_age_mins'] / 60:.2f}", delta=f"{metrics['pool_min_age_mins']:.0f} / {metrics['pool_max_age_mins']:.0f} seconds")

    st.write("---")
    st.write("### 🔎 Transactions Insights")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Buys", metrics['tx_total_buys'])
    col2.metric("Total Sells", metrics['tx_total_sells'])
    col3.metric("Avg Tx Amount (SOL)", f"{metrics['tx_avg_amount_side']:.4f}")
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Unique Wallets", metrics['tx_unique_wallets'])
    col2.metric("Avg Wallet Age (days)", f"{metrics['tx_avg_wallet_age_days']:.2f}")
    col3.metric("Min-Max Wallet Age (days)", f"{metrics['tx_min_wallet_age_days']} / {metrics['tx_max_wallet_age_days']}")

    # with col1:
    #     st.metric("Number of Dev Wallets", metrics["number_of_dev_wallets"])
    #     st.metric("Avg Dev Wallet Age (days)", metrics["dev_avg_wallet_age"])
    #     st.metric("Min Dev Wallet Age (days)", metrics["dev_min_wallet_age"])
    #     st.metric("Max Dev Wallet Age (days)", metrics["dev_max_wallet_age"])
    #     # st.metric("Dev Bought Amount", f"{metrics['dev_bought_amount']:.4f}")
    #     # st.metric("Fastest Tx (s)", metrics["tx_fastest_tx_time"])

    # with col2:
    #     # st.metric("Dev Sold Amount", f"{metrics['dev_sold_amount']:.4f}")
    #     st.metric("Avg Tx Amount", f"{metrics['tx_avg_amount_side']:.4f}")
    #     st.metric("Avg Tx Time (s)", f"{metrics['tx_avg_block_time']:.1f}")

    # with col3:
    #     st.metric("How Many Pools", metrics["how_many_pools"])
    #     st.metric("Has Social Media", str(metrics["has_social_media"]))

# --------------------------
# INIT
# --------------------------
if __name__ == "__main__":
    Page()
