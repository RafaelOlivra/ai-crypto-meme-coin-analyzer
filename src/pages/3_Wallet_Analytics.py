import streamlit as st
import json
import pandas as pd
import matplotlib.pyplot as plt

from dotenv import load_dotenv, find_dotenv

from lib.Utils import Utils
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
    return metrics

# --------------------------
# Page
# ---------------------------
def Page():
    CACHE_TTL = 60 * 60 * 1  # 1 hour
    app_data = AppData()
    solana = SolanaTokenSummary()

    # Set page title
    st.set_page_config(page_title="Wallet Analytics", page_icon="💰", layout="wide")

    st.title("💰 Wallet Analytics")

    wallet_address = st.text_input("Inform Wallet Address", value=app_data.get_state("wa_wallet_address") or "")
    if wallet_address != "":
        app_data.set_state("wa_wallet_address", wallet_address, CACHE_TTL)
        wallet_overview = solana._birdeye_get_wallet_overview(wallet_address)
    else:
        app_data.clear_state("wa_wallet_address")

    if not wallet_address or not wallet_overview:
        st.info("Please enter a valid Solana wallet address to see analytics.")
        return
    
    st.write("### Overview:")
    
    col1, col2 = st.columns(2)
    col1.metric(f"Net Worth ({wallet_overview.get('currency', 'N/A').upper()})", f"{wallet_overview.get('net_worth', 0):,.2f}")
    col2.metric("Requested At", wallet_overview.get("requested_timestamp", "N/A"))
    
    #---------------------------------------
    # Prepare Data for Analysis
    #---------------------------------------
    
    recent_tx = app_data.get_state("wa_wallet_tx")
    if not recent_tx:
        with st.spinner("Loading recent transactions (This may take a while)..."):
            recent_tx = solana._birdeye_get_wallet_trades(wallet_address)
            app_data.set_state("wa_wallet_tx", recent_tx, CACHE_TTL)
    
    if not recent_tx or len(recent_tx) == 0:
        st.info("No recent transactions found for this wallet.")
        return
    df_recent_tx = pd.DataFrame(recent_tx)
    
    recent_traded_tokens = app_data.get_state("wa_wallet_traded_tokens")
    if not recent_traded_tokens:
        with st.spinner("Loading recent traded tokens (This may take a while)..."):
            recent_traded_tokens = solana._birdeye_get_wallet_traded_tokens(wallet_address, max_trades=100)
            app_data.set_state("wa_wallet_traded_tokens", recent_traded_tokens, CACHE_TTL)

    if not recent_traded_tokens or len(recent_traded_tokens) == 0:
        st.info("No recent traded tokens found for this wallet.")
        return
    df_recent_traded_tokens = pd.DataFrame(recent_traded_tokens)

    recent_pnls = app_data.get_state("wa_wallet_pnls")
    if recent_pnls is None:
        with st.spinner("Retrieving PnL for recent transactions (This may take a while)..."):
            recent_pnls = solana._birdeye_get_wallet_tokens_pnl(wallet_address, recent_traded_tokens)
            app_data.set_state("wa_wallet_pnls", recent_pnls, CACHE_TTL)

    if recent_pnls is None:
        st.info("No PnL data found for this wallet's recent transactions.")
        return
    
    parsed_pnls = []
    for mint_address in recent_pnls:
        data = recent_pnls[mint_address]

        block_timestamp = None
        block_number = None
        pair_address = None
        for token in recent_traded_tokens:
            if token.get("mint_address") == mint_address:
                block_timestamp = token.get("block_timestamp", "")
                block_number = token.get("block_number", "")
                pair_address = token.get("pair_address", "")
                break

        parsed_pnls.append({
            "block_timestamp": block_timestamp,
            "block_number": block_number,
            "block_time": Utils.formatted_date(block_timestamp),
            # "analyzed_at": data.get("analyzed_at", ""),
            "mint_address": mint_address,
            "pair_address": pair_address,
            "symbol": data.get("symbol", ""),
            "net_profit_usd": data.get("pnl", {}).get("total_usd", 0),
            "net_profit_percent": data.get("pnl", {}).get("total_percent", 0),
            "avg_profit_per_trade": data.get("pnl", {}).get("avg_profit_per_trade_usd", 0),
            "total_tokens_bought": data.get("quantity", {}).get("total_bought_amount", 0),
            "total_tokens_sold": data.get("quantity", {}).get("total_sold_amount", 0),
            "avg_buy_cost": data.get("pricing", {}).get("avg_buy_cost", 0),
            "avg_sell_cost": data.get("pricing", {}).get("avg_sell_cost", 0),
        })

    df_recent_pnls = pd.DataFrame(parsed_pnls)
    
    
    
    
    tokens_meta = app_data.get_state("wa_tokens_meta")
    if not tokens_meta:
        tokens_meta = {}
        with st.spinner("Loading token metadata (This may take a while)..."):
            mint_addresses = df_recent_pnls['mint_address'].unique().tolist()
            tokens_meta = solana._dexscreener_get_tokens_meta(mint_addresses)
            app_data.set_state("wa_tokens_meta", tokens_meta, CACHE_TTL)

    if not tokens_meta:
        st.info("No token metadata found.")
        return
    
    columns = []
    for token in tokens_meta.values():
        if token:
            columns.extend(token.keys())
            break
    columns = list(set(columns))

    socials_df = pd.DataFrame.from_dict(tokens_meta, orient="index")
    socials_df = socials_df.reindex(columns=columns)
    socials_df.index.name = "mint_address"
    socials_df.drop(columns=["name", "symbol", "decimals"], inplace=True, errors='ignore')

    # merge the social_df with the analysis DataFrame
    df_recent_pnls_merged = df_recent_pnls.merge(socials_df, on="mint_address", how="left")
    
    
    
    # Add developer info (creator)
    tokens_developers = app_data.get_state("wa_tokens_developers")
    if not tokens_developers:
        mint_addresses = df_recent_pnls_merged["mint_address"].unique().tolist()
        tokens_security_info = solana._birdeye_get_tokens_security(mint_addresses)
        for mint_address, security_info in tokens_security_info.items():
            creator = security_info.get("creatorAddress", "")
            df_recent_pnls_merged.loc[df_recent_pnls_merged["mint_address"] == mint_address, "developer_address"] = creator

    #---------------------------------------
    # Output the Analysis Results
    #---------------------------------------
    
    # Coins Analyzed
    
    st.write(f"#### {len(df_recent_pnls_merged)} Coins Analyzed:")
    st.dataframe(df_recent_pnls_merged, use_container_width=True)
    
    # Profit and Loss
    st.metric("Total PnL", f"${df_recent_pnls_merged['net_profit_usd'].sum():,.2f}")

    #---------------------------------------
    # Social Analysis
    #---------------------------------------

    # --- Social Presence
    
    # Always ensure required social columns exist
    required_cols_fix = ['discord', 'twitter', 'instagram', 'facebook']
    for col in required_cols_fix:
        if col not in df_recent_pnls_merged.columns:
            df_recent_pnls_merged[col] = ""

    # Assuming socials_df is your DataFrame
    socials_to_analyze = list(socials_df.columns)
    socials_to_analyze = [c for c in df_recent_pnls_merged.columns if c not in ["website"] and c in required_cols_fix]

    # Total unique tokens
    total_coins = len(df_recent_pnls_merged)

    # Check if website is valid (not null, empty, or "NULL")
    df_recent_pnls_merged["has_website"] = df_recent_pnls_merged["website"].notna() & (df_recent_pnls_merged["website"].str.strip() != "") & (df_recent_pnls_merged["website"].str.upper() != "NULL")
    count_coins_with_websites = df_recent_pnls_merged["has_website"].sum()

    # Check if any social is valid per row
    socials_only = [s for s in socials_to_analyze if s != "website"]
    df_recent_pnls_merged["has_social"] = df_recent_pnls_merged[socials_only].apply(
        lambda row: any((str(v).strip() not in ["", "NULL", "None", "nan"]) for v in row), axis=1
    )
    count_coins_with_socials = df_recent_pnls_merged["has_social"].sum()

    # Count per social type
    count_socials = {
        social: ((df_recent_pnls_merged[social].notna()) & (df_recent_pnls_merged[social].astype(str).str.strip().isin(["", "NULL", "None", "nan"]) == False)).sum()
        for social in socials_to_analyze
    }
    
    st.write("#### Social Media Presence:")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Unique Tokens", total_coins)
    col2.metric("Total Tokens with Socials", count_coins_with_socials, delta=(f"{count_coins_with_socials / total_coins * 100:.2f}%"))
    col3.metric("Total Tokens with Websites", count_coins_with_websites, delta=(f"{count_coins_with_websites / total_coins * 100:.2f}%"))

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Twitter", count_socials["twitter"], delta=(f"{count_socials['twitter'] / total_coins * 100:.2f}%"))
    col2.metric("Discord", count_socials["discord"], delta=(f"{count_socials['discord'] / total_coins * 100:.2f}%"))
    col3.metric("Instagram", count_socials["instagram"], delta=(f"{count_socials['instagram'] / total_coins * 100:.2f}%"))
    col4.metric("Facebook", count_socials["facebook"], delta=(f"{count_socials['facebook'] / total_coins * 100:.2f}%"))
    
    
    # --- PnL vs Social
    
    st.write("#### PnL Analysis by Social Presence")

    # 1. Tokens with no social links (neither website nor socials)
    df_no_socials = df_recent_pnls_merged[~df_recent_pnls_merged["has_website"] & ~df_recent_pnls_merged["has_social"]]
    total_pnl_no_socials = df_no_socials['net_profit_usd'].sum()
    count_no_socials = len(df_no_socials)
    avg_pnl_no_socials = total_pnl_no_socials / count_no_socials if count_no_socials > 0 else 0

    # 2. Tokens with at least one social link (social or website)
    df_with_any_social = df_recent_pnls_merged[df_recent_pnls_merged["has_website"] | df_recent_pnls_merged["has_social"]]
    total_pnl_with_any_social = df_with_any_social['net_profit_usd'].sum()
    count_with_any_social = len(df_with_any_social)
    avg_pnl_with_any_social = total_pnl_with_any_social / count_with_any_social if count_with_any_social > 0 else 0

    # 3. Tokens with just a website (no other socials)
    df_website_only = df_recent_pnls_merged[df_recent_pnls_merged["has_website"] & ~df_recent_pnls_merged["has_social"]]
    total_pnl_website_only = df_website_only['net_profit_usd'].sum()
    count_website_only = len(df_website_only)
    avg_pnl_website_only = total_pnl_website_only / count_website_only if count_website_only > 0 else 0

    # 4. Tokens with a website and a Twitter link
    df_website_and_twitter = df_recent_pnls_merged[
        (df_recent_pnls_merged["has_website"]) & 
        (df_recent_pnls_merged["twitter"].notna() & (df_recent_pnls_merged["twitter"].str.strip() != "") & (df_recent_pnls_merged["twitter"].str.upper() != "NULL"))
    ]
    total_pnl_website_and_twitter = df_website_and_twitter['net_profit_usd'].sum()
    count_website_and_twitter = len(df_website_and_twitter)
    avg_pnl_website_and_twitter = total_pnl_website_and_twitter / count_website_and_twitter if count_website_and_twitter > 0 else 0

    # 5. Tokens with only a Twitter link (no website)
    # This requires checking that the website column is empty/invalid AND the twitter column is valid.
    df_twitter_only = df_recent_pnls_merged[
        ~df_recent_pnls_merged["has_website"] &
        (df_recent_pnls_merged["twitter"].notna() & (df_recent_pnls_merged["twitter"].str.strip() != "") & (df_recent_pnls_merged["twitter"].str.upper() != "NULL")) &
        df_recent_pnls_merged[[s for s in socials_only if s != 'twitter']].apply(
            lambda row: not any((str(v).strip() not in ["", "NULL", "None", "nan"]) for v in row), axis=1
        )
    ]
    total_pnl_twitter_only = df_twitter_only['net_profit_usd'].sum()
    count_twitter_only = len(df_twitter_only)
    avg_pnl_twitter_only = total_pnl_twitter_only / count_twitter_only if count_twitter_only > 0 else 0
    
    # ---- Output in Streamlit ----

    col1, col2 = st.columns(2)
    col1.metric("Avg. PnL (No Socials)", f"{avg_pnl_no_socials:.2f} USD")
    col2.metric("Avg. PnL (With Socials/Website)", f"{avg_pnl_with_any_social:.2f} USD")

    col1, col2 = st.columns(2)
    col1.metric("Avg. PnL (Website Only)", f"{avg_pnl_website_only:.2f} USD")
    col2.metric("Avg. PnL (Website + Twitter)", f"{avg_pnl_website_and_twitter:.2f} USD")

    col1, col2 = st.columns(2)
    col1.metric("Avg. PnL (Twitter Only)", f"{avg_pnl_twitter_only:.2f} USD")


# --------------------------
# INIT
# --------------------------
if __name__ == "__main__":
    Page()
