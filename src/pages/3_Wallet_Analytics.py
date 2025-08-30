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
    MAX_TRADES_TOKENS = 400

    app_data = AppData()
    solana = SolanaTokenSummary()

    # Set page title
    st.set_page_config(page_title="Wallet Analytics", page_icon="💰", layout="wide")

    st.title("💰 Wallet Analytics") 

    wallet_overview = None
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
    
    requested_at = wallet_overview.get("requested_timestamp", "N/A")
    if requested_at != "N/A":
        requested_at = Utils.formatted_date(requested_at, format="%Y-%m-%d %H:%M:%S") 
    
    col1, col2 = st.columns(2)
    col1.metric(f"Net Worth", f"$ {wallet_overview.get('net_worth', 0):,.2f}")
    col2.metric("Requested At", requested_at)
    
    #---------------------------------------
    # Prepare Data for Analysis
    #---------------------------------------
    
    recent_tx = app_data.get_state(f"wa_wallet_tx_{MAX_TRADES_TOKENS}")
    recent_tx = None
    if not recent_tx:
        with st.spinner("Loading recent transactions (This may take a while)..."):
            recent_tx = solana._birdeye_get_wallet_trades(wallet_address, max_trades=MAX_TRADES_TOKENS)
            app_data.set_state(f"wa_wallet_tx_{MAX_TRADES_TOKENS}", recent_tx, CACHE_TTL)
    
    if not recent_tx or len(recent_tx) == 0:
        st.info("No recent transactions found for this wallet.")
        return
    recent_tx_df = pd.DataFrame(recent_tx)
    
    
    recent_traded_tokens = app_data.get_state(f"wa_wallet_traded_tokens_{MAX_TRADES_TOKENS}")
    if not recent_traded_tokens:
        with st.spinner("Loading recent traded tokens (This may take a while)..."):
            recent_traded_tokens = solana._birdeye_get_wallet_traded_tokens(wallet_address, max_trades=MAX_TRADES_TOKENS)
            app_data.set_state(f"wa_wallet_traded_tokens_{MAX_TRADES_TOKENS}", recent_traded_tokens, CACHE_TTL)

    if not recent_traded_tokens or len(recent_traded_tokens) == 0:
        st.info("No recent traded tokens found for this wallet.")
        return
    

    recent_pnls = app_data.get_state(f"wa_wallet_pnls_{MAX_TRADES_TOKENS}")
    if recent_pnls is None:
        with st.spinner("Retrieving PnL for recent transactions (This may take a while)..."):
            recent_pnls = solana._birdeye_get_wallet_tokens_pnl(wallet_address, recent_traded_tokens)
            app_data.set_state(f"wa_wallet_pnls_{MAX_TRADES_TOKENS}", recent_pnls, CACHE_TTL)

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
            "symbol": token.get("symbol", ""),
            "net_profit_usd": data.get("pnl", {}).get("total_usd", 0),
            "net_profit_percent": data.get("pnl", {}).get("total_percent", 0),
            "avg_profit_per_trade": data.get("pnl", {}).get("avg_profit_per_trade_usd", 0),
            "total_tokens_bought": data.get("quantity", {}).get("total_bought_amount", 0),
            "total_tokens_sold": data.get("quantity", {}).get("total_sold_amount", 0),
            "avg_buy_cost": data.get("pricing", {}).get("avg_buy_cost", 0),
            "avg_sell_cost": data.get("pricing", {}).get("avg_sell_cost", 0),
        })

    df_recent_pnls = pd.DataFrame(parsed_pnls)
    
    
    
    
    tokens_meta = app_data.get_state(f"wa_tokens_meta_{MAX_TRADES_TOKENS}")
    if not tokens_meta:
        tokens_meta = {}
        with st.spinner("Loading token metadata (This may take a while)..."):
            mint_addresses = df_recent_pnls['mint_address'].unique().tolist()
            tokens_meta = solana._dexscreener_get_tokens_meta(mint_addresses)
            app_data.set_state(f"wa_tokens_meta_{MAX_TRADES_TOKENS}", tokens_meta, CACHE_TTL)

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
    df_analysis = df_recent_pnls.merge(socials_df, on="mint_address", how="left")
    
    
    
    # Add developer info (creator)
    mint_addresses = df_analysis["mint_address"].unique().tolist()

    tokens_security_info = app_data.get_state(f"wa_tokens_security_info_{MAX_TRADES_TOKENS}")
    if not tokens_security_info:
        with st.spinner("Loading token security information (This may take a while)..."):
            tokens_security_info = solana._birdeye_get_tokens_security(mint_addresses)

    for mint_address, security_info in tokens_security_info.items():
        creator = security_info.get("creatorAddress", "")
        df_analysis.loc[df_analysis["mint_address"] == mint_address, "developer_address"] = creator
    
    # Add created pools info
    developer_created_pools = app_data.get_state(f"wa_developer_created_pools_{MAX_TRADES_TOKENS}")
    if not developer_created_pools:
        with st.spinner("Loading wallets pools (This may take a while)..."):
            developer_created_pools = solana._solscan_get_wallets_created_pools(df_analysis['developer_address'].dropna().unique().tolist())
    
    for developer, pools in developer_created_pools.items():
        df_analysis.loc[df_analysis["developer_address"] == developer, "developer_total_tokens_created"] = len(pools)

    #---------------------------------------
    # Output the Analysis Results
    #---------------------------------------
    
    # Coins Analyzed
    
    st.write(f"#### {len(df_analysis)} Unique Coins Analyzed:")
    st.dataframe(df_analysis, use_container_width=True)
    
    # Profit and Loss
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Trades Analyzed", f"{len(recent_tx)}")
    col2.metric("Coins Analyzed", f"{len(df_analysis)}")
    col3.metric("Total PnL", f"${df_analysis['net_profit_usd'].sum():,.2f}")
    
    col1, col2, col3, col4 = st.columns(4)
    best_coin_idx = df_analysis['net_profit_usd'].idxmax()
    worst_coin_idx = df_analysis['net_profit_usd'].idxmin()
    col1.metric("Most Profitable Coin", str(df_analysis.loc[best_coin_idx, 'symbol']), delta=f"{df_analysis.loc[best_coin_idx, 'net_profit_usd']:,.2f} $")
    col2.metric("Most Unprofitable Coin", str(df_analysis.loc[worst_coin_idx, 'symbol']), delta=f"{df_analysis.loc[worst_coin_idx, 'net_profit_usd']:,.2f} $")
    col3.metric("Avg. PnL per Coin", f"${df_analysis['net_profit_usd'].mean():,.2f}")

    #---------------------------------------
    # Social Analysis
    #---------------------------------------
    st.write("---")

    # --- Social Presence
    
    # Always ensure required social columns exist
    required_cols_fix = ['discord', 'twitter', 'instagram', 'facebook', 'telegram']
    for col in required_cols_fix:
        if col not in df_analysis.columns:
            df_analysis[col] = ""

    # Assuming socials_df is your DataFrame
    socials_to_analyze = list(socials_df.columns)
    socials_to_analyze = [c for c in df_analysis.columns if c not in ["website"] and c in required_cols_fix]

    # Total unique tokens
    total_coins = len(df_analysis)

    # Check if website is valid (not null, empty, or "NULL")
    df_analysis["has_website"] = df_analysis["website"].notna() & (df_analysis["website"].str.strip() != "") & (df_analysis["website"].str.upper() != "NULL")
    count_coins_with_websites = df_analysis["has_website"].sum()

    # Check if any social is valid per row
    socials_only = [s for s in socials_to_analyze if s != "website"]
    df_analysis["has_social"] = df_analysis[socials_only].apply(
        lambda row: any((str(v).strip() not in ["", "NULL", "None", "nan"]) for v in row), axis=1
    )
    count_coins_with_socials = df_analysis["has_social"].sum()

    # Count per social type
    count_socials = {
        social: ((df_analysis[social].notna()) & (df_analysis[social].astype(str).str.strip().isin(["", "NULL", "None", "nan"]) == False)).sum()
        for social in socials_to_analyze
    }
    
    st.write("#### Social Media Presence:")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Tokens with Socials", count_coins_with_socials, delta=(f"{count_coins_with_socials / total_coins * 100:.2f}%"))
    col2.metric("Total Tokens with Websites", count_coins_with_websites, delta=(f"{count_coins_with_websites / total_coins * 100:.2f}%"))

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Twitter", count_socials["twitter"], delta=(f"{count_socials['twitter'] / total_coins * 100:.2f}%"))
    col2.metric("Discord", count_socials["discord"], delta=(f"{count_socials['discord'] / total_coins * 100:.2f}%"))
    col3.metric("Instagram", count_socials["instagram"], delta=(f"{count_socials['instagram'] / total_coins * 100:.2f}%"))
    col4.metric("Facebook", count_socials["facebook"], delta=(f"{count_socials['facebook'] / total_coins * 100:.2f}%"))
    col1.metric("Telegram", count_socials["telegram"], delta=(f"{count_socials['telegram'] / total_coins * 100:.2f}%"))


    # --- PnL vs Social
    
    st.write("#### PnL Analysis by Social Presence")

    # 1. Tokens with no social links (neither website nor socials)
    df_no_socials = df_analysis[~df_analysis["has_website"] & ~df_analysis["has_social"]]
    total_pnl_no_socials = df_no_socials['net_profit_usd'].sum()
    count_no_socials = len(df_no_socials)
    avg_pnl_no_socials = total_pnl_no_socials / count_no_socials if count_no_socials > 0 else 0

    # 2. Tokens with at least one social link (social or website)
    df_with_any_social = df_analysis[df_analysis["has_website"] | df_analysis["has_social"]]
    total_pnl_with_any_social = df_with_any_social['net_profit_usd'].sum()
    count_with_any_social = len(df_with_any_social)
    avg_pnl_with_any_social = total_pnl_with_any_social / count_with_any_social if count_with_any_social > 0 else 0

    # 3. Tokens with just a website (no other socials)
    df_website_only = df_analysis[df_analysis["has_website"] & ~df_analysis["has_social"]]
    total_pnl_website_only = df_website_only['net_profit_usd'].sum()
    count_website_only = len(df_website_only)
    avg_pnl_website_only = total_pnl_website_only / count_website_only if count_website_only > 0 else 0

    # 4. Tokens with a website and a Twitter link
    df_website_and_twitter = df_analysis[
        (df_analysis["has_website"]) & 
        (df_analysis["twitter"].notna() & (df_analysis["twitter"].str.strip() != "") & (df_analysis["twitter"].str.upper() != "NULL"))
    ]
    total_pnl_website_and_twitter = df_website_and_twitter['net_profit_usd'].sum()
    count_website_and_twitter = len(df_website_and_twitter)
    avg_pnl_website_and_twitter = total_pnl_website_and_twitter / count_website_and_twitter if count_website_and_twitter > 0 else 0

    # 5. Tokens with only a Twitter link (no website)
    # This requires checking that the website column is empty/invalid AND the twitter column is valid.
    df_twitter_only = df_analysis[
        ~df_analysis["has_website"] &
        (df_analysis["twitter"].notna() & (df_analysis["twitter"].str.strip() != "") & (df_analysis["twitter"].str.upper() != "NULL")) &
        df_analysis[[s for s in socials_only if s != 'twitter']].apply(
            lambda row: not any((str(v).strip() not in ["", "NULL", "None", "nan"]) for v in row), axis=1
        )
    ]
    total_pnl_twitter_only = df_twitter_only['net_profit_usd'].sum()
    count_twitter_only = len(df_twitter_only)
    avg_pnl_twitter_only = total_pnl_twitter_only / count_twitter_only if count_twitter_only > 0 else 0
    
    # ---- Output in Streamlit ----

    col1, col2 = st.columns(2)
    col1.metric("Avg. PnL (No Socials)", f"$ {avg_pnl_no_socials:.2f}")
    col2.metric("Avg. PnL (With Socials/Website)", f"$ {avg_pnl_with_any_social:.2f}")

    col1, col2 = st.columns(2)
    col1.metric("Avg. PnL (Website Only)", f"$ {avg_pnl_website_only:.2f}")
    col2.metric("Avg. PnL (Website + Twitter)", f"$ {avg_pnl_website_and_twitter:.2f} ")

    col1, col2 = st.columns(2)
    col1.metric("Avg. PnL (Twitter Only)", f"$ {avg_pnl_twitter_only:.2f}")


    #---------------------------------------
    # Dev Influence
    #---------------------------------------
    st.write("---")

    df_dev_analysis_dev = df_analysis[df_analysis["developer_total_tokens_created"].notna()]
    df_dev_analysis_dev_1 = df_dev_analysis_dev[df_dev_analysis_dev["developer_total_tokens_created"] == 1]
    df_dev_analysis_dev_up_to_5 = df_dev_analysis_dev[df_dev_analysis_dev["developer_total_tokens_created"] <= 5]
    df_dev_analysis_dev_5_to_10 = df_dev_analysis_dev[(
        df_dev_analysis_dev["developer_total_tokens_created"] > 5)
        & (df_dev_analysis_dev["developer_total_tokens_created"] <= 10)]
    df_dev_analysis_dev_above_10 = df_dev_analysis_dev[df_dev_analysis_dev["developer_total_tokens_created"] > 10]
    count_rows_without_dev = df_analysis[df_analysis["developer_total_tokens_created"].isna()].shape[0]

    # --- Dev Proficiency

    st.write("#### Developer Proficiency")
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Unique Developers", df_dev_analysis_dev.shape[0])
    col2.metric("Tokens without Dev (IGNORED)", count_rows_without_dev)
    col3.metric("Avg. Tokens per Dev", f"{df_dev_analysis_dev['developer_total_tokens_created'].mean():.2f}")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Devs with 1 Token", df_dev_analysis_dev_1.shape[0])
    col2.metric("Devs with 1-5 Tokens", df_dev_analysis_dev_up_to_5.shape[0])
    col3.metric("Devs with 5-10 Tokens", df_dev_analysis_dev_5_to_10.shape[0])
    col4.metric("Devs with 10+ Tokens", df_dev_analysis_dev_above_10.shape[0])

    # --- PnL by Dev Proficiency

    st.write("#### PnL Analysis by Developer Proficiency")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Avg. PnL (when Dev 1 Token)", f"$ {df_dev_analysis_dev_1['net_profit_usd'].mean():,.2f}")
    col2.metric("Avg. PnL (when Dev 1-5 Tokens)", f"$ {df_dev_analysis_dev_up_to_5['net_profit_usd'].mean():,.2f}")
    col3.metric("Avg. PnL (when Dev 5-10 Tokens)", f"$ {df_dev_analysis_dev_5_to_10['net_profit_usd'].mean():,.2f}")
    col4.metric("Avg. PnL (when Dev 10+ Tokens)", f"$ {df_dev_analysis_dev_above_10['net_profit_usd'].mean():,.2f}")

# --------------------------
# INIT
# --------------------------
if __name__ == "__main__":
    Page()
