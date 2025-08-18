import streamlit as st
from dotenv import load_dotenv, find_dotenv

from services.AppData import AppData
from services.BitQuerySolana import BitQuerySolana
from services.SolanaTokenSummary import SolanaTokenSummary

# --------------------------
# Configurations
# ---------------------------

# Load environment variables
load_dotenv(find_dotenv("../.env"))


# --------------------------
# Page
# ---------------------------
def Home():

    # Set logo
    assets_dir = AppData().get_assets_dir()
    # st.logo(f"{assets_dir}logo.svg", size="large")

    # Set page title
    st.set_page_config(page_title="Home", page_icon="🐸", layout="wide")

    # Styles
    with open(f"{assets_dir}style.css") as css:
        st.markdown(f"<style>{css.read()}</style>", unsafe_allow_html=True)

    st.title("🐸 Meme Coin Analyzer")
    
    bitquery = BitQuerySolana()
    solana = SolanaTokenSummary()
    
    addresses = {
        "BILLY": {
            "mint": "3B5wuUrMEi5yATD7on46hKfej3pfmd7t1RKgrsN3pump",
            "pair": "9uWW4C36HiCTGr6pZW9VFhr9vdXktZ8NA8jVnzQU35pJ"
        },
        "RIZZMASTER": {
            "mint": "2BpJpCW9LfSPbgsgNKcFoxz96Cs8J8NZsi6PZJcHpump",
            "pair": "Af5op3qJJ87sU4GJqjKX9isJEJGHkzHvxRQRoWmPCfxw"
        }
    }

    token_selector = st.selectbox("Select a token", options=list(addresses.keys()))

    addresses = {
        "BILLY": {
            "mint": "3B5wuUrMEi5yATD7on46hKfej3pfmd7t1RKgrsN3pump",
            "pair": "9uWW4C36HiCTGr6pZW9VFhr9vdXktZ8NA8jVnzQU35pJ"
        },
        "RIZZMASTER": {
            "mint": "2BpJpCW9LfSPbgsgNKcFoxz96Cs8J8NZsi6PZJcHpump",
            "pair": "Af5op3qJJ87sU4GJqjKX9isJEJGHkzHvxRQRoWmPCfxw"
        }
    }

    token = addresses[token_selector]["mint"]
    pair_address = addresses[token_selector]["pair"]

    st.markdown("### Token Summary (BirdEye+Dexscreener)")
    df_status = solana.get_token_summary_df(token, pair_address)
    st.dataframe(df_status.T.rename_axis("Agg Token Summary"), use_container_width=True)

    st.markdown("### Token Summary (BitQuery)")
    df_summary = bitquery.get_gmgn_token_pair_summary_df(token, pair_address)
    st.dataframe(df_summary.T.rename_axis("BitQuery Summary"), use_container_width=True)
    
    st.markdown("### Token Summary (DexScreener)")
    

    st.markdown("### Recent Trades  (BitQuery)")
    st.dataframe(bitquery.get_gmgn_recent_token_pair_trades_df(token, pair_address), use_container_width=True)

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
