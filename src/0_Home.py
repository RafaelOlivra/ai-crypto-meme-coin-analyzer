import streamlit as st
from dotenv import load_dotenv, find_dotenv

# --------------------------
# Configurations
# ---------------------------

# Load environment variables
load_dotenv(find_dotenv("../.env"))

# --------------------------
# Page
# ---------------------------
def Home():
    
    # Set page title
    st.set_page_config(page_title="Home", page_icon="🐸", layout="wide")
    
    st.title("🐸 Meme Coin Tools")
    
    col_1, col_2 = st.columns([1, 1])
    
    with col_1:
        with st.container(border=True):
            st.write(
                """
                ### 🐸 Meme Coin Analytics
                Analyze the latest meme coins on Solana and get insights on their performance.
            """
            )
            open_meme_coin_analytics = st.button(
                "Open Meme Coin Analytics", use_container_width=True, key="open_meme_coin_analytics"
            )
            if open_meme_coin_analytics:
                st.switch_page("pages/1_Meme_Coin_Analytics.py")


# --------------------------
# INIT
# --------------------------
if __name__ == "__main__":
    Home()
