import streamlit as st

from streamlit_extras.switch_page_button import switch_page
from dotenv import load_dotenv, find_dotenv

from services.AppData import AppData

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
    st.logo(f"{assets_dir}logo.svg", size="large")

    # Set page title
    st.set_page_config(page_title="Home", page_icon="🏠", layout="wide")

    # Styles
    with open(f"{assets_dir}style.css") as css:
        st.markdown(f"<style>{css.read()}</style>", unsafe_allow_html=True)

    st.title("🏠 Home")
    # st.image(f"{assets_dir}logo.svg", width=350)
    st.write(
        """
        Page Description
        """
    )

    col1, col2 = st.columns(2)
    with col1:
        with st.container(border=True):
            st.write(
                """
                ### 🏠 BOX 1
                Description
            """
            )

    with col2:
        with st.container(border=True):
            st.write(
                """
                ### ✏️ BOX 2
                Description
            """
            )


# --------------------------
# INIT
# --------------------------
if __name__ == "__main__":
    Home()
