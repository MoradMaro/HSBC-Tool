# config.py
import streamlit as st

def init_page(title="HSBC"):
    st.set_page_config(page_title=title, page_icon="🏦", layout="wide", initial_sidebar_state="auto")
    # hide_streamlit_style = """
    #     <style>
    #     #MainMenu {visibility: hidden;}
    #     footer {visibility: hidden;}
    #     header {visibility: hidden;}
    #     </style>
    # """
    
    hide_streamlit_style = """
        <style>
        /* إخفاء Menu (فيه Deploy & Settings) */
        #MainMenu {visibility: hidden;}

        /* إخفاء الفوتر */
        footer {visibility: hidden;}
        
        </style>
        """

    st.markdown(hide_streamlit_style, unsafe_allow_html=True)
    
    # hide_streamlit_style = """
    #     <style>
    #     /* إخفاء زر Deploy */
    #     button[data-testid="stDeployButton"] {
    #         display: none;
    #     }

    #     /* إخفاء Settings / Menu */
    #     #MainMenu {
    #         visibility: hidden;
    #     }

    #     /* إخفاء الفوتر */
    #     footer {
    #         visibility: hidden;
    #     }
    #     </style>
    # """
    # st.markdown(hide_streamlit_style, unsafe_allow_html=True)