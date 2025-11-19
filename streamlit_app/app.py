import streamlit as st
import pandas as pd
import os

# Path to dummy or real data
DATA_PATH = "streamlit_app/dummy_data/final_results.parquet"

st.set_page_config(page_title="M&A Impact Dashboard", layout="wide")
st.title("M&A Impact Dashboard – Prototype")


# Load data
@st.cache_data
def load_data(path):
    return pd.read_parquet(path)


df = load_data(DATA_PATH)

# Sidebar filters
industries = df["industry"].unique().tolist()
selected_industries = st.sidebar.multiselect("Industry filter:", industries, industries)

df_filt = df[df["industry"].isin(selected_industries)]

# KPIs
col1, col2, col3 = st.columns(3)
col1.metric("Avg ΔEV%", f"{df_filt['delta_ev_pct'].mean():.2%}")
col2.metric("Avg ΔMargin%", f"{df_filt['delta_margin_pct'].mean():.2f}")
col3.metric("Deals", len(df_filt))

# Chart 1
st.subheader("ΔEV% by Industry")
st.bar_chart(df_filt.groupby("industry")["delta_ev_pct"].mean())

# Chart 2
st.subheader("ΔEV% vs Deal Size Ratio")
scatter_df = df_filt[["deal_size_ratio", "delta_ev_pct"]].rename(
    columns={"deal_size_ratio": "x", "delta_ev_pct": "y"}
)
st.scatter_chart(scatter_df)
