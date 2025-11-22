# Load libraries and point to the Gold parquet
# ------------------------------------------------------

import streamlit as st
import pandas as pd
import os

DATA_PATH = "data/gold_data.parquet"


# Set up the page layout and dashboard title
# ------------------------------------------------------

st.set_page_config(page_title="M&A Impact Dashboard", layout="wide")
st.title("M&A Impact Dashboard – Prototype")

# Cached function to load the data once (“Load the Gold data from disk only once. If the user refreshes or clicks filters, don’t reload the file.”)
# ------------------------------------------------------

@st.cache_data
def load_data(path):
    df = pd.read_parquet(path)

    # Map Gold layer columns → Dashboard columns
    # ------------------------------------------------------

    if "primaryindustrysector" in df.columns and "industry" not in df.columns:
        df = df.rename(columns={"primaryindustrysector": "industry"})

    # If the Gold dataset has EBITDA growth (delta_ebitda_pct) but does NOT have margin growth (delta_margin_pct), then use EBITDA growth as the stand-in for margin
    # This statement is always true for now
    # Since our Gold dataset does not include revenue, we cannot compute true margins (EBITDA_post/Revenue_post - EBITDA_pre/Revenue_pre)
    # Delta_ebitda_pct = (EBITDA_post - EBITDA_pre) / EBITDA_pre
    # ------------------------------------------------------

    if "delta_ebitda_pct" in df.columns and "delta_margin_pct" not in df.columns:
        df["delta_margin_pct"] = df["delta_ebitda_pct"]

    return df

df = load_data(DATA_PATH)

# Stop the app if industry is missing (just a protection so user don't see broken charts)
# ------------------------------------------------------

if "industry" not in df.columns:
    st.error("`industry` column not found in data. Check column mapping in app.py.")
    st.stop()

# Builds the left sidebar UI, shows a multi-select dropdown containing all industries, default, it selects all industries
# Also another sidebar filter to filter by company
# ------------------------------------------------------

industries = df["industry"].dropna().unique().tolist()
selected_industries = st.sidebar.multiselect("Industry filter:", industries, industries)

# companies = df["acquirer_ticker"].dropna().unique().tolist()
# selected_companies = st.sidebar.multiselect("Company filter:", companies, companies)

# Filter the Gold dataset based on user selections (for selected industry & company)
# ------------------------------------------------------

df_filt = df[df["industry"].isin(selected_industries)]
# df_filt = df_filt[df_filt["acquirer_ticker"].isin(selected_companies)]

# Compute Headline KPIs (formula in gold_layer.py)

# Avg ΔEV% (is the Average Enterprise Value Growth (%)): This shows how much the acquirers’ Enterprise Value changed after the acquisition
    # Answers: EV = Market Cap + Debt - Cash
    # from gold layer: ((ev_post - ev_pre) / ev_pre).alias("delta_ev_pct")

# Avg ΔMargin% (AKA Average EBITDA Margin Growth (%) as said before): this approximates how the acquirer’s profitability changed after the deal
    # Answers: Did the acquirer’s profitability improve post-acquisition?
    # from gold layer: ((ebitda_post - ebitda_pre) / ebitda_pre).alias("delta_ebitda_pct")

# Deals: Count of Deals Currently Filtered (like user can filter tech-only, deals after 2010, high deal size ratio, etc)
# ------------------------------------------------------

col1, col2, col3 = st.columns(3)
col1.metric("Avg ΔEV%", f"{df_filt['delta_ev_pct'].mean():.2%}")
col2.metric("Avg ΔMargin%", f"{df_filt['delta_margin_pct'].mean():.2%}")
col3.metric("Deals", len(df_filt))

# Chart 1: EV Growth % by industry (bar chart with x-axis of industry, y-axis of avg EV%)
# ------------------------------------------------------

st.subheader("ΔEV% by Industry")
st.bar_chart(df_filt.groupby("industry")["delta_ev_pct"].mean())

# Chart 2: Deal Size Ratio vs EV Growth (scatter input)
# ------------------------------------------------------

st.subheader("ΔEV% vs Deal Size Ratio")
scatter_df = df_filt[["deal_size_ratio", "delta_ev_pct"]].rename(
    columns={"deal_size_ratio": "x", "delta_ev_pct": "y"}
)
st.scatter_chart(scatter_df)