# Load libraries and point to the Gold parquet
# ------------------------------------------------------

import streamlit as st
import pandas as pd
import altair as alt
import numpy as np
import os

DATA_PATH = "data/gold_data.parquet"


# Set up the page layout and dashboard title
# ------------------------------------------------------

st.set_page_config(page_title="M&A Impact Dashboard", layout="wide")
st.title("M&A Impact Dashboard")

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

# keep the most realistic 99% of values (SHORT TERM FIX)

q99 = df["deal_size_ratio"].quantile(0.99)
df = df[df["deal_size_ratio"] <= q99]


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

### companies = df["acquirer_ticker"].dropna().unique().tolist()
### selected_companies = st.sidebar.multiselect("Company filter:", companies, companies)

# Filter the Gold dataset based on user selections (for selected industry & company)
# ------------------------------------------------------

df_filt = df[df["industry"].isin(selected_industries)]
### df_filt = df_filt[df_filt["acquirer_ticker"].isin(selected_companies)]

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

# Chart 1a: EV Growth % by industry (bar chart with x-axis of industry, y-axis of avg EV%)
# ------------------------------------------------------

st.subheader("ΔEV% by Industry")
st.bar_chart(df_filt.groupby("industry")["delta_ev_pct"].mean())

# Chart 1b: ΔMarket Cap% by Industry
st.subheader("ΔMarket Cap% by Industry")
st.bar_chart(df_filt.groupby("industry")["delta_mkt_cap_pct"].mean())


# Chart 1c: ΔEBITDA% by Industry
st.subheader("ΔEBITDA% by Industry")
st.bar_chart(df_filt.groupby("industry")["delta_ebitda_pct"].mean())

# Chart 2: Deal Size Ratio vs EV Growth (scatter input) -- sample the scatter if large

# X-axis: Deal Size Ratio (AKA How big the acquisition was relative to the acquirer’s own size)
#  deal_size_ratio = deal_size / market_cap_pre

# Y-axis: ΔEV% (Enterprise Value % change) AKA How much the acquirer’s enterprise value increased or decreased after the deal
# delta_ev_pct = (EV_post – EV_pre) / EV_pre
# ------------------------------------------------------

# st.subheader("ΔEV% vs Deal Size Ratio")
# scatter_df = df_filt[["deal_size_ratio", "delta_ev_pct"]].rename(
#     columns={"deal_size_ratio": "x", "delta_ev_pct": "y"}
# )
# st.scatter_chart(scatter_df)

st.subheader("ΔEV% vs Deal Size Ratio")

max_points = 2000
scatter_source = (
    df_filt.sample(max_points, random_state=42)
    if len(df_filt) > max_points
    else df_filt
)

# Fit a linear regression line
X = scatter_source["deal_size_ratio"].values
Y = scatter_source["delta_ev_pct"].values

beta1, beta0 = np.polyfit(X, Y, 1)

# regression line
x_line = np.linspace(X.min(), X.max(), 100)
y_line = beta0 + beta1 * x_line

line_df = pd.DataFrame({"deal_size_ratio": x_line, "delta_ev_pct": y_line})

points = (
    alt.Chart(scatter_source)
    .mark_circle(size=60, opacity=0.5)
    .encode(
        x=alt.X("deal_size_ratio", title="Deal Size Ratio"),
        y=alt.Y("delta_ev_pct", title="ΔEV%"),
        tooltip=["deal_size_ratio", "delta_ev_pct"],
    )
)

reg_line = (
    alt.Chart(line_df)
    .mark_line(color="red", size=1)  #
    .encode(x="deal_size_ratio", y="delta_ev_pct")
)

st.altair_chart(points + reg_line, use_container_width=True)

# Interpretation of the regression slope
if not np.isnan(beta1):
    interpretation_text = (
        f"The regression slope is **{beta1:.4f}**, indicating that for each 1-unit increase "
        f"in deal size ratio, the expected change in ΔEV% is **{beta1:.4f}** units, on average. "
        "This captures the linear association between acquisition size (relative to the acquirer) "
        "and post-deal enterprise value growth."
    )
else:
    interpretation_text = (
        "Not enough data to compute a regression under the current filters."
    )

st.markdown(interpretation_text)
