# Gold Layer Schema Design

**Layer Purpose:** Metric Calculation, Aggregation, and Analysis Readiness.  
**Table Schema:** `gold`  
**Table Name:** `final_results`

The Gold layer reads the Silver layer’s clean data and computes the specific metrics required for regression analysis.  
Raw accounting fields (e.g., raw debt/cash) are dropped; only calculated indicators remain.

---

# Logic & Calculations

Transformations performed in `src/analyze_regression.py`:

1. **Market Cap** = StockPrice × SharesOutstanding
2. **Enterprise Value (EV)** = MarketCap + LongTermDebt + CurrentDebt − Cash
3. **EV % Change** = Δ(EV_post − EV_pre) / EV_pre
4. **Deal Size Ratio** = DealSize / MarketCap_pre

---

# Table Definition: `final_results`

Optimized for `statsmodels` or BI tools (Tableau, PowerBI).  
Rows with null critical metrics are removed.


## Columns

| Column | Type | Description |
|--------|-------|-------------|
| dealid | Int | Unique deal ID |
| acquirer_ticker | String | Ticker symbol |
| primaryindustrysector | String | Used for fixed effects in regression |
| dealsize | Float | Transaction value (USD) |


## Calculated Metrics

| Column | Type | Description |
|--------|-------|-------------|
| ev_pre | Float | Enterprise Value before the deal |
| ev_post | Float | Enterprise Value after the deal |
| market_cap_pre | Float | Market Cap before the deal |
| market_cap_post | Float | Market Cap before the deal |
| ebitda_pre | Float | EBITDA before the deal |
| ebitda_post | Float | EBITDA after the deal |


## Regression Variables

| Column | Type | Explanation |
|--------|-------|-------------|
| delta_ev_pct | Float | **Y1:** % growth in enterprise value |
| delta_mkt_cap_pct | Float | **Y2:** % growth in market cap |
| delta_ebitda_pct | Float | **Y3:** % growth in EBITDA |
| deal_size_ratio | Float | **X1:** Deal size / pre-deal market cap |
