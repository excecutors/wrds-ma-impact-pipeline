# Silver Layer Schema Design

**Layer Purpose:** Data Cleaning, Denormalization, and Temporal Alignment.  
**Table Schema:** `silver`  
**Table Name:** `deal_financials_linked`  
**Transformation Engine:** Polars (Python)

In this layer, we transform raw data into a “Business Level” dataset.  
We perform the following key operations:

1. **Filtering:** Ensure all acquirers are North American public companies and deal sizes are valid (not NULL).
2. **Renaming:** Convert cryptic Compustat codes (e.g., `dlttq`) into readable business terms (e.g., `long_term_debt`).
3. **Structural Join:** Link deals to acquirer industry using PitchBook IDs.
4. **Temporal Join (Time-Travel):** Use “as-of joins” to match each deal with financial reports immediately preceding (**t−1**) and following (**t+1**) the announcement date.

---


---

# Table Definition: `deal_financials_linked`

A wide, denormalized table where each row represents one unique deal enriched with the acquirer’s financial state before and after the transaction.



## Identifiers

| Column | Source | Description |
|--------|--------|-------------|
| dealid | ot_glb_deal.dealid | Unique Deal ID |
| acquirer_ticker | ot_glb_company.ticker | Ticker symbol used for matching financials |
| target_name | ot_glb_deal.companyname | Name of target company |
| acquirer_name | ot_glb_company.companyname | Name of acquiring company |
| primaryindustrysector | ot_glb_company.primaryindustrysector | Industry sector of the acquirer |



## Deal Info

| Column | Source | Description |
|--------|--------|-------------|
| announceddate | ot_glb_deal.announceddate | Date the deal was announced |
| dealstatus | ot_glb_deal.dealstatus | Status (filtered to "Completed") |
| dealsize | ot_glb_deal.dealsize | Transaction value (USD) |



## Pre-Deal Financials (t−1)

Derived from `fundq` — closest report *before* announcement.

| Column | Source | Description |
|--------|--------|-------------|
| period_end_pre | apdedateq | Fiscal period-end date |
| stock_price_pre | prccq | Stock price at quarter end |
| shares_outstanding_pre | cshoq | Common shares outstanding |
| long_term_debt_pre | dlttq | Long-term debt |
| current_debt_pre | dlcq | Current liabilities debt |
| cash_pre | cheq | Cash & short-term investments |
| ebitda_pre | oibdpq | Operating income before depreciation |



## Post-Deal Financials (t+1)

Derived from `fundq` — closest report *after* announcement.

| Column | Source | Description |
|--------|--------|-------------|
| period_end_post | apdedateq | Fiscal period-end date |
| stock_price_post | prccq | Stock price at quarter end |
| shares_outstanding_post | cshoq | Common shares outstanding |
| long_term_debt_post | dlttq | Long-term debt |
| current_debt_post | dlcq | Current liabilities debt |
| cash_post | cheq | Cash & short-term investments |


