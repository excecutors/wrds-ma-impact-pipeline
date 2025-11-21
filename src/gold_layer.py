import os
import sys
import logging
import polars as pl
import pandas as pd

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.utils.db import get_postgres_engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def transform_gold():
    """
    Gold Layer Logic:
    1. Read from Silver Layer (clean, joined data).
    2. Compute financial metrics:
       - Market Cap (Pre & Post)
       - Enterprise Value (EV) (Pre & Post)
       - EBITDA Growth
    3. Compute Ratios & Deltas:
       - EV Growth %
       - Market Cap Growth %
       - Deal Size Ratio
    4. Filter NAs: Ensure completeness for regression.
    5. Output: A final, analysis-ready dataset (Parquet & DB).
    """
    engine = get_postgres_engine()


    # 1. Read Silver Data
    logging.info("Reading data from Silver layer...")
    
    q_silver = "SELECT * FROM silver.deal_financials_linked"
    df_silver = pl.read_database(q_silver, engine)
    
    logging.info(f"Loaded {df_silver.height} rows from silver.deal_financials_linked")


    # 2. Compute Core Metrics (Pre & Post)
    logging.info("Computing Core Metrics (Market Cap, EV)...")
    
    # EV Formula: Market Cap + Total Debt - Cash
    # Market Cap Formula: Price * Shares
    
    df_calc = df_silver.with_columns([
        # --- Pre-Deal ---
        (pl.col("stock_price_pre") * pl.col("shares_outstanding_pre")).alias("market_cap_pre"),
        ((pl.col("long_term_debt_pre").fill_null(0) + pl.col("current_debt_pre").fill_null(0))).alias("total_debt_pre"),
        
        # --- Post-Deal ---
        (pl.col("stock_price_post") * pl.col("shares_outstanding_post")).alias("market_cap_post"),
        ((pl.col("long_term_debt_post").fill_null(0) + pl.col("current_debt_post").fill_null(0))).alias("total_debt_post"),
    ])
    
    # Compute EV based on the new columns
    df_calc = df_calc.with_columns([
        (pl.col("market_cap_pre") + pl.col("total_debt_pre") - pl.col("cash_pre").fill_null(0)).alias("ev_pre"),
        (pl.col("market_cap_post") + pl.col("total_debt_post") - pl.col("cash_post").fill_null(0)).alias("ev_post")
    ])


    # 3. Compute Deltas & Ratios (The Regression Variables)
    logging.info("Computing Deltas (Growth Rates) & Ratios...")
    
    df_gold = df_calc.with_columns([
        # 1. EV Growth % (Target Variable)
        ((pl.col("ev_post") - pl.col("ev_pre")) / pl.col("ev_pre")).alias("delta_ev_pct"),
        
        # 2. Market Cap Growth % (Robustness Check)
        ((pl.col("market_cap_post") - pl.col("market_cap_pre")) / pl.col("market_cap_pre")).alias("delta_mkt_cap_pct"),
        
        # 3. EBITDA Growth % (Profitability Change)
        ((pl.col("ebitda_post") - pl.col("ebitda_pre")) / pl.col("ebitda_pre")).alias("delta_ebitda_pct"),
        
        # 4. Deal Size Ratio (Key Independent Variable)
        # How big was the deal relative to the acquirer?
        (pl.col("dealsize") / pl.col("market_cap_pre")).alias("deal_size_ratio")
    ])


    # 4. Filter NAs & Clean Data
    logging.info("Filtering NAs and invalid values...")
    
    initial_count = df_gold.height
    
    # Filter Logic:
    # - Must have valid EV (Pre/Post)
    # - Pre-deal EV cannot be 0 (division by zero risk)
    # - Must have valid growth rates (finite)
    # - Must have valid deal size ratio
    
    df_clean = df_gold.filter(
        pl.col("ev_pre").is_not_null() & (pl.col("ev_pre") != 0) &
        pl.col("ev_post").is_not_null() &
        pl.col("delta_ev_pct").is_finite() &
        pl.col("deal_size_ratio").is_not_null() &
        pl.col("delta_ebitda_pct").is_finite()
    )
    
    final_count = df_clean.height
    logging.info(f"Data Cleaning: {initial_count} -> {final_count} rows (Dropped {initial_count - final_count})")


    # 5. Write to Gold Layer
    logging.info("Writing final results to Gold Layer...")
    
    # Select ONLY columns needed for Analysis
    final_cols = [
        # Identifiers
        "dealid", 
        "acquirer_ticker", 
        "primaryindustrysector",
        
        # Key Inputs
        "dealsize",
        
        "market_cap_pre",
        "ev_pre",
        "ebitda_pre",
        
        "market_cap_post",
        "ev_post",
        "ebitda_post",

        
        # Key Ratios / Outputs (Regression Vars)
        
        "delta_ev_pct",        # Y1
        "delta_mkt_cap_pct",   # Y2
        "delta_ebitda_pct",    # Y3
        "deal_size_ratio"      # X1
    ]
    
    # Keep only existing columns
    existing_cols = [c for c in final_cols if c in df_clean.columns]
    df_final = df_clean.select(existing_cols)

    # 5.1 Write to Parquet (Local File for easy sharing)
    output_dir = "data"
    os.makedirs(output_dir, exist_ok=True)
    parquet_path = os.path.join(output_dir, "gold_data.parquet")
    df_final.write_parquet(parquet_path)
    logging.info(f"Saved Parquet to {parquet_path}")
    
    # 5.2 Write to Database (Gold Schema)
    df_final.to_pandas().to_sql(
        "final_data",
        engine,
        schema="gold",
        if_exists="replace",
        index=False
    )
    logging.info("Saved table to gold.final_data")

if __name__ == "__main__":
    try:
        transform_gold()
    except Exception as e:
        logging.error(f"Gold transformation failed: {e}")
        raise