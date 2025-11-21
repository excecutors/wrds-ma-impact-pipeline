import os
import sys
import logging
import polars as pl

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.utils.db import get_postgres_engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def transform_silver():
    """
    Silver Layer Logic:
    1. Clean: Filter out deals with NULL size.
    2. Join: Deal -> Acquirer -> Industry.
    3. Time-Travel Join: Find Pre-Deal (t-1) and Post-Deal (t+1) financials.
    4. Rename: Convert cryptic Compustat codes to readable English (e.g., dlttq -> long_term_debt).
    5. Filter: Drop rows with missing essential financial data.
    """
    engine = get_postgres_engine()
    
    # ==========================================
    # 1. Read Bronze Data
    # ==========================================
    logging.info("Reading tables from Bronze layer...")

    # A. Deals (Added Filter: dealsize must not be null)
    q_deal = "SELECT * FROM bronze.ot_glb_deal WHERE dealsize IS NOT NULL"
    df_deal = pl.read_database(q_deal, engine)

    # B. Relations
    q_rel = "SELECT * FROM bronze.ot_glb_companybuysiderelation"
    df_rel = pl.read_database(q_rel, engine)

    # C. Acquirer Company Info
    q_comp = """
        SELECT 
            companyid, companyname, ticker, 
            hqglobalsubregion, ownershipstatus, primaryindustrysector
        FROM bronze.ot_glb_company 
        WHERE hqglobalsubregion = 'North America' 
        AND ownershipstatus = 'Publicly Held'
    """
    df_comp = pl.read_database(q_comp, engine)

    # D. Financials (Compustat)
    # apdedateq is the Actual Period End Date
    q_fund = """
        SELECT tic, apdedateq, dlttq, dlcq, cheq, prccq, cshoq, oibdpq
        FROM bronze.fundq 
        WHERE apdedateq IS NOT NULL
    """
    df_fund = pl.read_database(q_fund, engine)

    logging.info(f"Loaded: Deal({df_deal.height}), Rel({df_rel.height}), Comp({df_comp.height}), Fund({df_fund.height})")

    # ==========================================
    # 2. Cleaning & Casting
    # ==========================================
    # Ensure Dates are pl.Date type
    df_deal = df_deal.with_columns(pl.col("announceddate").cast(pl.Date), pl.col("dealdate").cast(pl.Date))
    df_rel = df_rel.with_columns(pl.col("dealdate").cast(pl.Date))
    df_fund = df_fund.with_columns(pl.col("apdedateq").cast(pl.Date))

    # Rename internal join columns
    df_deal = df_deal.rename({"companyid": "target_company_id", "companyname": "target_company_name"})
    df_comp = df_comp.rename({"companyid": "acquirer_company_id", "companyname": "acquirer_name", "ticker": "acquirer_ticker"})
    df_rel = df_rel.rename({"companyid": "acquirer_company_id"}) 

    # ==========================================
    # 3. Structural Joins
    # ==========================================
    logging.info("Performing Structural Joins...")

    # 3.1 Deal -> Relation
    df_merged = df_deal.join(
        df_rel,
        left_on=["target_company_name", "dealdate"],
        right_on=["targetcompanyname", "dealdate"],
        how="inner"
    )

    # 3.2 Relation -> Acquirer Info
    df_merged = df_merged.join(
        df_comp,
        on="acquirer_company_id",
        how="inner"
    )

    # ==========================================
    # 4. Temporal Joins (Rename to Readable English)
    # ==========================================
    logging.info("Performing Temporal Joins & Renaming...")

    # Sort required for asof join
    df_fund = df_fund.sort(["tic", "apdedateq"])
    df_merged = df_merged.sort("announceddate")

    # --- 4.1 Pre-Deal Data (Backward Search) ---
    df_pre = df_merged.join_asof(
        df_fund,
        left_on="announceddate",
        right_on="apdedateq",
        by_left="acquirer_ticker",
        by_right="tic",
        strategy="backward"
    )
    
    # Rename to CLEAR ENGLISH
    rename_pre = {
        "apdedateq": "period_end_pre",
        "dlttq": "long_term_debt_pre",   # was dlttq
        "dlcq":  "current_debt_pre",     # was dlcq
        "cheq":  "cash_pre",             # was cheq
        "prccq": "stock_price_pre",      # was prccq
        "cshoq": "shares_outstanding_pre", # was cshoq
        "oibdpq": "ebitda_pre"           # was oibdpq
    }
    df_pre = df_pre.rename(rename_pre)

    # --- 4.2 Post-Deal Data (Forward Search) ---
    df_final = df_pre.join_asof(
        df_fund,
        left_on="announceddate",
        right_on="apdedateq",
        by_left="acquirer_ticker",
        by_right="tic",
        strategy="forward"
    )

    # Rename to CLEAR ENGLISH
    rename_post = {
        "apdedateq": "period_end_post",
        "dlttq": "long_term_debt_post",
        "dlcq":  "current_debt_post",
        "cheq":  "cash_post",
        "prccq": "stock_price_post",
        "cshoq": "shares_outstanding_post",
        "oibdpq": "ebitda_post"
    }
    df_final = df_final.rename(rename_post)

    # ==========================================
    # 5. Filter NA (Ensure Completeness)
    # ==========================================
    logging.info("Filtering out rows with missing financial data...")

    initial_count = df_final.height
    
    # Define critical columns (using new English names)
    critical_cols = [
        "stock_price_pre", "shares_outstanding_pre", "long_term_debt_pre", "cash_pre",
        "stock_price_post", "shares_outstanding_post", "long_term_debt_post", "cash_post"
    ]
    
    # Drop rows where ANY critical data is missing
    df_final = df_final.drop_nulls(subset=critical_cols)
    
    final_count = df_final.height
    logging.info(f"Dropped {initial_count - final_count} rows due to missing data. Final count: {final_count}")

    # ==========================================
    # 6. Write to Silver Layer
    # ==========================================
    logging.info("Writing to Silver Layer...")

    output_cols = [
        "dealid", "announceddate", "dealstatus", "dealsize",
        "target_name", "acquirer_name", "acquirer_ticker", 
        "primaryindustrysector",
        
        # Pre-Deal Vars (Readable)
        "period_end_pre", 
        "stock_price_pre", "shares_outstanding_pre", 
        "long_term_debt_pre", "current_debt_pre", "cash_pre", 
        "ebitda_pre",
        
        # Post-Deal Vars (Readable)
        "period_end_post", 
        "stock_price_post", "shares_outstanding_post", 
        "long_term_debt_post", "current_debt_post", "cash_post", 
        "ebitda_post"
    ]
    
    # Select only columns that exist
    existing_cols = [c for c in output_cols if c in df_final.columns]
    df_final = df_final.select(existing_cols)

    df_final.to_pandas().to_sql(
        "deal_financials_linked",
        engine,
        schema="silver",
        if_exists="replace",
        index=False
    )
    
    logging.info(f"Successfully wrote {len(df_final)} rows to silver.deal_financials_linked")

if __name__ == "__main__":
    try:
        transform_silver()
    except Exception as e:
        logging.error(f"Transformation failed: {e}")
        raise