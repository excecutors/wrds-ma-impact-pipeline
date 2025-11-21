import os
import gc
import sys
import wrds
import logging
import pandas as pd
from dotenv import load_dotenv

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.utils.db import get_postgres_engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_wrds_connection():
    """
    connect to WRDS
    """
    logging.info("Connecting to WRDS...")
    try:
        username = os.getenv('WRDS_USERNAME')
        password = os.getenv('WRDS_PASSWORD')
        
        if not username or not password:
            logging.error("WRDS_USERNAME or WRDS_PASSWORD is not found in .env file.")
            raise ValueError("The WRDS credentials have not been set in the .env file.")
        
        db = wrds.Connection(
            wrds_username=username,
            wrds_password=password
        )
        logging.info("Successfully connected to WRDS.")
        return db
    except Exception as e:
        logging.error(f"Failed to connect to WRDS: {e}")
        logging.error("Please ensure that your WRDS credentials have been correctly set up either through .pgpass or the login prompt.")
        raise
    
    
def extract_and_load(db_wrds, db_postgres_engine):
    """
    Extract the *filtered* historical data (from 2020.01 to 2024.12) from WRDS.
    Load it into the Bronze layer of PostgreSQL.
    """
    
    bronze_schema = 'bronze'
    
    # 1. fact_deal 
    try:
        logging.info("Extracting filtered 'ot_glb_deal' data...")
        deal_query = """
            SELECT 
                dealid
                ,companyid
                ,companyname
                ,dealdate
                ,announceddate
                ,dealsize
                ,nativecurrencyofdeal
                ,dealstatus
                ,dealtype
                ,dealclass
                ,percentacquired
            FROM pitchbk_other_row.ot_glb_deal
            WHERE announceddate >= '2000-01-01' 
            AND announceddate <= '2024-12-31' 
            AND dealstatus = 'Completed'      
            AND dealtype = 'Merger/Acquisition' 
            AND percentacquired > 50        
            AND nativecurrencyofdeal = 'US Dollars (USD)'
        """
        df_deal = db_wrds.raw_sql(deal_query)
        logging.info(f"Loaded {len(df_deal)} completed M&A deals.")
        
        df_deal.to_sql('ot_glb_deal', db_postgres_engine, schema=bronze_schema, if_exists='replace', index=False)
        logging.info("Successfully loaded 'ot_glb_deal' into Bronze.")
        
    except Exception as e:
        logging.error(f"Failed to process 'ot_glb_deal': {e}")
        
    # 2. dim_company
    try:
        logging.info("Extracting filtered 'ot_glb_company' (North America Public Only)...")
        # north america public companies only 
        company_query = """
            SELECT 
                companyid
                ,companyname
                ,businessstatus
                ,ownershipstatus
                ,companyfinancingstatus
                ,universe
                ,hqglobalsubregion
                ,hqcountry
                ,ticker
                ,exchange
                ,primaryindustrysector
                ,primaryindustrygroup
                ,primaryindustrycode
            FROM pitchbk_other_row.ot_glb_company
            WHERE hqglobalsubregion = 'North America'
            AND ownershipstatus = 'Publicly Held'
        """
        df_company = db_wrds.raw_sql(company_query)
        list_north_america_public_companyid = df_company['companyid'].dropna().unique().tolist()
        list_north_america_public_ticker = df_company['ticker'].dropna().unique().tolist()
        logging.info(f"Loaded {len(df_company)} filtered companies.")
        
        df_company.to_sql('ot_glb_company', db_postgres_engine, schema=bronze_schema, if_exists='replace', index=False)
        logging.info("Successfully loaded 'ot_glb_company' into Bronze.")
        
    except Exception as e:
        logging.error(f"Failed to process 'ot_glb_company': {e}")

    # 3. dim_company_buyside_relations
    try:
        deal_relation_query = f"""
        SELECT 
            companyid
            ,targetcompanyid
            ,targetcompanyname
            ,Dealdate
            ,Dealtype
        FROM pitchbk_other_row.ot_glb_companybuysiderelation
        WHERE Dealdate >= '2000-01-01'
        AND Dealdate <= '2024-12-31'
        AND Dealtype = 'Merger/Acquisition'
        AND companyid IN {tuple(list_north_america_public_companyid)}
        """
        logging.info("Extracting 'ot_glb_companybuysiderelation'...")
        df_rel = db_wrds.raw_sql(deal_relation_query)
        # hqglobalsubregion
        df_rel.to_sql('ot_glb_companybuysiderelation', db_postgres_engine, schema=bronze_schema, if_exists='replace', index=False)
        logging.info("Successfully loaded 'ot_glb_companybuysiderelation'.")
    except Exception as e:
        logging.error(f"Failed to process 'ot_glb_companybuysiderelation': {e}")

    # 4. dim_company_industries
    try:
        logging.info("Extracting 'ot_glb_companyindustryrelation'...")
        company_industry_query = f"""
            SELECT 
                companyid
                ,industrysector
                ,industrygroup
                ,industrycode
                ,isprimary
            FROM pitchbk_other_row.ot_glb_companyindustryrelation
            WHERE companyid IN {tuple(list_north_america_public_companyid)}
        """
        df_company_ind = db_wrds.raw_sql(company_industry_query)
        logging.info("Successfully loaded 'ot_glb_companyindustryrelation'.")
        
        df_company_ind.to_sql('ot_glb_companyindustryrelation', db_postgres_engine, schema=bronze_schema, if_exists='replace', index=False)
        logging.info("Successfully loaded 'ot_glb_company' into Bronze.")
        
    except Exception as e:
        logging.error(f"Failed to process 'ot_glb_companyindustryrelation': {e}")
        
        
    # 5. fact_financial_quarterly
    try:
        logging.info("Extracting 'fundq'...")
        financial_quarterly = f"""
            SElECT 
                gvkey
                ,fyearq
                ,fqtr
                ,apdedateq
                ,tic
                ,curncdq
                ,dlttq
                ,dlcq
                ,cheq
                ,prccq
                ,prchq
                ,prclq
                ,cshoq
                ,oibdpq
            FROM comp_na_daily_all.fundq
            WHERE 
                tic IN {tuple(list_north_america_public_ticker)}
            AND 
                curncdq = 'USD'
            AND 
                fyearq >= 1999
            AND 
                fyearq <= 2025
        """
        df_financial_quarterly = db_wrds.raw_sql(financial_quarterly)
        logging.info("Successfully loaded 'fundq'.")
        
        df_financial_quarterly.to_sql('fundq', db_postgres_engine, schema=bronze_schema, if_exists='replace', index=False)
        logging.info("Successfully loaded 'ot_glb_company' into Bronze.")
        
    except Exception as e:
        logging.error(f"Failed to process 'fundq': {e}")


if __name__ == "__main__":
    logging.info("Loading environment variables...")
    load_dotenv()
    
    logging.info("--- Starting Smart Data Ingestion ---")
    try:
        db_wrds = get_wrds_connection()
        db_postgres = get_postgres_engine()
        extract_and_load(db_wrds, db_postgres)
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        
    logging.info("--- Pipeline Finished ---")