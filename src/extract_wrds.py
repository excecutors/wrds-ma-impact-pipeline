import os
import wrds
import logging
from src.utils.db import get_postgres_engine
from dotenv import load_dotenv

# 设置日志
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
    
    # 1. M&A 交易表 (fact_deal): 应用你的所有过滤器 
    try:
        logging.info("Extracting filtered 'ot_glb_deal' data...")
        deal_query = """
            SELECT * FROM pitchbk_other_row.ot_glb_deal
            WHERE announceddate >= '2000-01-01' 
              AND announceddate <= '2024-11-30' 
              AND dealstatus = 'Completed'      
              AND dealtype = 'Merge/Acquisition' 
              AND percentacquired > 50        
              AND nativecurrencyofdeal = 'US Dollars (USD)'
        """
        deal_df = db_wrds.raw_sql(deal_query)
        logging.info(f"Loaded {len(deal_df)} completed M&A deals.")
        
        deal_df.to_sql('ot_glb_deal', db_postgres_engine, schema=bronze_schema, if_exists='replace', index=False)
        logging.info("Successfully loaded 'ot_glb_deal' into Bronze.")
        
    except Exception as e:
        logging.error(f"Failed to process 'ot_glb_deal': {e}")


    pb_tables = [
        'ot_glb_companybuysiderelation',
        'ot_glb_company',
        'ot_glb_companyindustryrelation',
    ]
    
    for table_name in pb_tables:
        try:
            logging.info(f"Extracting pitchbk_other_row.{table_name}...")
            df = db_wrds.get_table(library='pitchbk_other_row', table=table_name)
            df.to_sql(table_name, db_postgres_engine, schema=bronze_schema, if_exists='replace', index=False)
            logging.info(f"Successfully loaded '{table_name}' into Bronze.")
        except Exception as e:
            logging.error(f"Failed to process {table_name}: {e}")

    # CRSP/Compustat tables (historical full data up to 2024-12)
    try:
        logging.info("Extracting 'ccmxpf_lnkhist'...")
        df_link = db_wrds.get_table(library='crsp_a_ccm', table='ccmxpf_lnkhist') 
        df_link.to_sql('ccmxpf_lnkhist', db_postgres_engine, schema=bronze_schema, if_exists='replace', index=False)

        logging.info("Extracting 'fundq' (pre-2025)...")
        fundq_query = "SELECT * FROM comp_na_daily_all.fundq WHERE apdedateq <= '2024-12-31'" 
        df_fundq = db_wrds.raw_sql(fundq_query)
        df_fundq.to_sql('fundq', db_postgres_engine, schema=bronze_schema, if_exists='replace', index=False)

        logging.info("Extracting 'dsf' (pre-2025)...")
        dsf_query = "SELECT * FROM crsp_a_stock.dsf WHERE date <= '2024-12-31'"
        df_dsf = db_wrds.raw_sql(dsf_query)
        df_dsf.to_sql('dsf', db_postgres_engine, schema=bronze_schema, if_exists='replace', index=False)
        
    except Exception as e:
        logging.error(f"Failed to process CRSP/Compustat tables: {e}")


if __name__ == "__main__":
    
    logging.info("Loading environment variables from .env file...")
    load_dotenv()
    
    logging.info("--- Starting Data Ingestion Pipeline (Historical Only) ---")
    
    db_wrds = get_wrds_connection()
    db_postgres = get_postgres_engine()
    
    extract_and_load(db_wrds, db_postgres)
    
    logging.info("--- Data Ingestion Pipeline Finished ---")