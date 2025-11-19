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


def chunked_sql_query(db, query_template, id_list, chunk_size=500):
    """
    辅助函数：分块查询 WRDS，防止 SQL 语句过长或返回数据过大
    query_template: 必须包含 {ids} 占位符，例如 "SELECT * FROM table WHERE id IN ({ids})"
    """
    all_dfs = []
    total_ids = len(id_list)
    
    for i in range(0, total_ids, chunk_size):
        chunk = id_list[i:i + chunk_size]

        # Escape single quotes for SQL by doubling them
        escaped_values = [str(x).replace("'", "''") for x in chunk]
        ids_str = ",".join([f"'{v}'" for v in escaped_values])
        
        query = query_template.format(ids=ids_str)
        try:
            df_chunk = db.raw_sql(query)
            if not df_chunk.empty:
                all_dfs.append(df_chunk)
        except Exception as e:
            logging.error(f"Error querying chunk {i}-{i+chunk_size}: {e}")
            
    if not all_dfs:
        return pd.DataFrame()
    
    return pd.concat(all_dfs, ignore_index=True)
    
    
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
            SELECT * FROM pitchbk_other_row.ot_glb_deal
            WHERE announceddate >= '2000-01-01' 
            AND announceddate <= '2024-11-30' 
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
            SELECT * FROM pitchbk_other_row.ot_glb_company
            WHERE hqglobalsubregion = 'North America'
            AND ownershipstatus = 'Publicly Held'
        """
        df_company = db_wrds.raw_sql(company_query)
        logging.info(f"Loaded {len(df_company)} filtered companies.")
        
        df_company.to_sql('ot_glb_company', db_postgres_engine, schema=bronze_schema, if_exists='replace', index=False)
        logging.info("Successfully loaded 'ot_glb_company' into Bronze.")
        
    except Exception as e:
        logging.error(f"Failed to process 'ot_glb_company': {e}")

    # 3. dim_company_relations
    try:
        logging.info("Extracting 'ot_glb_companybuysiderelation'...")
        # 如果这个表未来变大导致 crash，也可以加 LIMIT 或分块
        df_rel = db_wrds.get_table(library='pitchbk_other_row', table='ot_glb_companybuysiderelation')
        df_rel.to_sql('ot_glb_companybuysiderelation', db_postgres_engine, schema=bronze_schema, if_exists='replace', index=False)
        logging.info("Successfully loaded 'ot_glb_companybuysiderelation'.")
    except Exception as e:
        logging.error(f"Failed to process 'ot_glb_companybuysiderelation': {e}")

    # 4. dim_company_industries
    try:
        logging.info("Extracting 'ot_glb_companyindustryrelation'...")
        df_ind = db_wrds.get_table(library='pitchbk_other_row', table='ot_glb_companyindustryrelation')
        df_ind.to_sql('ot_glb_companyindustryrelation', db_postgres_engine, schema=bronze_schema, if_exists='replace', index=False)
        logging.info("Successfully loaded 'ot_glb_companyindustryrelation'.")
    except Exception as e:
        logging.error(f"Failed to process 'ot_glb_companyindustryrelation': {e}")

    
    # A. 筛选属于 Tech 或 Finance 的公司 ID
    target_sectors = ['Information Technology', 'Financial Services']
    # 确保大小写匹配，最好先转成小写对比，这里假设数据规范
    tech_fin_companies = df_ind[df_ind['industrysector'].isin(target_sectors)]['companyid'].unique()
    
    # B. 筛选出这些行业的 Acquirer Companies
    # df_company 已经是北美+上市公司了，现在进一步限制行业
    relevant_companies = df_company[df_company['companyid'].isin(tech_fin_companies)]
    
    # C. 获取这些公司的 Ticker
    # 我们只关心那些有 Ticker 的公司，因为没有 Ticker 无法连接到 CRSP/Compustat
    target_tickers = relevant_companies['ticker'].dropna().unique().tolist()
    
    logging.info(f"Found {len(target_tickers)} unique tickers for Tech/Finance Acquirers in NA.")

    # 释放内存 (可选，防止后续步骤OOM)
    del df_deal, df_company, df_rel, df_ind, relevant_companies
    gc.collect()
    
    if not target_tickers:
        logging.error("No tickers found! Check your filter logic.")
        return
    # ==========================================
    # Phase 3: Targeted Fetching (Bridging Ticker -> GVKEY -> PERMCO)
    # ==========================================

    # 5. Resolve Ticker to GVKEY (New Step)
    # 我们需要先通过 Compustat 的公司表，把 PitchBook 的 Ticker 翻译成 GVKEY
    logging.info("5. Mapping Tickers to GVKEYs (using comp_na_daily_all.company)...")
    
    # comp_na_daily_all.company 包含 tic (ticker) 和 gvkey
    map_query_tpl = "SELECT DISTINCT tic, gvkey FROM comp_na_daily_all.company WHERE tic IN ({ids})"
    df_map = chunked_sql_query(db_wrds, map_query_tpl, target_tickers, chunk_size=1000)
    
    if df_map.empty:
        logging.error("Could not map any PitchBook Tickers to Compustat GVKEYs. Stopping.")
        return
        
    valid_gvkeys = df_map['gvkey'].dropna().unique().tolist()
    logging.info(f"Mapped {len(target_tickers)} tickers to {len(valid_gvkeys)} unique GVKEYs.")

    # 6. Extract Link Table (Using GVKEYs)
    # 现在我们有了 GVKEY，可以正确地查询 Link Table 了
    logging.info("6. Extracting filtered 'ccmxpf_lnkhist' using GVKEY list...")
    
    link_query_tpl = "SELECT * FROM crsp_a_ccm.ccmxpf_lnkhist WHERE gvkey IN ({ids})"
    # 注意：这里传入的是 valid_gvkeys
    df_link = chunked_sql_query(db_wrds, link_query_tpl, valid_gvkeys, chunk_size=1000)
    
    if not df_link.empty:
        df_link.to_sql('ccmxpf_lnkhist', db_postgres_engine, schema=bronze_schema, if_exists='replace', index=False)
        logging.info(f"Loaded {len(df_link)} link records.")
        
        # 获取有效的 PERMCOs (用于查询股价)
        valid_permcos = df_link['lpermco'].dropna().unique().tolist()
        logging.info(f"Identified {len(valid_permcos)} PERMCOs for stock price fetch.")
    else:
        logging.error("No Link Table records found for these GVKEYs.")
        valid_permcos = []

    # 7. Fundq (Using GVKEYs)
    logging.info("7. Extracting filtered 'fundq' using GVKEY list...")
    
    fundq_query_tpl = "SELECT * FROM comp_na_daily_all.fundq WHERE gvkey IN ({ids}) AND apdedateq >= '1999-01-01'"
    df_fundq = chunked_sql_query(db_wrds, fundq_query_tpl, valid_gvkeys, chunk_size=1000)
    
    if not df_fundq.empty:
        df_fundq.to_sql('fundq', db_postgres_engine, schema=bronze_schema, if_exists='replace', index=False)
        logging.info(f"Successfully loaded {len(df_fundq)} financial records.")
    else:
        logging.warning("No financial records found.")

    # 8. DSF (Using PERMCOs)
    if valid_permcos:
        logging.info("8. Extracting filtered 'dsf' using PERMCO list...")
        
        # 每次查询 200 个公司的历史股价，防止内存溢出
        dsf_query_tpl = "SELECT * FROM crsp_a_stock.dsf WHERE permco IN ({ids}) AND date >= '1999-01-01'"
        df_dsf = chunked_sql_query(db_wrds, dsf_query_tpl, valid_permcos, chunk_size=200)
        
        if not df_dsf.empty:
            df_dsf.to_sql('dsf', db_postgres_engine, schema=bronze_schema, if_exists='replace', index=False)
            logging.info(f"Successfully loaded {len(df_dsf)} stock price records.")
        else:
            logging.warning("No stock price records found.")
    else:
        logging.warning("Skipping DSF fetch because no valid PERMCOs were found.")


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