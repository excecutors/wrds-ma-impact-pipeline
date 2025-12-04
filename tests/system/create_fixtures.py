import os
import polars as pl
from test_src.utils.db import get_postgres_engine


def create_fixtures(limit: int = 100):
    """
    Create fixture Parquet files from Bronze tables.
    These are small subsets (default 100 rows) used in system tests.
    """

    engine = get_postgres_engine()
    output_dir = os.path.join("tests", "system", "fixtures")
    os.makedirs(output_dir, exist_ok=True)

    tables = {
        "ot_glb_deal": "SELECT * FROM bronze.ot_glb_deal LIMIT {limit};",
        "ot_glb_company": "SELECT * FROM bronze.ot_glb_company LIMIT {limit};",
        "ot_glb_companybuysiderelation": "SELECT * FROM bronze.ot_glb_companybuysiderelation LIMIT {limit};",
        "company_industry_relation": "SELECT * FROM bronze.company_industry_relation LIMIT {limit};",
        "fundq": "SELECT * FROM bronze.fundq LIMIT {limit};",
    }

    for name, query in tables.items():
        print(f"Extracting fixture for {name}...")
        df = pl.read_database(query.format(limit=limit), engine)
        path = os.path.join(output_dir, f"{name}.parquet")
        df.write_parquet(path)
        print(f"Saved {len(df)} rows to {path}")


if __name__ == "__main__":
    create_fixtures(limit=100)
