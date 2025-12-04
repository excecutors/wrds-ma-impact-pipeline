import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DB_URL", "postgresql://test:test@localhost:5433/testdb")
engine = create_engine(DB_URL)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def init_schemas(engine):
    """
    Ensure the bronze, silver, and gold schemas exist in the PostgreSQL database.
    """
    with engine.begin() as conn:
        for schema in ["bronze", "silver", "gold"]:
            logging.info(f"Ensuring schema '{schema}' exists...")
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
    logging.info("Schemas bronze, silver, and gold are ready.")


def build_test_data():
    """
    Generate tables and files for the system test:
    1. Load in fixture files. These files will serve as the bronze tables.
    2. Store fixture files in PostgreSQL database.
    3. Produce the silver table with the transform_silver function in transform_clean2.py.
    4. Produce the gold dataset with the transform_gold function in gold_layer2.py.
    """
    init_schemas(engine)

    output_dir = os.path.join(os.path.dirname(__file__), "test_data")
    os.makedirs(output_dir, exist_ok=True)

    fixtures = {
        "ot_glb_deal": "tests/system/fixtures/ot_glb_deal.parquet",
        "ot_glb_company": "tests/system/fixtures/ot_glb_company.parquet",
        "ot_glb_companybuysiderelation": "tests/system/fixtures/ot_glb_companybuysiderelation.parquet",
        "company_industry_relation": "tests/system/fixtures/company_industry_relation.parquet",
        "fundq": "tests/system/fixtures/fundq.parquet",
    }

    for table, path in fixtures.items():
        df = pd.read_parquet(path)
        df.to_sql(table, engine, schema="bronze", if_exists="replace", index=False)

    from transform_clean2 import transform_silver

    transform_silver()

    from gold_layer2 import transform_gold

    transform_gold(output_dir=output_dir)


if __name__ == "__main__":
    build_test_data()
