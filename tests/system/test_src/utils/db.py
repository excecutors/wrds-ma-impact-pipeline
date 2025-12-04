# tests/system/test_src/utils/db.py
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv


def get_postgres_engine():
    """
    Connect to the PostgreSQL database
    Priority:
    1. Use DB_URL if set (CI/CD system tests).
    2. Otherwise, use the credentials from the .env file (Local use).

    """
    load_dotenv()  # load environment variables from .env file

    db_url = os.getenv("DB_URL")  # Allow override in CI/CD
    if db_url:
        return create_engine(db_url)

    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db_name = os.getenv("POSTGRES_DB")

    # !!! IMPORTANT !!!
    # When connecting from within the 'app' container,
    # the host name is the service name specified in the docker-compose.yml file, rather than 'localhost'.
    host = os.getenv("POSTGRES_HOST", "postgres_db")
    port = os.getenv("POSTGRES_PORT", "5432")

    try:
        engine_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
        engine = create_engine(engine_url)
        return engine
    except Exception as e:
        print(f"Failed to connect to PostgreSQL: {e}")
        raise
