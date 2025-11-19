# M&A Value Impact Pipeline (Local Deployment)

## Project Description

This project builds a complete data engineering pipeline that explores how mergers and acquisitions (M&A) affect a company's enterprise value (EV) and profitability. The data comes from WRDS (PitchBook, Preqin, and Compustat) and moves through a full local data workflow: ingestion, storage, transformation, orchestration, analysis, and testing.

Everything runs locally using **Docker and Apache Airflow**, with data stored in **PostgreSQL** and files organized in Bronze/Silver/Gold layers. This setup demonstrates modular design, reproducibility, and automation through GitHub Actions.

---

## Research Question

When a public company buys another firm, does it gain or lose enterprise value, and does that depend on deal size or industry?

---

## Architecture Overview

```
project-root/
├── .devcontainer/              # Configuration for reproducible environment
│   ├── devcontainer.json       # VS Code config
│   ├── docker-compose.yml      # Defines App + Postgres services
│   └── Dockerfile              # Python environment definition
├── dags/                     ← Airflow DAG (extract → transform → analyze)
│   └── ma_pipeline_dag.py
├── src/
│   ├── extract_wrds.py       ← Pulls M&A + Compustat data from WRDS
│   ├── transform_clean.py    ← Cleans and joins deal + financial data
│   ├── analyze_regression.py ← Calculates ΔEV% and runs regression
│   └── utils/                ← Helper functions and schema validation
│       └── db.py               # Database connection helper
├── postgres/
│   └── init.sql                # Database schema initialization (Bronze/Silver/Gold)
├── tests/                    ← Pytest unit and data quality tests
├── .env                        # Secrets (NOT synced to Git)
├── .gitignore                  # Git ignore rules
├── requirements.txt            # Python dependencies
└── README.md                   # Project Documentation
```

---

## Pipeline Components

### 1. Data Ingestion

Pulls M&A and financial data from WRDS (PitchBook, CRSP and Compustat). Saves raw CSVs into `data/bronze/`.

### 2. Data Storage

Data is stored in PostgreSQL or MinIO (S3-like) containers with schemas for `fact_transactions`, `fact_ev`, `fact_financials`, and `dim_company`.

### 3. Data Transformation and Analysis

Uses Polars to clean, normalize, and join data by ticker or WRDS ID. Calculates changes in EV and margins:

```
ΔEV% = (EV_30d_after − EV_5d_before) / EV_5d_before
ΔMargin% = EBITDA_margin_post − EBITDA_margin_pre
```

Runs regression: `ΔEV% ~ deal_size_ratio + industry_dummies`.

### 4. Orchestration

An Airflow DAG automates the full workflow — extract, transform, analyze — and can be scheduled to run daily or on demand.

### 5. Containerization and CI/CD

Everything runs in Docker Compose with Airflow, Postgres, and MinIO containers. GitHub Actions handles linting, testing, and schema validation before merges.

### 6. Testing

Pytest covers schema integrity, missing/null checks, and logical validation (e.g., deal sizes not negative). Includes a small regression smoke test.

### 7. Visualization

Streamlit dashboard built from `data/gold/` to show results by industry or deal size.

---

## Team Roles

| Role | Responsibility |
|------|----------------|
| **Data Engineer** | Build and containerize the local environment using **Docker Compose** (Airflow, Postgres, MinIO). Implement data lake structure (Bronze → Silver → Gold) and manage credential security and local orchestration. |
| **Data Analyst** | Design and maintain **WRDS extraction scripts** (PitchBook, Preqin, Compustat), perform data cleaning and transformation with Polars, and document schema design for each stage of the pipeline. |
| **Fin/Quant Analyst** | Define **event windows** (pre/post M&A), calculate enterprise value (EV) and profitability metrics, run regression analyses, and create visual analytics to interpret the results for the final presentation. |
| **Data Architect & QA Engineer** | Define overall **pipeline architecture and metadata standards**, ensure modularity and observability in Airflow DAGs, and develop **automated validation tests** for schema integrity, data completeness, and reproducibility. |

---

## Data Engineering Principles

| Principle       | Implementation                                          |
| --------------- | ------------------------------------------------------- |
| Scalability     | Bronze → Silver → Gold layers allow incremental updates |
| Modularity      | Each script has a single, clear function                |
| Reusability     | Reusable helper functions and schema validators         |
| Observability   | Airflow logs and test reports track pipeline health     |
| Security        | WRDS credentials stored locally, not in repo            |
| Reproducibility | Docker ensures identical environments                   |

---

## Tech Stack

* **Python:** Polars, Pandas, Statsmodels
* **Storage:** PostgreSQL, MinIO (S3)
* **Orchestration:** Apache Airflow (Docker Compose)
* **CI/CD:** GitHub Actions, Pytest
* **Visualization:** Streamlit or Power BI (optional)

---

## How to Run Locally
1. Clone the repository:  
```
git clone https://github.com/excecutors/wrds-ma-impact-pipeline.git
cd wrds-ma-impact-pipeline
```
2. Configure Environment Variables
Create a file named .env in the project root directory. Copy the content below and fill in your credentials.  
```
# .env
# Local Database Credentials (DO NOT CHANGE)
POSTGRES_USER=admin
POSTGRES_PASSWORD=strongpassword123
POSTGRES_DB=ma_pipeline_db

# Your WRDS Username (Required)
WRDS_USER=your_wrds_username
WRDS_PASSWORD=our_wrds_password
```

3. Launch the Environment (Docker)
We use Docker Compose to spin up the database and the application environment. Run the following command from the project root:
```
# Builds the Python environment and starts PostgreSQL in the background
docker-compose -f .devcontainer/docker-compose.yml up -d --build
```
*Wait a moment for the database to initialize.*

4. Running the Pipeline  
**Step 1: Data Ingestion (Bronze Layer)**   
This script pulls historical data (2000.01-2024.12) from WRDS and loads it into our local PostgreSQL database.   
Since the environment is containerized, you need to execute the script inside the running container:  
```
# 1. Enter the application container
docker exec -it ma_project_app bash
```
  
# 2. Run the ingestion script (inside the container)

```
python src/extract_wrds.py
```
*Estimated time: 10-30 minutes depending on network.*    
    
**Step 2: Verify Data**   
You can connect to the database using DBeaver or DataGrip from your host machine (outside VS Code):   
- Host: localhost   
- Port: 5432   
- Database: ma_pipeline_db   
- Username: admin   
- Password: strongpassword123      
Run this SQL to check if data loaded:
```
SELECT COUNT(*) FROM bronze.ot_glb_deal;
```
5. Shutdown
When you are finished, you can stop and remove the containers with:
```
docker-compose -f .devcontainer/docker-compose.yml down
```
......(other steps)........   
  
Access Airflow at [http://localhost:8080](http://localhost:8080) and run the `ma_pipeline_dag` to execute the workflow.

---

## Deliverables

* Full GitHub repository and working pipeline
* README documentation and architecture diagram
* 5–10 minute walkthrough video
* Final results file (`/data/gold/final_results.parquet`) with ΔEV% and regression output

---

## Summary

A reproducible, locally containerized data pipeline demonstrating end-to-end engineering — from data ingestion to regression analysis — to answer one question:

**Do M&A deals actually create value, and what drives the difference?**
