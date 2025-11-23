# IDS706: Airflow + Streamlit: Doom Reset & Fresh Run

> How to rerun (start from step 0) or get the pipeline running from scratch (start from step 1).

This README explains:

* **Step 0 – Doom reset**: delete old environments, clear ports, and restart Docker.
* **Step 1 – Clean Airflow setup**: correct Python, install Airflow, and init metadata.
* **Step 2 – Run Airflow (scheduler + webserver)**.
* **Step 3 – Trigger the DAG and verify Bronze → Silver → Gold**.
* **Step 4 – Launch the Streamlit dashboard.**

All commands are shown so you can copy‑paste them into your terminal.

---

## Step 0 – Doom Reset (Clean Slate)

Use this if things are in a weird state or you just pulled the repo on a new machine and want to be sure everything is clean.

### 0.1. Kill anything on Airflow ports (8080, 8793)

```bash
lsof -i :8080
kill -9 <PID>

lsof -i :8793
kill -9 <PID>
```

If you can’t kill what’s on 8080 (or Docker is using it), we’ll just use **8081** for Airflow later.

### 0.2. Make sure Docker is running

* Open **Docker Desktop**.
* Wait until it says **Running**.

Then check:

```bash
docker ps
```

If you see errors like:

```bash
Cannot connect to the Docker daemon ... Is the docker daemon running?
```

→ Docker Desktop is not fully up yet.

### 0.3. Stop any old project containers

From the repo root (`wrds-ma-impact-pipeline`):

```bash
cd wrds-ma-impact-pipeline

# Stop and remove old containers
docker-compose -f .devcontainer/docker-compose.yml down
```

Optional hard reset (if you really want to clean Docker):

```bash
docker system prune -f
```

### 0.4. Delete any broken Airflow venv

If you previously installed Airflow with the wrong Python version, clean it:

```bash
deactivate  # if a venv is active
rm -rf .airflow-venv
```

At this point: **no containers from this project are running and no Airflow venv exists.**

---

## Step 1 – Create a Fresh Airflow Environment

Airflow 2.9.1 supports Python **3.10** and **3.11**, not 3.12+.

We’ll set up a new venv with Python 3.11 and install Airflow using the official constraints.

### 1.1. Create and activate the venv

From the project root:

```bash
python3.11 -m venv .airflow-venv
source .airflow-venv/bin/activate
pip install --upgrade pip
```

### 1.2. Install Airflow 2.9.1 with constraints

```bash
export AIRFLOW_VERSION=2.9.1
export PYTHON_VERSION=3.11
export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

Verify:

```bash
airflow version
# should print: 2.9.1
```

---

## Step 2 – Start a Clean Docker Stack

Now bring up the app + Postgres containers.

From the repo root:

```bash
cd wrds-ma-impact-pipeline

docker-compose -f .devcontainer/docker-compose.yml up -d --build
```

Check containers:

```bash
docker ps
```

You should see something like:

* `ma_project_app` (exposes 8080, 8501)
* `ma_project_db` (Postgres on 5432)

If you get a daemon error, reopen Docker Desktop and run the `docker-compose ... up` command again.

---

## Step 3 – Configure and Initialize Airflow

Now we wire up Airflow to the local `airflow` folder, initialize the metadata DB, and create an admin user.

### 3.1. Activate venv and set `AIRFLOW_HOME`

```bash
source .airflow-venv/bin/activate
export AIRFLOW_HOME=$(pwd)/airflow
```

### 3.2. Initialize the Airflow metadata database

Run once per clean reset:

```bash
airflow db init
```

> If you see an error like:
>
> ```
> Can't locate revision identified by '1949afb29106'
> ```
>
> then delete the old DB and re‑init:
>
> ```bash
> rm -f airflow/airflow.db
> airflow db init
> ```

### 3.3. Create the Airflow admin user

Do this once per DB reset so the UI is immediately usable:

```bash
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin
```

### 3.4. Confirm the DAG is registered

```bash
airflow dags list | grep ma_value_impact_pipeline
```

You should see a row pointing to:

```text
airflow/dags/ma_pipeline_dag.py
```

If not:

* Make sure the file exists at `airflow/dags/ma_pipeline_dag.py`.

---

## Step 4 – Run Airflow (Scheduler + Webserver)

You’ll use **two terminals** for this.

### 4.1. Terminal A – Run the scheduler

From project root:

```bash
source .airflow-venv/bin/activate
export AIRFLOW_HOME=$(pwd)/airflow
airflow scheduler
```

Leave this running.

### 4.2. Terminal B – Run the webserver (use 8081 to avoid conflicts)

Open a new terminal, go to the project root, then:

```bash
source .airflow-venv/bin/activate
export AIRFLOW_HOME=$(pwd)/airflow
airflow webserver -p 8081
```

Now open the UI:

```text
http://localhost:8081
```

Log in with:

* **Username:** `admin`
* **Password:** `admin`

---

## Step 5 – Trigger the DAG (Bronze → Silver → Gold)

In the Airflow UI:

1. Find DAG **`ma_value_impact_pipeline`**.
2. Toggle it **ON** (unpause).
3. Click **▶ Trigger DAG** (top right).
4. Watch **Graph View** to see:

   * `load_bronze`
   * `build_silver`
   * `build_gold`

If tasks get stuck:

* Check the scheduler logs (Terminal A).
* Make sure Docker containers `ma_project_app` and `ma_project_db` are healthy:

```bash
docker ps
```

---

## Step 6 – Verify Gold Layer in Postgres

You can quickly check inside the app container that `gold.final_data` is populated.

```bash
docker exec -it ma_project_app bash
```

Inside the container:

```bash
python - << 'EOF'
from src.utils.db import get_postgres_engine
import pandas as pd

engine = get_postgres_engine()
df = pd.read_sql("SELECT COUNT(*) AS rows FROM gold.final_data", engine)
print(df)
EOF
```

You should see a non‑zero row count.

---

## Step 7 – Launch the Streamlit Dashboard

From your host machine:

```bash
docker exec -it ma_project_app bash
cd /workspace

streamlit run streamlit_app/app.py \
  --server.address=0.0.0.0 \
  --server.port=8501
```

Then open:

```text
http://localhost:8501
```

You should see the M&A Value Impact dashboard using the **Gold** table.

---

## Step 8 – Daily Short Start (No Doom)

If everything is already set up and you just want to run things again on the same machine:

1. **Start Docker stack**

   ```bash
   docker-compose -f .devcontainer/docker-compose.yml up -d
   ```

2. **Start Airflow scheduler (Terminal A)**

   ```bash
   source .airflow-venv/bin/activate
   export AIRFLOW_HOME=$(pwd)/airflow
   airflow scheduler
   ```

3. **Start Airflow webserver (Terminal B)**

   ```bash
   source .airflow-venv/bin/activate
   export AIRFLOW_HOME=$(pwd)/airflow
   airflow webserver -p 8081
   ```

4. **Trigger DAG in UI** → `ma_value_impact_pipeline`.

5. **Start Streamlit**

   ```bash
   docker exec -it ma_project_app bash
   cd /workspace
   streamlit run streamlit_app/app.py \
     --server.address=0.0.0.0 \
     --server.port=8501
   ```