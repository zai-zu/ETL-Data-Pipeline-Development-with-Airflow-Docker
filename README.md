# Driven Data – Local Batch Pipeline  
_A complete, container-based playground that shows how to generate synthetic
network-usage data, model it with **dbt**, orchestrate the flow with **Apache
Airflow**, and store everything in **PostgreSQL**._

---

## 1. What you’ll find here 🚀
| Layer | What lives here | Why it matters |
|-------|-----------------|----------------|
| **Docker** | `docker-compose.yml`, `Dockerfile` | Spins up *seven* services (Airflow webserver, scheduler, worker, triggerer, Redis broker, Postgres, custom image) with **one command**. |
| **Airflow** | `dags/driven_data_pipeline.py` | A DAG that<br>① creates raw schema/table<br>② generates & loads daily CSV batches<br>③ runs dbt *staging* models<br>④ runs dbt *trusted* models. |
| **dbt** | `dbt/` (project, profiles, models) | Six *staging* models and four *trusted* models (payment, technical, PII, non-PII). Tags let Airflow pick which subset to run. |
| **Data generator** | `dags/driven_data_pipeline.py` (Faker + Polars code) | Creates 💯-1 000 synthetic rows per run, masks PII in non-PII layer, appends a UUID, and writes to `data/raw_data.csv`. |

> The project adapts the “Chapter 3: Batch Processing – Local Pipeline ” guided in the course Big Data Engineer-Softserve Academy
> practice from the Driven Path course. 

---

## 2. Prerequisites 📋
* **Docker Desktop / Docker Engine 24 +**
* **Docker Compose v2** (already bundled with Docker Desktop)
* **Git** (to clone this repo)
* **Make** (optional – shortcuts)

---

## 3. Getting started ⚡


### 1 Clone the repository
`git clone https://github.com/zai-zu/ETL-Data-Pipeline-Development-with-Airflow-Docker.git`


### 2 Spin up the entire stack (builds the custom Airflow image on first run)
docker compose up --build -d         # add `-V` to reset volumes from scratch

### 3 Log in to Airflow UI
open http://localhost:8080           # user: airflow  password: airflow

---

## 4. Project structure 🗂️ 
```
src_3/
├─ dags/                # Airflow DAGs
│  └─ driven_data_pipeline.py
├─ dbt/                 # full dbt project
│  ├─ dbt_project.yml
│  ├─ profiles.yml
│  └─ models/
│     ├─ staging_*.sql
│     ├─ trusted_*.sql
│     └─ source.yml     
├─ data/                # raw CSVs generated at runtime (mounted into Postgres)
├─ Dockerfile           # extends apache/airflow:2.10.2 with project deps
├─ docker-compose.yml   # 7-service stack
└─ requirements.txt     # dbt-core, dbt-postgres, faker, polars
```
---
## 5. How the pipeline works 🛠️

1. `extract_raw_data` **( PythonOperator )**
Faker generates a CSV → Polars appends unique_id → file lands in
/opt/airflow/data/raw_data.csv (shared volume).

2. `create_raw_schema` & `create_raw_table` ( SQLExecuteQueryOperator )
Build the landing‐zone table driven_raw.raw_batch_data.

3. `load_raw_data` **( COPY … FROM … )**
Bulk-loads the CSV straight inside Postgres.

4. `run_dbt_staging` **( BashOperator )**
dbt run --select tag:staging creates six dim_/fact_ tables in
driven_staging.

5. `run_dbt_trusted` **( BashOperator )**
`dbt run --select tag:trusted` materialises four business-ready tables in
`driven_trusted`.

_Non-PII & PII_ models reference `dim_person`; keep it fresh if columns
change.

Schedule: daily at 07:00, no backfilling (`catchup=False`).
---
## 6. Accessing the data 🔎

### psql inside the container
`docker exec -it src_3-postgres-1 psql -U airflow -d airflow`

### list schemas
airflow=# \dn
### list trusted tables
airflow=# \dt driven_trusted.*
Or connect via pgAdmin 4
`Host: localhost | Port: 5432 | DB: airflow | User/Pass: airflow`
---
## 7. Common commands 🧰

| What                       | Command                                    |
| -------------------------- | ------------------------------------------ |
| Re-run only staging models | `dbt run -s tag:staging --full-refresh`    |
| Re-run a single model      | `dbt run -s staging_dim_person`            |
| Tail Airflow logs          | `docker compose logs -f airflow-scheduler` |
| Stop everything            | `docker compose down`                      |
| Stop & delete volumes      | `docker compose down -v`                   |

---
## 8. Troubleshooting 🩹
- `column … does not exist` during dbt run
→ Run `dbt run -s staging_dim_person --full-refresh` first, then trusted
models – they rely on the latest column names.

- `could not translate host name "postgres"` inside Airflow tasks
→ Confirm the `postgres` service has the `network.aliases: - postgres` entry
in `docker-compose.yml` and is healthy.

- Password auth fails when using psql on host
→ Use the container port (`localhost:5432`, user/pass `airflow`) and
ensure no local Postgres is already listening on 5432.
---
## 9. Contributing 🤝
Pull requests are welcome! Please open an issue first to discuss your
proposed change. When updating DAG code, run `pre-commit run --all-files`
---