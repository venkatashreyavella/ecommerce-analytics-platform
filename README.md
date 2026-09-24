# End-to-End E-Commerce Analytics Platform

A local, production-shaped hybrid Kappa/Lambda platform:

- **Real time:** Kafka receives clickstream events; the stream processor maintains five-minute event-type aggregates in PostgreSQL.
- **Batch:** The generator writes users, products, and orders to `raw_data/` as JSONL landing files.
- **Warehouse:** dbt builds staging views, conformed dimensions, and a transaction fact table in PostgreSQL.
- **Orchestration and BI:** Airflow runs the daily load and dbt test workflow; Superset provides a local dashboarding UI.

## Prerequisites

- Docker Desktop with Docker Compose v2
- At least 8 GB RAM allocated to Docker
- Ports `5432`, `8080`, `8088`, and `9092` available

### Windows first-time setup

Docker Desktop is already installed if `C:\Program Files\Docker\Docker\resources\bin\docker.exe` exists, but Windows still needs WSL 2 and Virtual Machine Platform enabled. Open **PowerShell as Administrator**, run:

```powershell
cd "C:\Users\User\Desktop\End-to-End Real-Time & Batch E-Commerce Analytics Platform"
Set-ExecutionPolicy -Scope Process Bypass
.\setup-windows.ps1
```

If the script enables Windows features and asks for a restart, restart Windows, open Administrator PowerShell again, and run the script again. In Docker Desktop, enable **Settings > General > Use the WSL 2 based engine** before rerunning it.

For a normal non-administrator terminal after setup, add Docker to the current session if `docker` is not recognized:

```powershell
$env:Path = "C:\Program Files\Docker\Docker\resources\bin;$env:Path"
```

## Start the platform

From the repository root:

```bash
docker compose up -d --build
```

Check service health and logs:

```bash
docker compose ps
docker compose logs -f data-generator stream-processor
```

The first startup builds the Airflow and Python images and initializes the Postgres schemas. Airflow may take a few minutes to become healthy.

## Verify Kafka

List topics from the Kafka container:

```bash
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list
```

Read a few clickstream messages:

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic ecommerce_clicks \
  --from-beginning \
  --max-messages 5
```

The generator writes batch files locally as well:

```bash
ls raw_data
```

On PowerShell, use `Get-ChildItem raw_data`.

## Trigger Airflow

Open [http://localhost:8080](http://localhost:8080), then sign in with:

- Username: `admin`
- Password: `admin`

Enable and trigger `ecommerce_batch_daily` from the DAG page. The DAG checks `raw_data`, loads JSONL into `raw.*`, runs `dbt run`, and then runs `dbt test`.

The same workflow can be triggered from the command line:

```bash
docker compose exec airflow-scheduler airflow dags trigger ecommerce_batch_daily
```

## Verify PostgreSQL

Inspect raw, real-time, and modeled tables:

```bash
docker compose exec postgres psql -U warehouse -d warehouse -c "\\dt raw.*"
docker compose exec postgres psql -U warehouse -d warehouse -c "SELECT * FROM analytics.realtime_click_aggregates ORDER BY window_end DESC LIMIT 10;"
docker compose exec postgres psql -U warehouse -d warehouse -c "SELECT COUNT(*) FROM analytics.fact_ecommerce_transactions;"
```

The stream processor should begin populating `analytics.realtime_click_aggregates` as soon as Kafka receives events.

## Connect Superset

Open [http://localhost:8088](http://localhost:8088), then sign in with `admin` / `admin`.

Add a PostgreSQL database connection in **Settings > Database Connections**:

```text
postgresql+psycopg2://warehouse:warehouse@postgres:5432/warehouse
```

Create datasets from `analytics.dim_users`, `analytics.dim_products`, `analytics.fact_ecommerce_transactions`, and `analytics.realtime_click_aggregates`.

## Useful operations

```bash
# Stop services but retain Kafka/Postgres/Superset volumes
docker compose down

# Stop services and delete all persistent volumes
docker compose down -v

# Follow Airflow scheduler logs
docker compose logs -f airflow-scheduler

# Run dbt commands manually
docker compose exec airflow-scheduler bash -lc \
  'cd /opt/airflow/dbt_project && dbt run --profiles-dir . && dbt test --profiles-dir .'
```

## Local production notes

This stack is intentionally single-node for development. Before production, use managed Kafka/Postgres, external secret management, TLS/SASL for Kafka, non-default credentials, separate Airflow metadata storage, object storage for immutable landing data, and a multi-worker execution strategy.
