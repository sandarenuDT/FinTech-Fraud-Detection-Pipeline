# FinTech Fraud Detection Pipeline
### EC8207 Applied Big Data Engineering — Mini Project

---

## Architecture

```
Python Producer → Kafka (transactions topic)
                        ↓
               Spark Structured Streaming
                  ├─ Rule 1: Impossible Travel → fraud_alerts (PostgreSQL)
                  ├─ Rule 2: High Value > $5000 → fraud_alerts (PostgreSQL)
                  └─ Clean transactions        → valid_transactions (PostgreSQL)
                                                        ↓
                                             Apache Airflow (every 6h)
                                              ├─ Export → Parquet (DW)
                                              ├─ Reconciliation Report
                                              └─ Fraud Analysis Report
```

---

## Tech Stack Justification

| Component | Tool | Why |
|---|---|---|
| Ingestion | Apache Kafka | Durable, partitioned, high-throughput message queue. Decouples producer from consumer. |
| Stream Processing | Apache Spark Structured Streaming | Stateful processing, watermarking for event-time, built-in Kafka connector |
| Orchestration | Apache Airflow | DAG-based scheduling, retries, task logging, visual UI |
| Storage (hot) | PostgreSQL | ACID compliance for fraud alerts, immediate queryability |
| Storage (cold) | Parquet files | Columnar format optimised for analytical queries |

---

## Prerequisites

- Docker Desktop installed and running
- Docker Compose v2+
- ~6GB RAM allocated to Docker

---

## Quick Start

### Step 1 — Start all services

```bash
docker-compose up -d
```

Wait ~60 seconds for all services to initialise.

### Step 2 — Verify services are running

| Service | URL |
|---|---|
| Kafka UI | http://localhost:8090 |
| Spark Master UI | http://localhost:8080 |
| Airflow UI | http://localhost:8081 (admin / admin) |

### Step 3 — Start the transaction producer

```bash
# Run inside the producer container (already starts via docker-compose)
# Or run locally:
cd producer
pip install -r requirements.txt
python producer.py --rate 1 --duration 300   # 1 msg/sec for 5 minutes
```

Producer log format:
```
10:23:41 | INFO     | [NORMAL] user_012   |   $  45.20 | grocery             | Colombo_LK
10:23:45 | WARNING  | ⚠  [FRAUD_INJECT] Impossible travel | user_007 | London_UK → New_York_US | $320.00
10:23:48 | WARNING  | ⚠  [FRAUD_INJECT] High-value transaction | user_031 | $12450.00 | electronics
```

### Step 4 — Run the Spark fraud detector

```bash
# Copy the spark job into the spark-master container
docker cp spark/spark_fraud_detector.py spark-master:/opt/bitnami/spark/work/

# Execute the spark job
docker exec -it spark-master bash /opt/bitnami/spark/submit_job.sh
```

### Step 5 — Monitor fraud detection (real-time)

```bash
# Watch fraud_alerts table grow in real-time
docker exec -it postgres psql -U fraud_user -d fraud_db \
  -c "SELECT fraud_reason, user_id, amount, location, flagged_at FROM fraud_alerts ORDER BY flagged_at DESC LIMIT 10;"
```

### Step 6 — Trigger Airflow ETL (or wait for schedule)

1. Open http://localhost:8081
2. Login: admin / admin
3. Find DAG `fraud_etl_pipeline` → toggle ON → click ▶ Run

Or trigger manually via CLI:
```bash
docker exec -it airflow airflow dags trigger fraud_etl_pipeline
```

### Step 7 — Generate the final analytic report

```bash
python airflow/scripts/generate_report.py
```

Reports are saved to `./reports/`:
- `fraud_by_merchant_category.csv`
- `fraud_by_rule.csv`
- `fraud_timeline.csv`
- `pipeline_summary.csv`

---

## Event Time vs Processing Time

**Event Time** is the timestamp embedded in the transaction JSON — when the
transaction actually occurred at the merchant terminal.

**Processing Time** is when Spark receives and processes the message from Kafka —
always later due to network and queue delays.

This pipeline uses **event time** for fraud detection logic by:
1. Parsing the `timestamp` field from the transaction JSON
2. Applying a 2-minute **watermark** via `.withWatermark("event_ts", "2 minutes")`
3. This allows Spark to handle late-arriving events (e.g. a transaction queued
   offline then sent) without holding state indefinitely

The impossible travel rule compares event timestamps, not processing timestamps —
ensuring a transaction that took 5 minutes to reach Kafka is still correctly
evaluated against the original 10-minute window.

---

## Fraud Detection Rules

### Rule 1 — Impossible Travel
```
Same user_id appears in transactions from two different countries
within a 10-minute event-time window.
```
Detected by a self-join on `user_id` where countries differ and
`|timestamp_a - timestamp_b| < 600 seconds`.

### Rule 2 — High Value
```
Transaction amount > $5,000 USD
```
Simple filter applied to every record before writing.

---

## Ethics & Privacy

### Privacy Implications
Transaction data is highly sensitive — it reveals spending habits, physical
location at specific times, health conditions (healthcare merchants), and
financial vulnerability.

### Data Governance Measures Applied
- **Data minimisation**: Only `user_id` (pseudonym, not real name) is stored;
  no PII like card numbers or names
- **Encryption at rest**: PostgreSQL volume should use encrypted storage in
  production (pgcrypto extension)
- **Access control**: Fraud alerts table should be restricted to security team
  only (GRANT/REVOKE on PostgreSQL roles)
- **Retention limits**: Raw transaction logs should be purged after 90 days
  per GDPR Article 5(1)(e) storage limitation principle
- **Audit logging**: All queries to fraud_alerts should be logged via
  PostgreSQL `pg_audit` extension
- **Right to erasure**: A data deletion procedure should exist to purge
  all records for a given `user_id` on request

---

## Project Team

| Member | Responsibility |
|---|---|
| Member 1 | Producer, Kafka, Docker Compose |
| Member 2 | Spark Structured Streaming, fraud logic |
| Member 3 | Airflow DAG, reports, project report |

---

## Troubleshooting

**Kafka not ready**: Wait 30 seconds after `docker-compose up`, then retry.

**Spark job fails on packages**: First run downloads JARs — can take 2-3 minutes.

**Airflow DB error**: Run `docker exec -it airflow airflow db init` manually.

**No data in PostgreSQL**: Check producer logs — ensure it says "Connected to Kafka".
