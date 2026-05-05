"""
FinTech Fraud Detection Pipeline
=================================
Airflow DAG: fraud_etl_pipeline

Schedule: Every 6 hours
Tasks:
  1. extract_transactions    — Pull validated records from PostgreSQL
  2. export_to_parquet       — Write to Parquet (Data Warehouse)
  3. reconciliation_report   — Compare total ingress vs validated amounts
  4. fraud_analysis_report   — Breakdown of fraud by merchant category

This represents the BATCH layer of the Lambda architecture.
"""

from __future__ import annotations

import os
import json
import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import psycopg2
import psycopg2.extras

# ─── Config ──────────────────────────────────────────────────────────────────
PG_CONN = {
    "host":     os.getenv("PG_HOST",   "postgres"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "dbname":   os.getenv("PG_DB",     "fraud_db"),
    "user":     os.getenv("PG_USER",   "fraud_user"),
    "password": os.getenv("PG_PASS",   "fraud_pass"),
}

PARQUET_OUTPUT_DIR  = "/opt/airflow/reports/parquet"
REPORTS_DIR         = "/opt/airflow/reports"

os.makedirs(PARQUET_OUTPUT_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR,        exist_ok=True)

log = logging.getLogger("fraud_etl")


# ─── Helper ───────────────────────────────────────────────────────────────────
def get_connection():
    return psycopg2.connect(**PG_CONN)


def window_label(execution_date: datetime) -> str:
    """Return a human-readable 6-hour window label."""
    end   = execution_date
    start = end - timedelta(hours=6)
    return f"{start.strftime('%Y-%m-%d %H:%M')} → {end.strftime('%Y-%m-%d %H:%M')} UTC"


# ─── Task 1: Extract validated transactions ───────────────────────────────────
def extract_transactions(**context):
    """
    Pull valid_transactions and fraud_alerts from the last 6-hour window.
    Pushes summary stats to XCom for downstream tasks.
    """
    execution_date = context["execution_date"]
    window_start   = execution_date - timedelta(hours=6)
    window_end     = execution_date

    conn = get_connection()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Valid transactions in window
    cur.execute("""
        SELECT user_id, timestamp, merchant_category, amount, location, processed_at
        FROM   valid_transactions
        WHERE  processed_at BETWEEN %s AND %s
        ORDER  BY timestamp
    """, (window_start, window_end))
    valid_rows = cur.fetchall()

    # Fraud alerts in window
    cur.execute("""
        SELECT user_id, timestamp, merchant_category, amount, location,
               fraud_reason, flagged_at
        FROM   fraud_alerts
        WHERE  flagged_at BETWEEN %s AND %s
        ORDER  BY timestamp
    """, (window_start, window_end))
    fraud_rows = cur.fetchall()

    # All raw transactions in window
    cur.execute("""
        SELECT SUM(amount) AS total_ingress, COUNT(*) AS count
        FROM   transactions
        WHERE  ingested_at BETWEEN %s AND %s
    """, (window_start, window_end))
    totals = cur.fetchone()

    cur.close()
    conn.close()

    summary = {
        "window":          window_label(execution_date),
        "valid_count":     len(valid_rows),
        "fraud_count":     len(fraud_rows),
        "total_ingress":   float(totals["total_ingress"] or 0),
        "valid_amount":    sum(float(r["amount"]) for r in valid_rows),
        "fraud_amount":    sum(float(r["amount"]) for r in fraud_rows),
        "valid_rows":      [dict(r) for r in valid_rows],
        "fraud_rows":      [dict(r) for r in fraud_rows],
    }

    log.info(
        "Extracted | window=%s | valid=%d | fraud=%d | ingress=$%.2f",
        summary["window"], summary["valid_count"],
        summary["fraud_count"], summary["total_ingress"]
    )

    # Push to XCom so downstream tasks can access
    context["ti"].xcom_push(key="etl_summary", value=json.dumps(summary, default=str))


# ─── Task 2: Export to Parquet ────────────────────────────────────────────────
def export_to_parquet(**context):
    """
    Convert the extracted valid transactions to Parquet format (Data Warehouse sink).
    File is partitioned by date for efficient future querying.
    """
    ti      = context["ti"]
    summary = json.loads(ti.xcom_pull(key="etl_summary", task_ids="extract_transactions"))

    valid_rows = summary["valid_rows"]
    if not valid_rows:
        log.info("No valid transactions to export — skipping Parquet write")
        return

    df = pd.DataFrame(valid_rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Partition by date
    execution_date = context["execution_date"]
    date_str       = execution_date.strftime("%Y-%m-%d")
    hour_str       = execution_date.strftime("%H")
    out_path       = f"{PARQUET_OUTPUT_DIR}/date={date_str}/hour={hour_str}/valid_txns.parquet"

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_parquet(out_path, index=False, engine="pyarrow")

    log.info(
        "Parquet written | rows=%d | path=%s | size=%.1f KB",
        len(df), out_path, os.path.getsize(out_path) / 1024
    )

    context["ti"].xcom_push(key="parquet_path", value=out_path)


# ─── Task 3: Reconciliation Report ───────────────────────────────────────────
def reconciliation_report(**context):
    """
    Generate a reconciliation report comparing:
      - Total transaction ingress amount
      - Total validated (non-fraud) amount
      - Total fraud amount

    Saves as CSV + writes summary row to reconciliation_reports table.
    """
    ti      = context["ti"]
    summary = json.loads(ti.xcom_pull(key="etl_summary", task_ids="extract_transactions"))

    execution_date  = context["execution_date"]
    window          = summary["window"]
    total_ingress   = summary["total_ingress"]
    valid_amount    = summary["valid_amount"]
    fraud_amount    = summary["fraud_amount"]
    valid_count     = summary["valid_count"]
    fraud_count     = summary["fraud_count"]
    total_count     = valid_count + fraud_count

    # Percentages
    fraud_rate = (fraud_count / total_count * 100) if total_count > 0 else 0
    leak_pct   = (fraud_amount / total_ingress * 100) if total_ingress > 0 else 0

    report_lines = [
        "=" * 60,
        "   FRAUD DETECTION PIPELINE — RECONCILIATION REPORT",
        "=" * 60,
        f"  Window          : {window}",
        f"  Generated at    : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "",
        "  TRANSACTION VOLUMES",
        f"  Total ingress   : ${total_ingress:>12,.2f}  ({total_count:,} txns)",
        f"  Validated       : ${valid_amount:>12,.2f}  ({valid_count:,} txns)",
        f"  Flagged fraud   : ${fraud_amount:>12,.2f}  ({fraud_count:,} txns)",
        "",
        "  RISK METRICS",
        f"  Fraud rate      : {fraud_rate:.2f}% of transactions",
        f"  Value at risk   : {leak_pct:.2f}% of total ingress",
        "=" * 60,
    ]

    report_text = "\n".join(report_lines)
    print(report_text)   # visible in Airflow task logs

    # Save as text file
    date_str     = execution_date.strftime("%Y-%m-%d_%H%M")
    report_path  = f"{REPORTS_DIR}/reconciliation_{date_str}.txt"
    with open(report_path, "w") as f:
        f.write(report_text)

    # Save as CSV for further analysis
    csv_data = {
        "window":              [window],
        "total_ingress_usd":   [total_ingress],
        "valid_amount_usd":    [valid_amount],
        "fraud_amount_usd":    [fraud_amount],
        "total_txn_count":     [total_count],
        "valid_txn_count":     [valid_count],
        "fraud_txn_count":     [fraud_count],
        "fraud_rate_pct":      [round(fraud_rate, 4)],
        "value_at_risk_pct":   [round(leak_pct, 4)],
    }
    csv_path = f"{REPORTS_DIR}/reconciliation_{date_str}.csv"
    pd.DataFrame(csv_data).to_csv(csv_path, index=False)

    # Persist to DB
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO reconciliation_reports
            (report_period, total_ingress, total_validated,
             total_fraud, transaction_count, fraud_count)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (window, total_ingress, valid_amount, fraud_amount, total_count, fraud_count))
    conn.commit()
    cur.close()
    conn.close()

    log.info("Reconciliation report saved: %s", report_path)


# ─── Task 4: Fraud Analysis Report ───────────────────────────────────────────
def fraud_analysis_report(**context):
    """
    Analyse fraud attempts grouped by merchant category.
    This is the primary analytic deliverable required by the brief.
    """
    ti      = context["ti"]
    summary = json.loads(ti.xcom_pull(key="etl_summary", task_ids="extract_transactions"))

    fraud_rows = summary["fraud_rows"]

    if not fraud_rows:
        log.info("No fraud records for analysis in this window")
        return

    df = pd.DataFrame(fraud_rows)
    df["amount"] = df["amount"].astype(float)

    # Group by merchant category
    analysis = (
        df.groupby("merchant_category")
        .agg(
            fraud_count=("amount",  "count"),
            total_fraud_amount=("amount", "sum"),
            avg_fraud_amount=("amount",   "mean"),
            max_fraud_amount=("amount",   "max"),
        )
        .reset_index()
        .sort_values("fraud_count", ascending=False)
    )

    # Fraud by reason
    reason_breakdown = (
        df.groupby("fraud_reason")
        .agg(count=("amount", "count"), total=("amount", "sum"))
        .reset_index()
    )

    # Print summary table
    print("\n" + "=" * 70)
    print("   FRAUD ANALYSIS — BY MERCHANT CATEGORY")
    print("=" * 70)
    print(analysis.to_string(index=False))
    print("\n" + "─" * 70)
    print("   FRAUD BY RULE TYPE")
    print("─" * 70)
    print(reason_breakdown.to_string(index=False))
    print("=" * 70)

    # Save CSVs
    execution_date = context["execution_date"]
    date_str       = execution_date.strftime("%Y-%m-%d_%H%M")

    analysis_path        = f"{REPORTS_DIR}/fraud_by_category_{date_str}.csv"
    reason_path          = f"{REPORTS_DIR}/fraud_by_reason_{date_str}.csv"
    analysis.to_csv(analysis_path,         index=False)
    reason_breakdown.to_csv(reason_path,   index=False)

    log.info("Fraud analysis saved: %s", analysis_path)
    log.info("Fraud reason summary: %s", reason_path)


# ─── DAG Definition ──────────────────────────────────────────────────────────
default_args = {
    "owner":            "fraud_team",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

with DAG(
    dag_id="fraud_etl_pipeline",
    description="FinTech Fraud Detection — ETL every 6 hours + reconciliation report",
    schedule_interval="0 */6 * * *",   # every 6 hours: 00:00, 06:00, 12:00, 18:00
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["fraud", "etl", "fintech"],
) as dag:

    t1_extract = PythonOperator(
        task_id="extract_transactions",
        python_callable=extract_transactions,
    )

    t2_parquet = PythonOperator(
        task_id="export_to_parquet",
        python_callable=export_to_parquet,
    )

    t3_recon = PythonOperator(
        task_id="reconciliation_report",
        python_callable=reconciliation_report,
    )

    t4_fraud_analysis = PythonOperator(
        task_id="fraud_analysis_report",
        python_callable=fraud_analysis_report,
    )

    # Task dependencies: extract → parquet & recon & analysis (parallel)
    t1_extract >> [t2_parquet, t3_recon, t4_fraud_analysis]
