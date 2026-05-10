
from __future__ import annotations
import os, json, logging
from datetime import datetime, timedelta, timezone
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import psycopg2, psycopg2.extras

PG_CONN = {"host":"postgres","port":5432,"dbname":"fraud_db","user":"postgres","password":"root"}
REPORTS_DIR = "/opt/airflow/reports"
os.makedirs(REPORTS_DIR, exist_ok=True)
log = logging.getLogger("fraud_etl")

def get_connection():
    return psycopg2.connect(**PG_CONN)

def extract_transactions(**context):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("SELECT user_id, timestamp, merchant_category, amount, location, processed_at FROM valid_transactions ORDER BY timestamp DESC LIMIT 5000")
    valid_rows = cur.fetchall()

    cur.execute("SELECT user_id, timestamp, merchant_category, amount, location, fraud_reason, flagged_at FROM fraud_alerts ORDER BY timestamp DESC LIMIT 5000")
    fraud_rows = cur.fetchall()

    cur.execute("SELECT COALESCE(SUM(amount),0) AS total_ingress, COUNT(*) AS cnt FROM transactions")
    totals = cur.fetchone()

    cur.close()
    conn.close()

    summary = {
        "window": "All available data",
        "valid_count": len(valid_rows),
        "fraud_count": len(fraud_rows),
        "total_ingress": float(totals["total_ingress"]),
        "valid_amount": sum(float(r["amount"]) for r in valid_rows),
        "fraud_amount": sum(float(r["amount"]) for r in fraud_rows),
        "valid_rows": [dict(r) for r in valid_rows],
        "fraud_rows": [dict(r) for r in fraud_rows],
    }

    log.info("Extracted valid=%d fraud=%d ingress=%.2f",
             summary["valid_count"], summary["fraud_count"], summary["total_ingress"])
    context["ti"].xcom_push(key="etl_summary", value=json.dumps(summary, default=str))


def export_to_parquet(**context):
    ti = context["ti"]
    summary = json.loads(ti.xcom_pull(key="etl_summary", task_ids="extract_transactions"))
    valid_rows = summary["valid_rows"]
    if not valid_rows:
        log.info("No valid transactions to export")
        return
    df = pd.DataFrame(valid_rows)
    out_path = f"{REPORTS_DIR}/valid_transactions.parquet"
    df.to_parquet(out_path, index=False, engine="pyarrow")
    log.info("Parquet written rows=%d path=%s", len(df), out_path)


def reconciliation_report(**context):
    ti = context["ti"]
    summary = json.loads(ti.xcom_pull(key="etl_summary", task_ids="extract_transactions"))

    total_ingress = summary["total_ingress"]
    valid_amount  = summary["valid_amount"]
    fraud_amount  = summary["fraud_amount"]
    valid_count   = summary["valid_count"]
    fraud_count   = summary["fraud_count"]
    total_count   = valid_count + fraud_count
    fraud_rate    = (fraud_count / total_count * 100) if total_count > 0 else 0

    lines = [
        "=" * 55,
        "   FRAUD DETECTION PIPELINE - RECONCILIATION REPORT",
        "=" * 55,
        f"  Generated : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"  Window    : All available data",
        "",
        f"  Total Ingress  : ${total_ingress:>12,.2f}  ({total_count:,} txns)",
        f"  Validated      : ${valid_amount:>12,.2f}  ({valid_count:,} txns)",
        f"  Fraud Flagged  : ${fraud_amount:>12,.2f}  ({fraud_count:,} txns)",
        f"  Fraud Rate     : {fraud_rate:.2f}%",
        "=" * 55,
    ]
    report_text = "\n".join(lines)
    print(report_text)

    with open(f"{REPORTS_DIR}/reconciliation_report.txt", "w") as f:
        f.write(report_text)

    pd.DataFrame({
        "metric": ["total_ingress", "validated", "fraud"],
        "value":  [total_ingress, valid_amount, fraud_amount],
        "count":  [total_count, valid_count, fraud_count],
    }).to_csv(f"{REPORTS_DIR}/reconciliation_report.csv", index=False)

    log.info("Reconciliation report saved")


def fraud_analysis_report(**context):
    ti = context["ti"]
    summary = json.loads(ti.xcom_pull(key="etl_summary", task_ids="extract_transactions"))
    fraud_rows = summary["fraud_rows"]

    if not fraud_rows:
        log.info("No fraud records for analysis")
        return

    df = pd.DataFrame(fraud_rows)
    df["amount"] = df["amount"].astype(float)

    analysis = (
        df.groupby("merchant_category")
        .agg(
            fraud_count=("amount", "count"),
            total_fraud_amount=("amount", "sum"),
            avg_fraud_amount=("amount", "mean"),
        )
        .reset_index()
        .sort_values("fraud_count", ascending=False)
    )

    reason = (
        df.groupby("fraud_reason")
        .agg(count=("amount", "count"), total=("amount", "sum"))
        .reset_index()
    )

    print("\nFRAUD BY MERCHANT CATEGORY")
    print(analysis.to_string(index=False))
    print("\nFRAUD BY RULE TYPE")
    print(reason.to_string(index=False))

    analysis.to_csv(f"{REPORTS_DIR}/fraud_by_category.csv", index=False)
    reason.to_csv(f"{REPORTS_DIR}/fraud_by_reason.csv", index=False)
    log.info("Fraud analysis reports saved")


default_args = {
    "owner": "fraud_team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="fraud_etl_pipeline",
    description="FinTech Fraud Detection — ETL + reconciliation report",
    schedule_interval="0 */6 * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["fraud", "etl", "fintech"],
) as dag:

    t1 = PythonOperator(task_id="extract_transactions",  python_callable=extract_transactions)
    t2 = PythonOperator(task_id="export_to_parquet",     python_callable=export_to_parquet)
    t3 = PythonOperator(task_id="reconciliation_report", python_callable=reconciliation_report)
    t4 = PythonOperator(task_id="fraud_analysis_report", python_callable=fraud_analysis_report)

    t1 >> [t2, t3, t4]