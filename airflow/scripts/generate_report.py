"""
FinTech Fraud Detection Pipeline
=================================
Report Generator

Queries PostgreSQL and generates the final analytic report:
  - Fraud Attempts by Merchant Category (CSV + printed table)
  - Reconciliation summary

Run this AFTER the pipeline has processed some data:
    python generate_report.py
"""

import os
import pandas as pd
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone

PG_CONN = {
    "host":     os.getenv("PG_HOST",   "localhost"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "dbname":   os.getenv("PG_DB",     "fraud_db"),
    "user":     os.getenv("PG_USER",   "fraud_user"),
    "password": os.getenv("PG_PASS",   "fraud_pass"),
}

OUTPUT_DIR = "./reports"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def main():
    conn = psycopg2.connect(**PG_CONN)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # ── 1. Fraud by merchant category ──────────────────────────────────────
    cur.execute("""
        SELECT
            merchant_category,
            COUNT(*)                    AS fraud_attempts,
            SUM(amount)                 AS total_fraud_amount,
            AVG(amount)                 AS avg_fraud_amount,
            MAX(amount)                 AS max_single_fraud,
            COUNT(DISTINCT user_id)     AS unique_users_targeted
        FROM   fraud_alerts
        GROUP  BY merchant_category
        ORDER  BY fraud_attempts DESC
    """)
    category_rows = cur.fetchall()

    # ── 2. Fraud by rule type ──────────────────────────────────────────────
    cur.execute("""
        SELECT
            fraud_reason,
            COUNT(*)        AS count,
            SUM(amount)     AS total_amount,
            AVG(amount)     AS avg_amount
        FROM   fraud_alerts
        GROUP  BY fraud_reason
        ORDER  BY count DESC
    """)
    reason_rows = cur.fetchall()

    # ── 3. Timeline: fraud vs valid per hour ───────────────────────────────
    cur.execute("""
        SELECT
            DATE_TRUNC('hour', flagged_at) AS hour,
            COUNT(*)                        AS fraud_count,
            SUM(amount)                     AS fraud_amount
        FROM   fraud_alerts
        GROUP  BY hour
        ORDER  BY hour
    """)
    timeline_rows = cur.fetchall()

    # ── 4. Overall stats ───────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*), COALESCE(SUM(amount),0) FROM transactions")
    total_txn, total_ingress = cur.fetchone().values()

    cur.execute("SELECT COUNT(*), COALESCE(SUM(amount),0) FROM valid_transactions")
    valid_txn, valid_amount = cur.fetchone().values()

    cur.execute("SELECT COUNT(*), COALESCE(SUM(amount),0) FROM fraud_alerts")
    fraud_txn, fraud_amount = cur.fetchone().values()

    cur.close()
    conn.close()

    # ── Build DataFrames ───────────────────────────────────────────────────
    df_category = pd.DataFrame(category_rows)
    df_reason   = pd.DataFrame(reason_rows)
    df_timeline = pd.DataFrame(timeline_rows)

    # ── Print report ───────────────────────────────────────────────────────
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    print()
    print("╔" + "═" * 68 + "╗")
    print("║  FINTECH FRAUD DETECTION PIPELINE — ANALYTIC REPORT" + " " * 16 + "║")
    print(f"║  Generated: {generated_at}" + " " * (68 - 13 - len(generated_at)) + "║")
    print("╠" + "═" * 68 + "╣")
    print("║  PIPELINE SUMMARY" + " " * 51 + "║")
    print(f"║  Total transactions ingested : {int(total_txn or 0):,}".ljust(70) + "║")
    print(f"║  Total ingress amount        : ${float(total_ingress or 0):>12,.2f}".ljust(70) + "║")
    print(f"║  Validated (clean)           : {int(valid_txn or 0):,}  (${float(valid_amount or 0):,.2f})".ljust(70) + "║")
    print(f"║  Fraud flagged               : {int(fraud_txn or 0):,}  (${float(fraud_amount or 0):,.2f})".ljust(70) + "║")
    if total_txn and int(total_txn) > 0:
        fraud_rate = int(fraud_txn or 0) / int(total_txn) * 100
        print(f"║  Overall fraud rate          : {fraud_rate:.2f}%".ljust(70) + "║")
    print("╚" + "═" * 68 + "╝")

    if not df_category.empty:
        print("\n  FRAUD ATTEMPTS BY MERCHANT CATEGORY")
        print("  " + "─" * 66)
        df_category["total_fraud_amount"]  = df_category["total_fraud_amount"].apply(lambda x: f"${float(x):,.2f}")
        df_category["avg_fraud_amount"]    = df_category["avg_fraud_amount"].apply(lambda x: f"${float(x):,.2f}")
        df_category["max_single_fraud"]    = df_category["max_single_fraud"].apply(lambda x: f"${float(x):,.2f}")
        print(df_category.to_string(index=False))

    if not df_reason.empty:
        print("\n  FRAUD BREAKDOWN BY DETECTION RULE")
        print("  " + "─" * 66)
        df_reason["total_amount"] = df_reason["total_amount"].apply(lambda x: f"${float(x):,.2f}")
        df_reason["avg_amount"]   = df_reason["avg_amount"].apply(lambda x: f"${float(x):,.2f}")
        print(df_reason.to_string(index=False))

    # ── Save CSVs ──────────────────────────────────────────────────────────
    if not df_category.empty:
        path = f"{OUTPUT_DIR}/fraud_by_merchant_category.csv"
        pd.DataFrame(category_rows).to_csv(path, index=False)
        print(f"\n  ✓  Saved: {path}")

    if not df_reason.empty:
        path = f"{OUTPUT_DIR}/fraud_by_rule.csv"
        pd.DataFrame(reason_rows).to_csv(path, index=False)
        print(f"  ✓  Saved: {path}")

    if not df_timeline.empty:
        path = f"{OUTPUT_DIR}/fraud_timeline.csv"
        pd.DataFrame(timeline_rows).to_csv(path, index=False)
        print(f"  ✓  Saved: {path}")

    # Overall summary CSV
    summary = {
        "metric":  ["total_ingress_usd", "validated_usd", "fraud_usd",
                    "total_transactions", "valid_transactions", "fraud_transactions"],
        "value":   [float(total_ingress or 0), float(valid_amount or 0),
                    float(fraud_amount or 0), int(total_txn or 0),
                    int(valid_txn or 0), int(fraud_txn or 0)],
    }
    path = f"{OUTPUT_DIR}/pipeline_summary.csv"
    pd.DataFrame(summary).to_csv(path, index=False)
    print(f"  ✓  Saved: {path}")
    print()


if __name__ == "__main__":
    main()
