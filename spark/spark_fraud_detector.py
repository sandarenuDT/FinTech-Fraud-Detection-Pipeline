"""
FinTech Fraud Detection Pipeline
=================================
Spark Structured Streaming Job

Reads from Kafka topic 'transactions', applies two fraud detection rules,
and writes results to PostgreSQL in real-time.

Fraud Rules:
  Rule 1 — Impossible Travel:
    Same user transacts from two DIFFERENT countries within 10 minutes.

  Rule 2 — High Value:
    Transaction amount > $5000.

Clean (non-fraud) transactions are written to the valid_transactions table.

Run:
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,\
                   org.postgresql:postgresql:42.6.0 \
        spark_fraud_detector.py
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from pyspark.sql.window import Window

# ─── Config ──────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC       = "transactions"
PG_URL            = os.getenv("PG_URL", "jdbc:postgresql://postgres:5432/fraud_db")
PG_USER           = os.getenv("PG_USER", "postgres")
PG_PASS           = os.getenv("PG_PASS", "root")
CHECKPOINT_DIR    = "/tmp/checkpoints"
HIGH_VALUE_LIMIT  = 5000.0
TRAVEL_WINDOW_MIN = 10        # minutes for impossible travel check

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger("spark_fraud")

# ─── Schema of incoming JSON ──────────────────────────────────────────────────
TXN_SCHEMA = StructType([
    StructField("user_id",           StringType(),    True),
    StructField("timestamp",         StringType(),    True),   # ISO string → cast later
    StructField("merchant_category", StringType(),    True),
    StructField("amount",            DoubleType(),    True),
    StructField("location",          StringType(),    True),
])

# ─── JDBC write helper ────────────────────────────────────────────────────────
def write_to_postgres(df, table: str, mode: str = "append"):
    """Write a static DataFrame batch to PostgreSQL via JDBC."""
    (df.write
       .format("jdbc")
       .option("url",      PG_URL)
       .option("dbtable",  table)
       .option("user",     PG_USER)
       .option("password", PG_PASS)
       .option("driver",   "org.postgresql.Driver")
       .mode(mode)
       .save())


# ─── Batch processing function (called per micro-batch) ───────────────────────
def process_batch(batch_df, batch_id: int):
    """
    Process each micro-batch:
    1. Extract country from location field
    2. Detect impossible travel using session window state
    3. Detect high-value transactions
    4. Write fraud records to fraud_alerts table
    5. Write clean records to valid_transactions table
    """
    if batch_df.isEmpty():
        return

    log.info("─── Batch %d | %d records ───", batch_id, batch_df.count())

    # ── Parse timestamp + extract country ──────────────────────────────────
    enriched = (
        batch_df
        .withColumn("event_ts",
                    F.to_timestamp(F.col("timestamp")))
        .withColumn("country",
                    F.regexp_extract(F.col("location"), r"_([A-Z]{2})$", 1))
        # Add processing time for audit
        .withColumn("processing_ts", F.current_timestamp())
    )

    # ── Rule 2: High-value detection (simple filter) ───────────────────────
    high_value = (
        enriched
        .filter(F.col("amount") > HIGH_VALUE_LIMIT)
        .withColumn("fraud_reason", F.lit("HIGH_VALUE"))
    )

    # ── Rule 1: Impossible travel ──────────────────────────────────────────
    # Within each user's transactions in this batch, check if they appear in
    # 2+ different countries within TRAVEL_WINDOW_MIN minutes.
    #
    # Strategy: self-join on user_id where abs(time diff) < window
    # and countries differ.

    txn_a = enriched.alias("a")
    txn_b = enriched.alias("b")

    impossible_travel = (
        txn_a.join(txn_b,
            (F.col("a.user_id")  == F.col("b.user_id")) &
            (F.col("a.country")  != F.col("b.country")) &
            (F.abs(F.col("a.event_ts").cast("long") -
                   F.col("b.event_ts").cast("long")) < TRAVEL_WINDOW_MIN * 60),
            "inner"
        )
        .select(
            F.col("a.user_id"),
            F.col("a.timestamp"),
            F.col("a.merchant_category"),
            F.col("a.amount"),
            F.col("a.location"),
            F.lit("IMPOSSIBLE_TRAVEL").alias("fraud_reason")
        )
        .dropDuplicates(["user_id", "timestamp"])
    )

    # ── Combine fraud records ──────────────────────────────────────────────
    fraud_cols = ["user_id", "timestamp", "merchant_category", "amount",
                  "location", "fraud_reason"]

    fraud_df = (
        high_value.select(fraud_cols)
        .union(impossible_travel.select(fraud_cols))
        .dropDuplicates(["user_id", "timestamp", "fraud_reason"])
    )

    # ── Valid = no fraud match ──────────────────────────────────────────────
    fraud_keys = fraud_df.select("user_id", "timestamp")
    valid_df   = (
        enriched
        .join(fraud_keys,
              on=["user_id", "timestamp"],
              how="left_anti")
        .select("user_id", "timestamp", "merchant_category", "amount", "location")
    )

    # ── Write to PostgreSQL ───────────────────────────────────────────────
    fraud_count = fraud_df.count()
    valid_count = valid_df.count()

    if fraud_count > 0:
        write_to_postgres(fraud_df.withColumn("timestamp", F.to_timestamp(F.col("timestamp"))), "fraud_alerts")
        log.warning("  ⚠  Fraud records written: %d", fraud_count)
        # Show fraud details in logs
        fraud_df.show(truncate=False)
    else:
        log.info("  ✓  No fraud detected in this batch")

    if valid_count > 0:
        write_to_postgres(valid_df.withColumn("timestamp", F.to_timestamp(F.col("timestamp"))), "valid_transactions")
        log.info("  ✓  Valid records written: %d", valid_count)

    # Also write all records to raw transactions table
    all_cols = ["user_id", "timestamp", "merchant_category", "amount", "location"]
    write_to_postgres(enriched.select(all_cols).withColumn("timestamp", F.to_timestamp(F.col("timestamp"))), "transactions")


# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    log.info("Starting Spark Fraud Detector")
    log.info("Kafka: %s  |  Topic: %s", KAFKA_BOOTSTRAP, KAFKA_TOPIC)
    log.info("High-value threshold: $%.0f", HIGH_VALUE_LIMIT)
    log.info("Impossible travel window: %d minutes", TRAVEL_WINDOW_MIN)

    spark = (
        SparkSession.builder
        .appName("FraudDetectionPipeline")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        # Event time / watermark settings
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── Read from Kafka ────────────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe",               KAFKA_TOPIC)
        .option("startingOffsets",         "latest")
        # Watermark: tolerate up to 2 minutes of late-arriving events
        # Event time is the transaction timestamp, not Kafka ingestion time
        .option("failOnDataLoss",          "false")
        .load()
    )

    # ── Deserialise JSON payload ──────────────────────────────────────────
    parsed = (
        raw_stream
        .select(
            F.from_json(
                F.col("value").cast("string"),
                TXN_SCHEMA
            ).alias("data"),
            # Kafka metadata columns (useful for debugging)
            F.col("timestamp").alias("kafka_ts"),
            F.col("partition"),
            F.col("offset"),
        )
        .select("data.*", "kafka_ts")
        # Apply watermark on event time (transaction timestamp)
        # This is key difference: event time vs processing time
        .withColumn("event_ts", F.to_timestamp(F.col("timestamp")))
        .withWatermark("event_ts", "2 minutes")
        .filter(F.col("user_id").isNotNull())
    )

    # ── Write stream (foreachBatch) ────────────────────────────────────────
    query = (
        parsed.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/fraud_detector")
        .trigger(processingTime="10 seconds")   # micro-batch every 10s
        .start()
    )

    log.info("✓  Stream started. Awaiting termination ...")
    query.awaitTermination()


if __name__ == "__main__":
    main()