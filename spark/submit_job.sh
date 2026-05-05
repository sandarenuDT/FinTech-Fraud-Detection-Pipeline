#!/bin/bash
# ──────────────────────────────────────────────────────────────────────────────
# Submit the Spark Fraud Detector to the Spark cluster
#
# Run this from your HOST machine (not inside the container):
#   bash spark/submit_job.sh
# ──────────────────────────────────────────────────────────────────────────────

docker exec spark-master \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages \
      org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
      org.postgresql:postgresql:42.6.0 \
    --conf spark.sql.shuffle.partitions=4 \
    --conf spark.streaming.stopGracefullyOnShutdown=true \
    /opt/spark/work-dir/spark_fraud_detector.py
