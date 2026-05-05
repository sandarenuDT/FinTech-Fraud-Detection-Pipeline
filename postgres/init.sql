-- ============================================================
-- Fraud Detection Pipeline - Database Initialisation
-- ============================================================

-- All raw transactions received from Kafka
CREATE TABLE IF NOT EXISTS transactions (
    id              SERIAL PRIMARY KEY,
    user_id         VARCHAR(20)    NOT NULL,
    timestamp       TIMESTAMPTZ    NOT NULL,
    merchant_category VARCHAR(50)  NOT NULL,
    amount          NUMERIC(12, 2) NOT NULL,
    location        VARCHAR(50)    NOT NULL,
    ingested_at     TIMESTAMPTZ    DEFAULT NOW()
);

-- Fraud-flagged records written by Spark in real-time
CREATE TABLE IF NOT EXISTS fraud_alerts (
    id              SERIAL PRIMARY KEY,
    user_id         VARCHAR(20)    NOT NULL,
    timestamp       TIMESTAMPTZ    NOT NULL,
    merchant_category VARCHAR(50)  NOT NULL,
    amount          NUMERIC(12, 2) NOT NULL,
    location        VARCHAR(50)    NOT NULL,
    fraud_reason    VARCHAR(100)   NOT NULL,   -- e.g. "IMPOSSIBLE_TRAVEL" / "HIGH_VALUE"
    flagged_at      TIMESTAMPTZ    DEFAULT NOW()
);

-- Clean (non-fraud) transactions validated by Spark
CREATE TABLE IF NOT EXISTS valid_transactions (
    id              SERIAL PRIMARY KEY,
    user_id         VARCHAR(20)    NOT NULL,
    timestamp       TIMESTAMPTZ    NOT NULL,
    merchant_category VARCHAR(50)  NOT NULL,
    amount          NUMERIC(12, 2) NOT NULL,
    location        VARCHAR(50)    NOT NULL,
    processed_at    TIMESTAMPTZ    DEFAULT NOW()
);

-- Reconciliation summary written by Airflow every 6 hours
CREATE TABLE IF NOT EXISTS reconciliation_reports (
    id              SERIAL PRIMARY KEY,
    report_period   VARCHAR(30)    NOT NULL,   -- e.g. "2024-01-15 00:00 to 06:00"
    total_ingress   NUMERIC(15, 2) NOT NULL,
    total_validated NUMERIC(15, 2) NOT NULL,
    total_fraud     NUMERIC(15, 2) NOT NULL,
    transaction_count INT          NOT NULL,
    fraud_count     INT            NOT NULL,
    generated_at    TIMESTAMPTZ    DEFAULT NOW()
);

-- Indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_fraud_user ON fraud_alerts(user_id);
CREATE INDEX IF NOT EXISTS idx_fraud_time ON fraud_alerts(timestamp);
CREATE INDEX IF NOT EXISTS idx_valid_time ON valid_transactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_txn_user   ON transactions(user_id);

-- Confirmation
SELECT 'Database initialised successfully' AS status;
