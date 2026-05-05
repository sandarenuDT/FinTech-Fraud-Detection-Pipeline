"""
FinTech Fraud Detection Pipeline
=================================
Producer: Synthetic Transaction Generator

Publishes realistic credit card transactions to Kafka topic 'transactions'.
Fraud scenarios are injected occasionally and are clearly logged.

Usage:
    python producer.py [--rate MSGS_PER_SEC] [--duration SECONDS]
"""

import json
import time
import random
import argparse
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
import os

# ─── Logging setup ───────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("producer")

# ─── Config ──────────────────────────────────────────────────────────────────
KAFKA_TOPIC          = "transactions"
BOOTSTRAP_SERVERS    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Fraud injection probabilities (keep low to mimic real-world scenarios)
FRAUD_IMPOSSIBLE_TRAVEL_PROB = 0.04   # 4%  chance per message → impossible travel
FRAUD_HIGH_VALUE_PROB        = 0.03   # 3%  chance per message → amount > $5000

# Data pools
USER_IDS = [f"user_{i:03d}" for i in range(1, 51)]   # 50 users

MERCHANT_CATEGORIES = [
    "grocery", "electronics", "travel", "restaurant",
    "fuel", "healthcare", "entertainment", "clothing",
    "online_retail", "atm_withdrawal"
]

COUNTRIES = {
    "LK": ["Colombo", "Kandy", "Galle"],
    "US": ["New_York", "Los_Angeles", "Chicago"],
    "UK": ["London", "Manchester", "Birmingham"],
    "IN": ["Mumbai", "Delhi", "Bangalore"],
    "SG": ["Singapore"],
    "AU": ["Sydney", "Melbourne"],
    "DE": ["Berlin", "Frankfurt"],
    "FR": ["Paris", "Lyon"],
}

# Flat list for random picks
ALL_LOCATIONS = [f"{city}_{country}" for country, cities in COUNTRIES.items()
                 for city in cities]

# Normal amount ranges per category (min, max)
AMOUNT_RANGES = {
    "grocery":        (5,    250),
    "electronics":    (50,   1500),
    "travel":         (100,  3000),
    "restaurant":     (10,   150),
    "fuel":           (20,   120),
    "healthcare":     (30,   500),
    "entertainment":  (10,   200),
    "clothing":       (20,   400),
    "online_retail":  (15,   800),
    "atm_withdrawal": (50,   500),
}

# Track last transaction per user to simulate impossible travel
user_last_location: dict[str, tuple[str, float]] = {}  # user → (location, timestamp)


# ─── Helpers ─────────────────────────────────────────────────────────────────
def get_location_country(location: str) -> str:
    """Extract country code from location string e.g. 'London_UK' → 'UK'."""
    return location.split("_")[-1]


def make_normal_transaction(user_id: str) -> dict:
    """Generate a realistic normal transaction."""
    category = random.choice(MERCHANT_CATEGORIES)
    lo, hi   = AMOUNT_RANGES[category]
    amount   = round(random.uniform(lo, hi), 2)
    location = random.choice(ALL_LOCATIONS)

    txn = {
        "user_id":            user_id,
        "timestamp":          datetime.now(timezone.utc).isoformat(),
        "merchant_category":  category,
        "amount":             amount,
        "location":           location,
        "is_injected_fraud":  False,   # metadata only; Spark ignores this field
    }

    user_last_location[user_id] = (location, time.time())
    return txn


def make_impossible_travel_fraud(user_id: str) -> dict:
    """
    Inject impossible travel:
    Send a transaction from a DIFFERENT country to the user's last known location
    within the same 10-minute window.
    """
    last_loc, last_ts = user_last_location.get(user_id, (random.choice(ALL_LOCATIONS), 0))
    last_country      = get_location_country(last_loc)

    # Pick a country that is different from last
    foreign_countries = [c for c in COUNTRIES if c != last_country]
    fraud_country     = random.choice(foreign_countries)
    fraud_city        = random.choice(COUNTRIES[fraud_country])
    fraud_location    = f"{fraud_city}_{fraud_country}"

    # Timestamp is close to last transaction (within 5 minutes)
    fraudulent_ts = datetime.now(timezone.utc).isoformat()

    category = random.choice(MERCHANT_CATEGORIES)
    lo, hi   = AMOUNT_RANGES[category]
    amount   = round(random.uniform(lo, hi), 2)

    txn = {
        "user_id":            user_id,
        "timestamp":          fraudulent_ts,
        "merchant_category":  category,
        "amount":             amount,
        "location":           fraud_location,
        "is_injected_fraud":  True,
    }

    log.warning(
        "⚠  [FRAUD_INJECT] Impossible travel | %s | %s → %s | $%.2f",
        user_id, last_loc, fraud_location, amount
    )
    return txn


def make_high_value_fraud(user_id: str) -> dict:
    """Inject a suspiciously high-value transaction (> $5000)."""
    category = random.choice(["electronics", "travel", "online_retail"])
    amount   = round(random.uniform(5001, 25000), 2)
    location = random.choice(ALL_LOCATIONS)

    txn = {
        "user_id":            user_id,
        "timestamp":          datetime.now(timezone.utc).isoformat(),
        "merchant_category":  category,
        "amount":             amount,
        "location":           location,
        "is_injected_fraud":  True,
    }

    log.warning(
        "⚠  [FRAUD_INJECT] High-value transaction | %s | $%.2f | %s",
        user_id, amount, category
    )
    return txn


# ─── Delivery callback ────────────────────────────────────────────────────────
def on_success(record_metadata):
    pass   # suppress per-message success noise


def on_error(excp):
    log.error("Kafka delivery error: %s", excp)


# ─── Main loop ────────────────────────────────────────────────────────────────
def run(rate: float, duration: int):
    log.info("Connecting to Kafka at %s ...", BOOTSTRAP_SERVERS)

    # Retry loop - Kafka may not be ready immediately on container start
    producer = None
    for attempt in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            log.info("✓  Connected to Kafka")
            break
        except Exception as exc:
            log.warning("Attempt %d/10 failed: %s — retrying in 5s", attempt + 1, exc)
            time.sleep(5)

    if producer is None:
        log.error("Could not connect to Kafka after 10 attempts. Exiting.")
        return

    interval  = 1.0 / rate
    start     = time.time()
    msg_count = 0
    fraud_count = 0

    log.info("Starting stream | rate=%.1f msg/s | duration=%ds", rate, duration)
    log.info("─" * 60)

    try:
        while True:
            if duration > 0 and (time.time() - start) >= duration:
                break

            user_id = random.choice(USER_IDS)
            roll    = random.random()

            # Decide transaction type
            if roll < FRAUD_IMPOSSIBLE_TRAVEL_PROB and user_id in user_last_location:
                txn = make_impossible_travel_fraud(user_id)
                fraud_count += 1
            elif roll < (FRAUD_IMPOSSIBLE_TRAVEL_PROB + FRAUD_HIGH_VALUE_PROB):
                txn = make_high_value_fraud(user_id)
                fraud_count += 1
            else:
                txn = make_normal_transaction(user_id)
                log.info(
                    "[NORMAL] %-10s | $%8.2f | %-20s | %s",
                    user_id, txn["amount"], txn["merchant_category"], txn["location"]
                )

            producer.send(KAFKA_TOPIC, value=txn, key=user_id.encode()) \
                    .add_callback(on_success).add_errback(on_error)

            msg_count += 1
            time.sleep(interval)

    except KeyboardInterrupt:
        log.info("Interrupted by user")

    finally:
        producer.flush()
        elapsed = time.time() - start
        log.info("─" * 60)
        log.info(
            "Session complete | messages=%d | fraud_injected=%d | elapsed=%.1fs",
            msg_count, fraud_count, elapsed
        )


# ─── Entry point ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fraud pipeline transaction producer")
    parser.add_argument("--rate",     type=float, default=1.0,
                        help="Messages per second (default: 1.0)")
    parser.add_argument("--duration", type=int,   default=0,
                        help="Run for N seconds then exit (0 = run forever)")
    args = parser.parse_args()

    run(rate=args.rate, duration=args.duration)
