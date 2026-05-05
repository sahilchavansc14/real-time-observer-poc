"""
orders_producer.py — Phase 1 data simulator for the Real-Time DQ Observability POC.

Runs on host (not in Docker). Connects to Kafka via the PLAINTEXT_HOST listener
at localhost:9094.

Usage examples:
    # Phase 1 baseline: ~500 records / 60s  (≈ 8.3 rps)
    python orders_producer.py --rps 8.3 --duration 60

    # Phase 2 scale-up: 1000 rps for 10 seconds
    python orders_producer.py --rps 1000 --duration 10

    # With failure injection (5% nulls, 2% duplicates, 1% late events, 0.5% schema drift)
    python orders_producer.py --rps 500 --duration 30 \
        --null-rate 0.05 --dup-rate 0.02 --late-rate 0.01 --drift-rate 0.005

    # Burst/volume-spike mode: 10× surge every 30s
    python orders_producer.py --rps 100 --duration 120 --spike-mult 10 --spike-every 30
"""
from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta

from kafka import KafkaProducer
from kafka.errors import KafkaError


# ---------------------------------------------------------------------------
# Record model
# ---------------------------------------------------------------------------
@dataclass
class Order:
    order_id: str
    customer_id: str | None
    product_id: str
    quantity: int
    amount: float
    currency: str
    email: str
    status: str
    event_time: str          # ISO8601, UTC — when the order actually happened
    source_region: str


PRODUCTS = [f"P-{i:04d}" for i in range(1, 201)]          # 200 SKUs
CUSTOMERS = [f"c-{i:03d}"  for i in range(1, 301)]        # 300 customers
REGIONS = ["us-east", "us-west", "eu-central", "ap-south", "ap-southeast"]
STATUSES = ["CREATED", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]


def make_order(now: datetime | None = None) -> Order:
    now = now or datetime.now(timezone.utc)
    qty = random.randint(1, 5)
    unit_price = round(random.uniform(5.0, 500.0), 2)
    cust = random.choice(CUSTOMERS)
    return Order(
        order_id=str(uuid.uuid4()),
        customer_id=cust,
        product_id=random.choice(PRODUCTS),
        quantity=qty,
        amount=round(qty * unit_price, 2),
        currency="USD",
        email=f"{cust}@example.com",
        status=random.choice(STATUSES),
        event_time=now.isoformat(),
        source_region=random.choice(REGIONS),
    )


# ---------------------------------------------------------------------------
# Failure injectors  — each returns a (possibly-mutated) dict, not an Order,
# because schema drift requires adding/removing keys.
# ---------------------------------------------------------------------------
def inject_null(record: dict) -> dict:
    # Null out a critical field (customer_id) — tests completeness rule.
    record["customer_id"] = None
    return record


def inject_late(record: dict, max_lateness_sec: int = 600) -> dict:
    # Backdate event_time — tests timeliness / watermark behavior.
    delay = random.randint(60, max_lateness_sec)
    backdated = datetime.now(timezone.utc) - timedelta(seconds=delay)
    record["event_time"] = backdated.isoformat()
    return record


def inject_schema_drift(record: dict) -> dict:
    # Remove a required key or rename one — tests validity/schema rule.
    # Randomly pick one of two drift modes:
    if random.random() < 0.5:
        record.pop("quantity", None)          # missing required field
    else:
        record["amt"] = record.pop("amount")  # renamed field
    return record


def inject_invalid_email(record: dict) -> dict:
    record["email"] = "not-an-email"
    return record


# ---------------------------------------------------------------------------
# Rate control — token bucket
# ---------------------------------------------------------------------------
class TokenBucket:
    """Accurate rate limiter that compensates for jitter."""

    def __init__(self, rate_per_sec: float, burst: int | None = None):
        self.rate = rate_per_sec
        self.capacity = burst or max(1, int(rate_per_sec))
        self.tokens = float(self.capacity)
        self.last = time.monotonic()

    def take(self, n: int = 1) -> None:
        while True:
            now = time.monotonic()
            self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate)
            self.last = now
            if self.tokens >= n:
                self.tokens -= n
                return
            time.sleep(max(0.0005, (n - self.tokens) / self.rate))


# ---------------------------------------------------------------------------
# Main producer loop
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap",   default="localhost:9094",
                    help="Kafka bootstrap server (host listener)")
    ap.add_argument("--topic",       default="orders_raw")
    ap.add_argument("--rps",         type=float, default=8.3,
                    help="Target records per second")
    ap.add_argument("--duration",    type=int, default=60,
                    help="How long to run, in seconds. -1 = run forever")
    # Failure injection rates (0.0–1.0)
    ap.add_argument("--null-rate",   type=float, default=0.0)
    ap.add_argument("--dup-rate",    type=float, default=0.0)
    ap.add_argument("--late-rate",   type=float, default=0.0)
    ap.add_argument("--drift-rate",  type=float, default=0.0)
    ap.add_argument("--email-bad-rate", type=float, default=0.0)
    # Volume spike
    ap.add_argument("--spike-mult",  type=float, default=1.0,
                    help="Multiplier applied during spike windows")
    ap.add_argument("--spike-every", type=int, default=0,
                    help="Seconds between spikes (0 = disabled)")
    ap.add_argument("--spike-duration", type=int, default=5,
                    help="Duration of each spike window, seconds")
    args = ap.parse_args()

    print(f"[producer] connecting to {args.bootstrap} topic={args.topic}")
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks=1,
        linger_ms=5,
    )

    # Graceful shutdown
    stop = {"flag": False}
    signal.signal(signal.SIGINT,  lambda *_: stop.update(flag=True))
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))

    base_rate = args.rps
    bucket = TokenBucket(rate_per_sec=base_rate)

    sent = 0
    nulls = dups = lates = drifts = bad_emails = 0
    last_report = time.monotonic()
    start = time.monotonic()

    # For duplicate injection, keep a rolling pool of recent records
    recent_pool: list[dict] = []
    POOL_MAX = 500

    current_rate = base_rate
    spike_end_at = 0.0

    while not stop["flag"]:
        now = time.monotonic()
        if args.duration > 0 and (now - start) >= args.duration:
            break

        # Handle spike window
        if args.spike_every > 0:
            elapsed = now - start
            in_spike = (int(elapsed) % args.spike_every) < args.spike_duration
            target_rate = base_rate * (args.spike_mult if in_spike else 1.0)
            if target_rate != current_rate:
                bucket = TokenBucket(rate_per_sec=target_rate)
                current_rate = target_rate

        bucket.take(1)

        # Build record
        order = make_order()
        record = asdict(order)

        # Duplicate injection: resend an earlier record unchanged
        if recent_pool and random.random() < args.dup_rate:
            record = dict(random.choice(recent_pool))   # copy
            dups += 1
        else:
            # Other injections only on non-duplicate path
            if random.random() < args.null_rate:
                record = inject_null(record);  nulls += 1
            if random.random() < args.late_rate:
                record = inject_late(record);  lates += 1
            if random.random() < args.drift_rate:
                record = inject_schema_drift(record); drifts += 1
            if random.random() < args.email_bad_rate:
                record = inject_invalid_email(record); bad_emails += 1

            # Feed the pool (only clean-ish records)
            recent_pool.append(record)
            if len(recent_pool) > POOL_MAX:
                recent_pool.pop(0)

        try:
            producer.send(args.topic, key=record.get("order_id"), value=record)
            sent += 1
        except KafkaError as e:
            print(f"[producer] send error: {e}", file=sys.stderr)

        # Progress reporting every 5s
        if now - last_report >= 5.0:
            elapsed = now - start
            print(f"[producer] t={elapsed:5.1f}s sent={sent:>7} "
                  f"rps_eff={sent/elapsed:7.1f} target={current_rate:>6.1f} "
                  f"nulls={nulls} dups={dups} lates={lates} "
                  f"drifts={drifts} bad_emails={bad_emails}")
            last_report = now

    producer.flush(timeout=10)
    producer.close()

    elapsed = time.monotonic() - start
    print(f"\n[producer] DONE. sent={sent} in {elapsed:.1f}s "
          f"({sent/elapsed:.1f} eps effective)")
    print(f"[producer] injected: nulls={nulls} dups={dups} lates={lates} "
          f"drifts={drifts} bad_emails={bad_emails}")


if __name__ == "__main__":
    main()
