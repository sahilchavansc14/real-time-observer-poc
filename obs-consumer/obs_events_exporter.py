"""
obs_events_exporter.py — Consumes observability_events from Kafka and exposes
them as Prometheus metrics on :9108/metrics.

Metrics exposed:
  dq_metric_value{rule_id,dq_dimension,pipeline_id}      — Gauge, last observed numeric value
  dq_metric_threshold{rule_id,dq_dimension,pipeline_id}  — Gauge, the configured threshold
  dq_events_total{rule_id,dq_dimension,severity,status}  — Counter, one increment per event
  dq_last_event_timestamp_seconds{pipeline_id}           — Gauge, Unix ts of most recent event
  dq_consumer_up                                          — Gauge, 1 when Kafka connection healthy
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from typing import Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from prometheus_client import Counter, Gauge, start_http_server

# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC           = os.getenv("OBS_TOPIC", "observability_events")
GROUP_ID        = os.getenv("GROUP_ID", "obs-events-prometheus-exporter")
METRICS_PORT    = int(os.getenv("METRICS_PORT", "9108"))

LABELS_VALUE    = ("rule_id", "dq_dimension", "pipeline_id")
LABELS_COUNTER  = ("rule_id", "dq_dimension", "severity", "status")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("obs-exporter")

# ---------------------------------------------------------------------------
dq_value    = Gauge("dq_metric_value",
                    "Most recent value reported for a DQ rule",
                    LABELS_VALUE)
dq_thresh   = Gauge("dq_metric_threshold",
                    "Configured threshold for a DQ rule",
                    LABELS_VALUE)
dq_events   = Counter("dq_events_total",
                      "Total observability events received",
                      LABELS_COUNTER)
dq_last_ts  = Gauge("dq_last_event_timestamp_seconds",
                    "Unix timestamp of most recent observability event",
                    ("pipeline_id",))
dq_up       = Gauge("dq_consumer_up",
                    "1 if the consumer is connected to Kafka, 0 otherwise")


# ---------------------------------------------------------------------------
def safe_label(v) -> str:
    """Prometheus labels must be strings; None/missing → 'unknown'."""
    if v is None or v == "":
        return "unknown"
    return str(v)


def process_event(evt: dict) -> None:
    rule_id      = safe_label(evt.get("rule_id"))
    dimension    = safe_label(evt.get("dq_dimension"))
    pipeline_id  = safe_label(evt.get("pipeline_id"))
    severity     = safe_label(evt.get("severity"))
    status       = safe_label(evt.get("status"))

    # Counter — always increments, one per event
    dq_events.labels(
        rule_id=rule_id,
        dq_dimension=dimension,
        severity=severity,
        status=status,
    ).inc()

    # Gauges — only update when numeric value is present
    val = evt.get("value")
    if isinstance(val, (int, float)):
        dq_value.labels(
            rule_id=rule_id, dq_dimension=dimension, pipeline_id=pipeline_id,
        ).set(float(val))

    thr = evt.get("threshold")
    if isinstance(thr, (int, float)):
        dq_thresh.labels(
            rule_id=rule_id, dq_dimension=dimension, pipeline_id=pipeline_id,
        ).set(float(thr))

    dq_last_ts.labels(pipeline_id=pipeline_id).set_to_current_time()


# ---------------------------------------------------------------------------
def connect_consumer() -> Optional[KafkaConsumer]:
    try:
        c = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            consumer_timeout_ms=-1,
        )
        dq_up.set(1)
        return c
    except (KafkaError, NoBrokersAvailable) as e:
        dq_up.set(0)
        log.error("Kafka connection failed: %s", e)
        return None


def main() -> None:
    log.info("starting Prometheus HTTP server on :%d", METRICS_PORT)
    start_http_server(METRICS_PORT)
    dq_up.set(0)

    stop = {"flag": False}
    signal.signal(signal.SIGINT,  lambda *_: stop.update(flag=True))
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))

    consumer: Optional[KafkaConsumer] = None
    while not stop["flag"]:
        if consumer is None:
            consumer = connect_consumer()
            if consumer is None:
                log.info("retry Kafka connection in 5s")
                time.sleep(5)
                continue
            log.info("connected to Kafka, consuming topic=%s", TOPIC)

        try:
            for msg in consumer:
                if stop["flag"]:
                    break
                try:
                    process_event(msg.value)
                except Exception as exc:
                    log.warning("bad event dropped: %s (payload=%r)",
                                exc, msg.value)
        except Exception as exc:
            log.error("consumer loop error: %s — reconnecting", exc)
            dq_up.set(0)
            try:
                consumer.close()
            except Exception:
                pass
            consumer = None
            time.sleep(3)

    if consumer is not None:
        consumer.close()
    log.info("shutdown complete")


if __name__ == "__main__":
    main()
