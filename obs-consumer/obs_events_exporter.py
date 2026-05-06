"""
obs_events_exporter.py — Consumes observability_events from Kafka and exposes
them as Prometheus metrics on :9108/metrics.

Metrics exposed:
  dq_metric_value{rule_id,dq_dimension,pipeline_id}       — Gauge, last observed numeric value
  dq_metric_threshold{rule_id,dq_dimension,pipeline_id}   — Gauge, configured threshold
  dq_check_status{rule_id,dq_dimension,pipeline_id}       — Gauge, 1=PASS 0=BREACH (current state)
  dq_events_total{rule_id,dq_dimension,severity,status,pipeline_id}
                                                           — Counter, one increment per event
  dq_last_event_timestamp_seconds{pipeline_id}            — Gauge, Unix ts of most recent event
  dq_consumer_up                                          — Gauge, 1 when Kafka connection healthy

  # Batch-level metrics (emitted via a "batch_summary" event type):
  dq_batch_size{pipeline_id}                              — Gauge, record count of last batch
  dq_records_clean_total{pipeline_id}                     — Counter, cumulative clean records
  dq_records_quarantined_total{pipeline_id,reason}        — Counter, cumulative quarantined records
"""
from __future__ import annotations

import json
import logging
import os
import signal
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
LABELS_COUNTER  = ("rule_id", "dq_dimension", "severity", "status", "pipeline_id")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("obs-exporter")

# ---------------------------------------------------------------------------
# DQ check metrics
dq_value    = Gauge("dq_metric_value",
                    "Most recent value reported for a DQ rule",
                    LABELS_VALUE)

dq_thresh   = Gauge("dq_metric_threshold",
                    "Configured threshold for a DQ rule",
                    LABELS_VALUE)

# NEW: per-rule live pass/fail state
dq_status   = Gauge("dq_check_status",
                    "Current pass/fail state: 1=PASS, 0=BREACH",
                    LABELS_VALUE)

dq_events   = Counter("dq_events_total",
                      "Total observability events received",
                      LABELS_COUNTER)

dq_last_ts  = Gauge("dq_last_event_timestamp_seconds",
                    "Unix timestamp of most recent observability event",
                    ("pipeline_id",))

dq_up       = Gauge("dq_consumer_up",
                    "1 if the consumer is connected to Kafka, 0 otherwise")

# NEW: batch-level metrics
dq_batch_size = Gauge("dq_batch_size",
                      "Number of records in the most recent processed batch",
                      ("pipeline_id",))

dq_clean_total = Counter("dq_records_clean_total",
                         "Cumulative count of records that passed record-level validation",
                         ("pipeline_id",))

dq_quarantined_total = Counter("dq_records_quarantined_total",
                               "Cumulative count of records sent to quarantine, by reason",
                               ("pipeline_id", "reason"))

dq_processed_total = Counter("dq_records_processed_total",
                             "Cumulative count of all records ingested (clean + quarantine)",
                             ("pipeline_id",))

dq_batches_total = Counter("dq_batches_processed_total",
                           "Cumulative count of micro-batches processed",
                           ("pipeline_id",))

dq_processing_seconds = Gauge("dq_batch_processing_seconds",
                              "Wall-clock seconds spent processing the last micro-batch",
                              ("pipeline_id",))

dq_quarantine_rate = Gauge("dq_quarantine_rate_pct",
                           "Percentage of records quarantined in the last batch (0-100)",
                           ("pipeline_id",))

dq_pipeline_lag = Gauge("dq_pipeline_lag_seconds",
                        "Seconds between the latest event_time in the batch and wall-clock "
                        "processing time — measures data freshness lag",
                        ("pipeline_id",))


# ---------------------------------------------------------------------------
def safe_label(v) -> str:
    """Prometheus labels must be strings; None/missing -> 'unknown'."""
    if v is None or v == "":
        return "unknown"
    return str(v)


def process_check_event(evt: dict) -> None:
    """Handle a standard DQ check event (one Soda check result)."""
    rule_id     = safe_label(evt.get("rule_id"))
    dimension   = safe_label(evt.get("dq_dimension"))
    pipeline_id = safe_label(evt.get("pipeline_id"))
    severity    = safe_label(evt.get("severity"))
    status      = safe_label(evt.get("status"))

    # Counter: now includes pipeline_id label for better cross-panel correlation
    dq_events.labels(
        rule_id=rule_id,
        dq_dimension=dimension,
        severity=severity,
        status=status,
        pipeline_id=pipeline_id,
    ).inc()

    # FIX: dq_check_status gives a clear per-rule live pass/fail state.
    # This is what the DQ Score and rule health panels should use.
    dq_status.labels(
        rule_id=rule_id, dq_dimension=dimension, pipeline_id=pipeline_id,
    ).set(1.0 if status == "PASS" else 0.0)

    # FIX: threshold of 0 is valid — check for None, not falsiness.
    # This ensures binary checks (count=0, row_count>0) show threshold correctly.
    val = evt.get("value")
    if isinstance(val, (int, float)):
        dq_value.labels(
            rule_id=rule_id, dq_dimension=dimension, pipeline_id=pipeline_id,
        ).set(float(val))

    thr = evt.get("threshold")
    if thr is not None and isinstance(thr, (int, float)):
        dq_thresh.labels(
            rule_id=rule_id, dq_dimension=dimension, pipeline_id=pipeline_id,
        ).set(float(thr))

    dq_last_ts.labels(pipeline_id=pipeline_id).set_to_current_time()


def process_batch_summary(evt: dict) -> None:
    """Handle a batch_summary event emitted by dq_stream_job after each micro-batch."""
    pipeline_id  = safe_label(evt.get("pipeline_id"))
    batch_size   = evt.get("batch_size")
    n_clean      = evt.get("n_clean")
    n_quarantine = evt.get("n_quarantine")
    quarantine   = evt.get("quarantine_by_reason", {}) or {}

    # Volume
    if isinstance(batch_size, int):
        dq_batch_size.labels(pipeline_id=pipeline_id).set(float(batch_size))
        dq_processed_total.labels(pipeline_id=pipeline_id).inc(batch_size)

    if isinstance(n_clean, int) and n_clean > 0:
        dq_clean_total.labels(pipeline_id=pipeline_id).inc(n_clean)

    if isinstance(n_quarantine, int) and n_quarantine > 0:
        for reason, count in quarantine.items():
            if isinstance(count, int) and count > 0:
                dq_quarantined_total.labels(
                    pipeline_id=pipeline_id,
                    reason=safe_label(reason),
                ).inc(count)

    # Batch counter
    dq_batches_total.labels(pipeline_id=pipeline_id).inc()

    # Performance: processing time
    proc_sec = evt.get("processing_seconds")
    if isinstance(proc_sec, (int, float)):
        dq_processing_seconds.labels(pipeline_id=pipeline_id).set(float(proc_sec))

    # Quality: quarantine rate %
    qrate = evt.get("quarantine_rate_pct")
    if isinstance(qrate, (int, float)):
        dq_quarantine_rate.labels(pipeline_id=pipeline_id).set(float(qrate))

    # Freshness: pipeline lag
    lag = evt.get("pipeline_lag_seconds")
    if isinstance(lag, (int, float)):
        dq_pipeline_lag.labels(pipeline_id=pipeline_id).set(float(lag))

    dq_last_ts.labels(pipeline_id=pipeline_id).set_to_current_time()


def process_event(evt: dict) -> None:
    """Route event to the correct handler by event_type."""
    if evt.get("event_type") == "batch_summary":
        process_batch_summary(evt)
    else:
        process_check_event(evt)


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
