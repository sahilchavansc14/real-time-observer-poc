"""
dq_stream_job.py — Phase 2 (v2): full DQ pipeline.

Flow per micro-batch (every 30 seconds):
  orders_raw  ──►  parse JSON
              ──►  record-level validation (PySpark):
                       GOOD records  ──►  orders_clean
                       BAD  records  ──►  orders_quarantine (with reason)
              ──►  batch-level DQ (Soda Core):
                       summary metrics  ──►  observability_events

Key fix vs v1:
  - Uses batch_df.sparkSession (the session the DataFrame is bound to)
    instead of the captured outer `spark`. This resolves the
    TABLE_OR_VIEW_NOT_FOUND error for `orders_batch`.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP   = "kafka:9092"
SOURCE_TOPIC      = "orders_raw"
CLEAN_TOPIC       = "orders_clean"
QUARANTINE_TOPIC  = "orders_quarantine"
OBS_TOPIC         = "observability_events"
TRIGGER_SEC       = 30
CHECKPOINT_BASE   = "/opt/checkpoints/dq_stream_job"

ORDER_SCHEMA = StructType([
    StructField("order_id",      StringType()),
    StructField("customer_id",   StringType()),
    StructField("product_id",    StringType()),
    StructField("quantity",      IntegerType()),
    StructField("amount",        DoubleType()),
    StructField("currency",      StringType()),
    StructField("email",         StringType()),
    StructField("status",        StringType()),
    StructField("event_time",    StringType()),
    StructField("source_region", StringType()),
])

EMAIL_RE = r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"


# ---------------------------------------------------------------------------
# Soda Core OSS does not return custom `attributes` in get_scan_results().
# This map lets us recover rule_id, dimension, and severity from the check name
# which IS reliably returned.
RULE_META = {
    "Completeness – customer_id null percentage under 5%": {
        "rule_id": "R-COMPL-001", "dimension": "completeness", "severity": "warn"
    },
    "Completeness – order_id never null": {
        "rule_id": "R-COMPL-002", "dimension": "completeness", "severity": "critical"
    },
    "Completeness – amount never null": {
        "rule_id": "R-COMPL-003", "dimension": "completeness", "severity": "critical"
    },
    "Completeness – email null percentage under 5%": {
        "rule_id": "R-COMPL-004", "dimension": "completeness", "severity": "warn"
    },
    "Completeness – status never null": {
        "rule_id": "R-COMPL-005", "dimension": "completeness", "severity": "warn"
    },
    "Uniqueness – order_id unique within window": {
        "rule_id": "R-UNIQ-001", "dimension": "uniqueness", "severity": "critical"
    },
    "Validity – email format compliance": {
        "rule_id": "R-VALID-001", "dimension": "validity", "severity": "warn"
    },
    "Validity – quantity must be positive integer (1–1000)": {
        "rule_id": "R-VALID-002", "dimension": "validity", "severity": "warn"
    },
    "Validity – amount must be positive": {
        "rule_id": "R-VALID-003", "dimension": "validity", "severity": "critical"
    },
    "Validity – currency must be a known ISO code": {
        "rule_id": "R-VALID-004", "dimension": "validity", "severity": "warn"
    },
    "Validity – order status must be an accepted value": {
        "rule_id": "R-VALID-005", "dimension": "validity", "severity": "warn"
    },
    "Volume – batch is non-empty": {
        "rule_id": "R-VOL-001", "dimension": "volume", "severity": "critical"
    },
    "Timeliness – most recent event_time within 10 minutes": {
        "rule_id": "R-TIME-001", "dimension": "timeliness", "severity": "warn"
    },
}


def _parse_num(v):
    """Coerce v to float. Handles int, float, and numeric strings like '5' or '5.0'."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).strip())
    except (ValueError, TypeError):
        return None


def to_obs_event(check: dict, batch_id: int, batch_size: int) -> dict:
    check_name = check.get("name", "")
    status     = "PASS" if check.get("outcome") == "pass" else "BREACH"

    # Soda Core OSS does not return custom attributes — look them up by check name.
    meta      = RULE_META.get(check_name, {})
    rule_id   = meta.get("rule_id", "unknown")
    dimension = meta.get("dimension", "unknown")
    default_sev = meta.get("severity", "warn")
    sev = default_sev.upper() if status == "BREACH" else "INFO"

    # Value and threshold live inside check["diagnostics"], not at top level.
    diagnostics    = check.get("diagnostics") or {}
    measured_value = _parse_num(diagnostics.get("value"))
    threshold      = _parse_num(diagnostics.get("fail threshold") or diagnostics.get("threshold"))

    return {
        "event_id":     str(uuid.uuid4()),
        "pipeline_id":  "orders-ingestion-pipeline",
        "batch_id":     str(batch_id),
        "entity":       f"kafka.{SOURCE_TOPIC}",
        "metric":       check_name,
        "dq_dimension": dimension,
        "rule_id":      rule_id,
        "value":        measured_value,
        "threshold":    threshold,
        "status":       status,
        "severity":     sev,
        "timestamp":    datetime.now(timezone.utc).isoformat(),
        "metadata": {
            "batch_size":    batch_size,
            "check_outcome": check.get("outcome"),
        },
    }


# ---------------------------------------------------------------------------
def build_handler():
    def handler(batch_df: DataFrame, batch_id: int):
        # CRITICAL FIX: use the session the DataFrame belongs to,
        # not the outer `spark` variable.
        spark = batch_df.sparkSession

        batch_start = datetime.now(timezone.utc)

        batch_df = batch_df.persist()
        size = batch_df.count()
        if size == 0:
            batch_df.unpersist()
            return

        # --- 1. Record-level validation: branch to clean vs quarantine ------

        # Mark duplicate order_ids within the batch.
        # First occurrence is kept; every subsequent occurrence is a DUPLICATE.
        w = Window.partitionBy("order_id").orderBy(F.monotonically_increasing_id())
        batch_df = (batch_df
                    .withColumn("_row_num", F.row_number().over(w))
                    .withColumn("_is_dup",  F.col("_row_num") > 1)
                    .drop("_row_num"))

        bad_cond = (
            F.col("order_id").isNull() |
            F.col("customer_id").isNull() |
            F.col("quantity").isNull() |
            (F.col("quantity") < 1) | (F.col("quantity") > 1000) |
            (F.col("email").isNotNull() & ~F.col("email").rlike(EMAIL_RE)) |
            F.col("_is_dup")
        )

        reason = (
            F.when(F.col("order_id").isNull(),     "NULL_ORDER_ID")
             .when(F.col("customer_id").isNull(),  "NULL_CUSTOMER_ID")
             .when(F.col("quantity").isNull(),     "NULL_QUANTITY")
             .when((F.col("quantity") < 1) | (F.col("quantity") > 1000), "INVALID_QUANTITY_RANGE")
             .when(F.col("email").isNotNull() & ~F.col("email").rlike(EMAIL_RE), "INVALID_EMAIL")
             .when(F.col("_is_dup"),               "DUPLICATE_ORDER_ID")
             .otherwise("OK")
        )

        annotated = (batch_df
                     .withColumn("_is_bad", bad_cond)
                     .withColumn("_reason", reason)
                     .drop("_is_dup"))

        clean_df      = annotated.filter(~F.col("_is_bad")).drop("_is_bad", "_reason")
        quarantine_df = annotated.filter( F.col("_is_bad"))

        n_clean      = clean_df.count()
        n_quarantine = quarantine_df.count()

        # Write CLEAN records → orders_clean
        if n_clean > 0:
            (clean_df
                .select(F.to_json(F.struct(*clean_df.columns)).alias("value"))
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("topic", CLEAN_TOPIC)
                .save())

        # Write BAD records → orders_quarantine (with reason + batch id)
        if n_quarantine > 0:
            quar_payload = (quarantine_df
                            .withColumn("_batch_id", F.lit(batch_id))
                            .withColumn("_quarantined_at", F.current_timestamp().cast("string")))
            (quar_payload
                .select(F.to_json(F.struct(*quar_payload.columns)).alias("value"))
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("topic", QUARANTINE_TOPIC)
                .save())

        # --- 2. Batch-level DQ: Soda Core -----------------------------------
        batch_df.createOrReplaceTempView("orders_batch")

        from soda.scan import Scan
        scan = Scan()
        scan.set_scan_definition_name(f"orders_batch_{batch_id}")
        scan.set_data_source_name("spark_df")
        scan.add_spark_session(spark, data_source_name="spark_df")
        scan.add_sodacl_yaml_file("/opt/jobs/dq/orders_checks.yml")
        scan.execute()

        results = scan.get_scan_results() or {}
        checks  = results.get("checks", [])
        events  = [to_obs_event(c, batch_id, size) for c in checks]

        if events:
            obs_df = spark.createDataFrame(
                [(json.dumps(e),) for e in events], ["value"]
            )
            (obs_df.write
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("topic", OBS_TOPIC)
                .save())

        n_breach = sum(1 for e in events if e["status"] == "BREACH")

        # --- 3. Collect extra batch-level stats for Prometheus ---------------
        batch_end = datetime.now(timezone.utc)
        processing_seconds = (batch_end - batch_start).total_seconds()

        # Quarantine rate as a percentage of the batch
        quarantine_rate = round((n_quarantine / size) * 100, 2) if size > 0 else 0.0

        # Pipeline lag: wall-clock time minus the latest event_time in the batch.
        # Measures how "stale" the data is when it arrives.
        pipeline_lag_seconds = None
        try:
            max_event_ts_row = batch_df.agg(F.max("event_time")).collect()[0][0]
            if max_event_ts_row:
                from datetime import datetime as dt
                max_event_dt = dt.fromisoformat(str(max_event_ts_row).replace("Z", "+00:00"))
                if max_event_dt.tzinfo is None:
                    max_event_dt = max_event_dt.replace(tzinfo=timezone.utc)
                pipeline_lag_seconds = round(
                    (batch_end - max_event_dt).total_seconds(), 2
                )
        except Exception:
            pass  # lag is optional; don't fail the batch over it

        # Quarantine reason breakdown
        quarantine_by_reason: dict = {}
        if n_quarantine > 0:
            reason_counts = (
                quarantine_df
                .groupBy("_reason")
                .count()
                .collect()
            )
            quarantine_by_reason = {row["_reason"]: row["count"] for row in reason_counts}

        batch_summary = {
            "event_type":             "batch_summary",
            "event_id":               str(uuid.uuid4()),
            "pipeline_id":            "orders-ingestion-pipeline",
            "batch_id":               str(batch_id),
            "timestamp":              batch_end.isoformat(),
            # volume
            "batch_size":             size,
            "n_clean":                n_clean,
            "n_quarantine":           n_quarantine,
            "quarantine_by_reason":   quarantine_by_reason,
            # quality
            "quarantine_rate_pct":    quarantine_rate,
            "n_checks":               len(events),
            "n_breaches":             n_breach,
            # performance
            "processing_seconds":     processing_seconds,
            "pipeline_lag_seconds":   pipeline_lag_seconds,
        }
        summary_df = spark.createDataFrame(
            [(json.dumps(batch_summary),)], ["value"]
        )
        (summary_df.write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("topic", OBS_TOPIC)
            .save())

        print(f"[dq] batch={batch_id} size={size} "
              f"clean={n_clean} quarantined={n_quarantine} ({quarantine_rate:.1f}%) "
              f"checks={len(events)} breaches={n_breach} "
              f"processing={processing_seconds:.2f}s lag={pipeline_lag_seconds}s")

        batch_df.unpersist()

    return handler


# ---------------------------------------------------------------------------
def main():
    spark = (SparkSession.builder
             .appName("dq_stream_job")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", SOURCE_TOPIC)
           .option("startingOffsets", "latest")
           .load())

    parsed = (raw
              .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS ingest_ts")
              .select(F.from_json("json_str", ORDER_SCHEMA).alias("o"),
                      F.col("ingest_ts"))
              .select("o.*", "ingest_ts"))

    query = (parsed.writeStream
             .foreachBatch(build_handler())
             .outputMode("append")
             .trigger(processingTime=f"{TRIGGER_SEC} seconds")
             .option("checkpointLocation", CHECKPOINT_BASE)
             .start())

    query.awaitTermination()


if __name__ == "__main__":
    main()
