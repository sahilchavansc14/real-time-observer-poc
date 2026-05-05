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
def to_obs_event(check: dict, batch_id: int, batch_size: int) -> dict:
    attrs  = (check.get("attributes") or {}) or {}
    status = "PASS" if check.get("outcome") == "pass" else "BREACH"
    sev    = attrs.get("severity", "warn").upper() if status == "BREACH" else "INFO"
    return {
        "event_id":     str(uuid.uuid4()),
        "pipeline_id":  "orders-ingestion-pipeline",
        "batch_id":     str(batch_id),
        "entity":       f"kafka.{SOURCE_TOPIC}",
        "metric":       check.get("name"),
        "dq_dimension": attrs.get("dimension"),
        "rule_id":      attrs.get("rule_id"),
        "value":        check.get("value"),
        "threshold":    check.get("threshold"),
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

        batch_df = batch_df.persist()
        size = batch_df.count()
        if size == 0:
            batch_df.unpersist()
            return

        # --- 1. Record-level validation: branch to clean vs quarantine ------
        bad_cond = (
            F.col("order_id").isNull() |
            F.col("customer_id").isNull() |
            F.col("quantity").isNull() |
            (F.col("quantity") < 1) | (F.col("quantity") > 1000) |
            (F.col("email").isNotNull() & ~F.col("email").rlike(EMAIL_RE))
        )

        reason = (
            F.when(F.col("order_id").isNull(),     "NULL_ORDER_ID")
             .when(F.col("customer_id").isNull(),  "NULL_CUSTOMER_ID")
             .when(F.col("quantity").isNull(),     "NULL_QUANTITY")
             .when((F.col("quantity") < 1) | (F.col("quantity") > 1000), "INVALID_QUANTITY_RANGE")
             .when(F.col("email").isNotNull() & ~F.col("email").rlike(EMAIL_RE), "INVALID_EMAIL")
             .otherwise("OK")
        )

        annotated = (batch_df
                     .withColumn("_is_bad", bad_cond)
                     .withColumn("_reason", reason))

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
        print(f"[dq] batch={batch_id} size={size} "
              f"clean={n_clean} quarantined={n_quarantine} "
              f"checks={len(events)} breaches={n_breach}")

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
