# real-time-observer-poc
# Phase 0 (v2) — What changed and how to smoke test

## Summary of changes from v1

| Area | v1 | v2 |
|---|---|---|
| Spark image | `bitnami/spark:3.5.1` | `apache/spark:4.0.2-python3` |
| Scala | 2.12 | **2.13** |
| Kafka broker image | `confluentinc/cp-kafka:7.5.0` + Zookeeper | `apache/kafka:3.9.0` (**KRaft only**) |
| Zookeeper | present | **removed** |
| UI | `provectuslabs/kafka-ui` | `redpanda-data/console` |
| Spark command | `SPARK_MODE` env var | explicit `spark-class` invocation |
| Ivy cache path | `/home/spark/.ivy2` | `/root/.ivy2` (since `user: root`) |

## Start the stack

```bash
mkdir -p jobs data
docker compose up -d
docker compose ps
```

Wait until `kafka` and `schema-registry` show `(healthy)`.

## Service URLs

| Service | URL |
|---|---|
| Redpanda Console | http://localhost:8080 |
| Schema Registry | http://localhost:8081 |
| Spark Master UI | http://localhost:8090 |
| Spark Worker UI | http://localhost:8091 |
| Kafka (from host) | `localhost:9094` |
| Kafka (inside Docker) | `kafka:9092` |

## Create topics (same as v1)

```bash
for t in orders_raw orders_clean orders_quarantine observability_events; do
  docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 \
    --create --topic $t --partitions 3 --replication-factor 1
done

# TO create Topics

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --topic orders_raw --partitions 3 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --topic orders_clean --partitions 3 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --topic orders_quarantine --partitions 3 --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --topic observability_events --partitions 3 --replication-factor 1
```


> Note the binary path is now `/opt/kafka/bin/kafka-topics.sh` (Apache image), not `kafka-topics` without `.sh` (Confluent image).

## Updated smoke test — `jobs/smoke_test.py`

Same code as v1 — no changes. PySpark API is stable across 3.5 → 4.0.2 for this use case.

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("smoke_test")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "orders_raw")
      .option("startingOffsets", "earliest")
      .load())

(df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
   .writeStream
   .format("console")
   .outputMode("append")
   .option("truncate", "false")
   .start()
   .awaitTermination())
```

## Updated submit command — **critical: package coordinates changed**

Spark 4.0.2 is Scala **2.13** only, so the Kafka connector suffix is `_2.13`:

## To run smoke test
```bash
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2 /opt/jobs/smoke_test.py
```

First run downloads transitive dependencies (`kafka-clients`, `commons-pool2`, etc.) — takes ~2 min. Cached in the `spark-ivy-cache` volume afterward.

## Produce a test message from host

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 \
  --topic orders_raw
> {"order_id":"1","customer_id":"c-101","amount":42}
> {"order_id":"2","customer_id":"c-102","amount":99}
```

Within seconds, the Spark console-sink stream should print both rows. Foundation is verified.

## Troubleshooting additions for v2

| Symptom | Cause | Fix |
|---|---|---|
| `NoClassDefFoundError: scala/...` on submit | Using `_2.12` package with Spark 4.0 | Use `_2.13:4.0.2` |
| Kafka container exits with `InconsistentClusterIdException` | Reused a stale data volume from v1 | `docker compose down -v` then `up -d` |
| Redpanda Console shows "no brokers" | Kafka not healthy yet at startup | Wait for `kafka` healthcheck, then restart console: `docker compose restart redpanda-console` |
| Worker memory warning (<=1G) | Default was inherited | Confirm `SPARK_WORKER_MEMORY: "2G"` in compose |
| Permission denied writing to `/opt/jobs` | Host directory owned by your UID, Spark runs as root inside | With `user: root` in compose, writes go in as root — acceptable for POC |
