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