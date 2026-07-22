import os
import sys
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, to_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType
)

# ── 1. Spark session ────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("FootballPerformanceRadar")
    .config("spark.driver.memory", "512m")
    .config("spark.executor.memory", "1g")        # increase from 512m
    .config("spark.executor.memoryOverhead", "512m") # add overhead
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.driver.host", "spark")          # use container name not 127.0.0.1
    .config("spark.driver.bindAddress", "0.0.0.0")
    .config("spark.cassandra.connection.host", "cassandra")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ── 2. Schema ────────────────────────────────────────────────────
schema = StructType([
    StructField("fixture_id",  IntegerType(), True),
    StructField("minute",      IntegerType(), True),
    StructField("event_type",  StringType(),  True),
    StructField("detail",      StringType(),  True),
    StructField("team",        StringType(),  True),
    StructField("player",      StringType(),  True),
    StructField("replayed_at", StringType(),  True),
])

# ── 3. UDF: generate UUID per event ─────────────────────────────
#generate_uuid = udf(lambda: str(uuid.uuid4()), StringType())

# ── 4. Read from Kafka ───────────────────────────────────────────
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")  # was localhost:9092
    .option("subscribe", "worldcup_match_events")
    .option("startingOffsets", "earliest")
    .load()
)

# ── 5. Parse JSON ────────────────────────────────────────────────
parsed = (
    raw_stream
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_id", expr("uuid()"))
    .withColumn("replayed_at", to_timestamp(col("replayed_at")))
)

# ── 6. Write to Cassandra ────────────────────────────────────────
def write_to_cassandra(batch_df, batch_id):
    print(f"Writing batch {batch_id} — {batch_df.count()} rows")
    (
        batch_df.write
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", "football_radar")
        .option("table", "match_events")
        .mode("append")
        .save()
    )

query = (
    parsed.writeStream
    .foreachBatch(write_to_cassandra)
    .option("checkpointLocation", "./checkpoints/match_events")
    .start()
)

print("Spark consumer started — waiting for events...")
query.awaitTermination()