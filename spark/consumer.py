import os
import sys
import subprocess

# Set all environment variables BEFORE any PySpark imports
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["SPARK_LOCAL_DIRS"] = r"C:\tmp\spark"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# ── Diagnostic: verify Java is reachable before starting Spark ──
java_home = os.environ.get("JAVA_HOME", "NOT SET")
print(f"JAVA_HOME:  {java_home}")
print(f"Python:     {sys.executable}")

java_exe = os.path.join(java_home, "bin", "java.exe")
print(f"Java exe:   {java_exe}")
print(f"Java exists: {os.path.exists(java_exe)}")

try:
    result = subprocess.run(
        [java_exe, "-version"],
        capture_output=True, text=True
    )
    print(f"Java exit code: {result.returncode}")
    print(f"Java version:   {result.stderr.strip()}")
except Exception as e:
    print(f"Java launch failed: {e}")

print("-" * 60)

import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType
)



# ── 1. Spark session ────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("FootballPerformanceRadar")
    .config("spark.driver.memory", "512m")
    .config("spark.executor.memory", "512m")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0"
    )
    .config("spark.cassandra.connection.host", "localhost")
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
generate_uuid = udf(lambda: str(uuid.uuid4()), StringType())

# ── 4. Read from Kafka ───────────────────────────────────────────
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "worldcup_match_events")
    .option("startingOffsets", "earliest")
    .load()
)

# ── 5. Parse JSON ────────────────────────────────────────────────
parsed = (
    raw_stream
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_id", generate_uuid())
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