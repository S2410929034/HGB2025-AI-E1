from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, count, window, from_unixtime, lower
)
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Configuration
CHECKPOINT_PATH = "/tmp/spark-checkpoints/crash-monitoring"
WINDOW_DURATION = "10 seconds"
WATERMARK_DELAY = "30 seconds"  # Allow 30 seconds for late arrivals

# Create Spark Session
spark = (
    SparkSession.builder
    .appName("CrashMonitoring")
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# Define JSON schema for incoming logs
schema = StructType([
    StructField("timestamp", LongType()),      # Unix timestamp in milliseconds
    StructField("status", StringType()),
    StructField("severity", StringType()),
    StructField("source_ip", StringType()),
    StructField("user_id", StringType()),
    StructField("content", StringType())
])

# Read from Kafka topic
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "logs")
    .option("startingOffsets", "latest")  # Start from latest to avoid backlog
    .option("failOnDataLoss", "false")
    .load()
)

# Parse JSON and extract fields
parsed_stream = (
    raw_stream
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
)

# Convert Unix timestamp (milliseconds) to timestamp type for windowing
# The timestamp field is critical - it defines event time, not processing time
parsed_stream = parsed_stream.withColumn(
    "event_timestamp", 
    from_unixtime(col("timestamp") / 1000).cast("timestamp")
)

# Apply filters:
# 1. Content contains "crash" (case-insensitive)
# 2. Severity is either "High" or "Critical"
filtered_stream = (
    parsed_stream
    .filter(lower(col("content")).contains("crash"))
    .filter(col("severity").isin("High", "Critical"))
)

# Apply watermarking to handle late-arriving data
# Watermark = "allow events up to 30 seconds late"
# Events more than 30 seconds late will be dropped
watermarked_stream = filtered_stream.withWatermark("event_timestamp", WATERMARK_DELAY)

# Window aggregation:
# - Group by 10-second tumbling windows based on event_timestamp
# - Group by user_id within each window
# - Count crashes per user per window
windowed_aggregation = (
    watermarked_stream
    .groupBy(
        window(col("event_timestamp"), WINDOW_DURATION),
        col("user_id")
    )
    .agg(count("*").alias("crash_count"))
)

# Filter to show only users with more than 2 crashes per window
high_crash_users = windowed_aggregation.filter(col("crash_count") > 2)

# Rename window column to "Interval" for cleaner output
output_stream = (
    high_crash_users
    .select(
        col("window").alias("Interval"),
        col("user_id"),
        col("crash_count")
    )
)

# Write results to console
# OutputMode "update" is required for windowed aggregations with watermarks
query = (
    output_stream.writeStream
    .outputMode("update")  # Only output updated rows
    .format("console")
    .option("truncate", "false")
    .option("numRows", 100)  # Show up to 100 rows per batch
    .start()
)

# Keep the application running
query.awaitTermination()
