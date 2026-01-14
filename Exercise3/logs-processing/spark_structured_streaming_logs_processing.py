from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lower, count, desc
from pyspark.sql.types import StructType, StructField, StringType, LongType

# 1. Configuration & Session Setup
CHECKPOINT_PATH = "/tmp/spark-checkpoints/logs-processing"

spark = (
    SparkSession.builder
    .appName("LogsProcessor")
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# 2. Define schema
schema = StructType([
    StructField("timestamp", LongType()), 
    StructField("status", StringType()),
    StructField("severity", StringType()),
    StructField("source_ip", StringType()),
    StructField("user_id", StringType()),
    StructField("content", StringType())
])

# 3. Read Stream
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "logs")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# 4. Processing, Filtering & Aggregation
# We filter first to keep the state store small, then group and count.
analysis_df = (
    raw_df.select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .filter(
        (lower(col("content")).contains("vulnerability")) & 
        (col("severity") == "High")
    )
    .groupBy("source_ip")
    .agg(count("*").alias("match_count"))
    .orderBy(desc("match_count"))
)

# 5. Writing
query = (
    analysis_df.writeStream
    .outputMode("complete") 
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()