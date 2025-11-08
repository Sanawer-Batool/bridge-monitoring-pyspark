"""
Bronze Layer - Raw Data Ingestion
Reads streaming sensor data and persists to Bronze layer with minimal transformation
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType


def create_spark_session(app_name="BronzeIngestion"):
    """Create Spark session with streaming configurations"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()


def get_sensor_schema():
    """Define explicit schema for sensor events"""
    return StructType([
        StructField("event_time", StringType(), False),
        StructField("bridge_id", StringType(), False),
        StructField("sensor_type", StringType(), False),
        StructField("value", DoubleType(), False),
        StructField("ingest_time", StringType(), True)
    ])


def ingest_stream(spark, input_path, output_path, checkpoint_path, sensor_type):
    """
    Read stream from input path and write to Bronze layer
    
    Args:
        spark: SparkSession
        input_path: Path to read streaming JSON files
        output_path: Path to write Bronze parquet files
        checkpoint_path: Checkpoint location for fault tolerance
        sensor_type: Type of sensor (for logging)
    """
    schema = get_sensor_schema()
    
    # Read streaming data
    df_stream = spark.readStream \
        .format("json") \
        .schema(schema) \
        .option("maxFilesPerTrigger", "10") \
        .load(input_path)
    
    # Basic transformations for Bronze layer
    df_bronze = df_stream \
        .withColumn("event_time_ts", to_timestamp(col("event_time"))) \
        .withColumn("ingest_time_ts", to_timestamp(col("ingest_time"))) \
        .withColumn("processing_time", current_timestamp()) \
        .withColumn("partition_date", col("event_time_ts").cast("date"))
    
    # Data quality - filter out records with critical nulls (route to quarantine)
    df_valid = df_bronze.filter(
        col("event_time_ts").isNotNull() & 
        col("bridge_id").isNotNull() & 
        col("value").isNotNull()
    )
    
    df_invalid = df_bronze.filter(
        col("event_time_ts").isNull() | 
        col("bridge_id").isNull() | 
        col("value").isNull()
    )
    
    # Write valid records to Bronze
    query_valid = df_valid.writeStream \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .partitionBy("partition_date") \
        .outputMode("append") \
        .start()
    
    # Write invalid records to quarantine
    query_invalid = df_invalid.writeStream \
        .format("parquet") \
        .option("path", f"{output_path}_quarantine") \
        .option("checkpointLocation", f"{checkpoint_path}_quarantine") \
        .outputMode("append") \
        .start()
    
    print(f"Started Bronze ingestion for {sensor_type}")
    print(f"  Input: {input_path}")
    print(f"  Output: {output_path}")
    print(f"  Checkpoint: {checkpoint_path}")
    
    return query_valid, query_invalid


def main():
    """Main entry point for Bronze layer ingestion"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Define paths
    base_input = "streams"
    base_output = "bronze"
    base_checkpoint = "checkpoints/bronze"
    
    # Ingest three sensor streams
    sensors = ["bridge_temperature", "bridge_vibration", "bridge_tilt"]
    queries = []
    
    for sensor in sensors:
        input_path = f"{base_input}/{sensor}"
        output_path = f"{base_output}/{sensor}"
        checkpoint_path = f"{base_checkpoint}/{sensor}"
        
        query_valid, query_invalid = ingest_stream(
            spark, input_path, output_path, checkpoint_path, sensor
        )
        queries.extend([query_valid, query_invalid])
    
    print("\n" + "="*60)
    print("Bronze Layer Ingestion Started")
    print("="*60)
    print(f"Monitoring {len(sensors)} sensor streams...")
    print("Press Ctrl+C to stop")
    print("="*60 + "\n")
    
    # Wait for all streams to finish (or Ctrl+C)
    try:
        for query in queries:
            query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping Bronze ingestion...")
        for query in queries:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
