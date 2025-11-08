"""
Silver Layer - Data Enrichment and Quality Checks
Enriches Bronze data with static metadata and applies data quality expectations
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit


def create_spark_session(app_name="SilverEnrichment"):
    """Create Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()


def load_bridge_metadata(spark, metadata_path="metadata/bridges.csv"):
    """Load static bridge metadata"""
    return spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(metadata_path)


def apply_data_quality_checks(df, sensor_type):
    """
    Apply data quality expectations based on sensor type
    
    Returns: (valid_df, rejected_df)
    """
    # Define valid ranges for each sensor type
    ranges = {
        "temperature": (-40, 80),  # Celsius
        "vibration": (0, 100),      # Hz
        "tilt": (0, 90)             # Degrees
    }
    
    min_val, max_val = ranges.get(sensor_type.lower(), (None, None))
    
    # Add validation columns
    df_with_checks = df \
        .withColumn("value_in_range", 
                    when((col("value") >= min_val) & (col("value") <= max_val), True)
                    .otherwise(False)) \
        .withColumn("has_valid_timestamp", col("event_time_ts").isNotNull()) \
        .withColumn("has_metadata", col("bridge_name").isNotNull())
    
    # Overall validation flag
    df_with_checks = df_with_checks.withColumn(
        "is_valid",
        col("value_in_range") & col("has_valid_timestamp") & col("has_metadata")
    )
    
    # Split into valid and rejected
    valid_df = df_with_checks.filter(col("is_valid") == True) \
        .drop("value_in_range", "has_valid_timestamp", "has_metadata", "is_valid")
    
    rejected_df = df_with_checks.filter(col("is_valid") == False) \
        .withColumn("rejection_reason",
                    when(~col("value_in_range"), lit(f"Value out of range [{min_val}, {max_val}]"))
                    .when(~col("has_valid_timestamp"), lit("Invalid timestamp"))
                    .when(~col("has_metadata"), lit("No metadata match"))
                    .otherwise(lit("Unknown")))
    
    return valid_df, rejected_df


def enrich_stream(spark, bronze_path, metadata_df, silver_path, 
                  checkpoint_path, rejected_path, sensor_type):
    """
    Read Bronze stream, enrich with metadata, apply quality checks
    
    Args:
        spark: SparkSession
        bronze_path: Path to Bronze layer data
        metadata_df: Static bridge metadata DataFrame
        silver_path: Path to write validated/enriched data
        checkpoint_path: Checkpoint location
        rejected_path: Path for rejected records
        sensor_type: Sensor type name
    """
    # Read Bronze stream
    df_bronze_stream = spark.readStream \
        .format("parquet") \
        .load(bronze_path)
    
    # Perform stream-static join
    df_enriched = df_bronze_stream.join(
        metadata_df,
        df_bronze_stream.bridge_id == metadata_df.bridge_id,
        "left"
    ).select(
        df_bronze_stream["*"],
        metadata_df["bridge_name"],
        metadata_df["location"],
        metadata_df["installation_date"],
        metadata_df["bridge_type"]
    )
    
    # Apply data quality checks
    df_valid, df_rejected = apply_data_quality_checks(df_enriched, sensor_type)
    
    # Write valid records to Silver
    query_valid = df_valid.writeStream \
        .format("parquet") \
        .option("path", silver_path) \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode("append") \
        .start()
    
    # Write rejected records
    query_rejected = df_rejected.writeStream \
        .format("parquet") \
        .option("path", rejected_path) \
        .option("checkpointLocation", f"{checkpoint_path}_rejected") \
        .outputMode("append") \
        .start()
    
    print(f"Started Silver enrichment for {sensor_type}")
    print(f"  Bronze input: {bronze_path}")
    print(f"  Silver output: {silver_path}")
    print(f"  Rejected output: {rejected_path}")
    
    return query_valid, query_rejected


def main():
    """Main entry point for Silver layer"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load static metadata
    print("Loading bridge metadata...")
    metadata_df = load_bridge_metadata(spark)
    metadata_df.cache()
    print(f"Loaded metadata for {metadata_df.count()} bridges")
    metadata_df.show(truncate=False)
    
    # Define paths
    sensors = [
        ("bridge_temperature", "temperature"),
        ("bridge_vibration", "vibration"),
        ("bridge_tilt", "tilt")
    ]
    
    queries = []
    
    for sensor_name, sensor_type in sensors:
        bronze_path = f"bronze/{sensor_name}"
        silver_path = f"silver/{sensor_name}"
        rejected_path = f"silver/{sensor_name}_rejected"
        checkpoint_path = f"checkpoints/silver/{sensor_name}"
        
        query_valid, query_rejected = enrich_stream(
            spark, bronze_path, metadata_df, silver_path,
            checkpoint_path, rejected_path, sensor_type
        )
        queries.extend([query_valid, query_rejected])
    
    print("\n" + "="*60)
    print("Silver Layer Enrichment Started")
    print("="*60)
    print("Enriching streams with bridge metadata...")
    print("Applying data quality checks...")
    print("Press Ctrl+C to stop")
    print("="*60 + "\n")
    
    # Wait for all streams
    try:
        for query in queries:
            query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping Silver enrichment...")
        for query in queries:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
