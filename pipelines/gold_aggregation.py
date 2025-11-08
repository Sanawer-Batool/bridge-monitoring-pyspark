"""
Gold Layer - Windowed Aggregations and Stream-Stream Joins
Computes 1-minute tumbling window metrics and joins all sensor streams
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max as spark_max, window, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType


def create_spark_session(app_name="GoldAggregation"):
    """Create Spark session with streaming configurations"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.streaming.stateStore.providerClass", 
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
        .getOrCreate()


def create_windowed_aggregation(df_stream, window_duration="1 minute", 
                                 watermark_duration="2 minutes"):
    """
    Apply watermark and window aggregation to a stream
    
    Args:
        df_stream: Input streaming DataFrame
        window_duration: Size of tumbling window
        watermark_duration: How late data can arrive
    
    Returns: DataFrame with watermark and window applied
    """
    # Apply watermark on event time
    df_watermarked = df_stream.withWatermark("event_time_ts", watermark_duration)
    
    return df_watermarked


def aggregate_temperature(df_stream):
    """Aggregate temperature stream - compute average per window"""
    df_windowed = create_windowed_aggregation(df_stream)
    
    return df_windowed.groupBy(
        col("bridge_id"),
        col("bridge_name"),
        window(col("event_time_ts"), "1 minute")
    ).agg(
        avg("value").alias("avg_temperature")
    ).select(
        col("bridge_id"),
        col("bridge_name"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_temperature")
    )


def aggregate_vibration(df_stream):
    """Aggregate vibration stream - compute maximum per window"""
    df_windowed = create_windowed_aggregation(df_stream)
    
    return df_windowed.groupBy(
        col("bridge_id"),
        col("bridge_name"),
        window(col("event_time_ts"), "1 minute")
    ).agg(
        spark_max("value").alias("max_vibration")
    ).select(
        col("bridge_id"),
        col("bridge_name"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("max_vibration")
    )


def aggregate_tilt(df_stream):
    """Aggregate tilt stream - compute maximum per window"""
    df_windowed = create_windowed_aggregation(df_stream)
    
    return df_windowed.groupBy(
        col("bridge_id"),
        col("bridge_name"),
        window(col("event_time_ts"), "1 minute")
    ).agg(
        spark_max("value").alias("max_tilt_angle")
    ).select(
        col("bridge_id"),
        col("bridge_name"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("max_tilt_angle")
    )


def join_metrics(df_temp, df_vib, df_tilt):
    """
    Perform stream-stream joins on windowed aggregations
    
    All streams must have watermarks and matching window definitions
    """
    # Join temperature and vibration
    df_joined = df_temp.join(
        df_vib,
        (df_temp.bridge_id == df_vib.bridge_id) &
        (df_temp.window_start == df_vib.window_start) &
        (df_temp.window_end == df_vib.window_end),
        "inner"
    ).select(
        df_temp.bridge_id,
        df_temp.bridge_name,
        df_temp.window_start,
        df_temp.window_end,
        df_temp.avg_temperature,
        df_vib.max_vibration
    )
    
    # Join with tilt
    df_final = df_joined.join(
        df_tilt,
        (df_joined.bridge_id == df_tilt.bridge_id) &
        (df_joined.window_start == df_tilt.window_start) &
        (df_joined.window_end == df_tilt.window_end),
        "inner"
    ).select(
        df_joined.bridge_id,
        df_joined.bridge_name,
        df_joined.window_start,
        df_joined.window_end,
        df_joined.avg_temperature,
        df_joined.max_vibration,
        df_tilt.max_tilt_angle
    ).withColumn("processing_time", current_timestamp())
    
    return df_final


def main():
    """Main entry point for Gold layer aggregation"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("Starting Gold layer aggregation pipeline...")
    
    # Read Silver streams
    df_temp_stream = spark.readStream \
        .format("parquet") \
        .load("silver/bridge_temperature")
    
    df_vib_stream = spark.readStream \
        .format("parquet") \
        .load("silver/bridge_vibration")
    
    df_tilt_stream = spark.readStream \
        .format("parquet") \
        .load("silver/bridge_tilt")
    
    print("✓ Loaded Silver streams")
    
    # Create windowed aggregations
    df_temp_agg = aggregate_temperature(df_temp_stream)
    df_vib_agg = aggregate_vibration(df_vib_stream)
    df_tilt_agg = aggregate_tilt(df_tilt_stream)
    
    print("✓ Created windowed aggregations (1-minute tumbling windows)")
    print("✓ Applied 2-minute watermarks")
    
    # Join all metrics
    df_bridge_metrics = join_metrics(df_temp_agg, df_vib_agg, df_tilt_agg)
    
    print("✓ Configured stream-stream joins")
    
    # Write to Gold layer
    query = df_bridge_metrics.writeStream \
        .format("parquet") \
        .option("path", "gold/bridge_metrics") \
        .option("checkpointLocation", "checkpoints/gold/bridge_metrics") \
        .outputMode("append") \
        .start()
    
    # Also write to console for monitoring (limited rows)
    query_console = df_bridge_metrics.writeStream \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "5") \
        .outputMode("append") \
        .start()
    
    print("\n" + "="*80)
    print("Gold Layer Aggregation Started")
    print("="*80)
    print("Computing 1-minute windowed metrics:")
    print("  • Average Temperature")
    print("  • Maximum Vibration")
    print("  • Maximum Tilt Angle")
    print("\nOutput: gold/bridge_metrics")
    print("Checkpoint: checkpoints/gold/bridge_metrics")
    print("\nPress Ctrl+C to stop")
    print("="*80 + "\n")
    
    # Wait for streams
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping Gold aggregation...")
        query.stop()
        query_console.stop()
        spark.stop()


if __name__ == "__main__":
    main()
