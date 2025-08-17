# Filename: streaming_patterns.py
"""
Real-time Streaming Patterns
Production patterns for Kafka and Spark Streaming
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
import json
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQuery

# =====================================================
# 1. KAFKA PRODUCER PATTERNS
# =====================================================

class ResilientKafkaProducer:
    """
    Production-ready Kafka producer with retry logic and monitoring.
    """
    def __init__(self, bootstrap_servers: str, topic: str):
        from kafka import KafkaProducer
        from kafka.errors import KafkaError
        
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Wait for all replicas
            retries=3,
            max_in_flight_requests_per_connection=1,  # Ensure ordering
            compression_type='snappy',
            batch_size=16384,
            linger_ms=10,
            buffer_memory=33554432  # 32MB
        )
        self.metrics = {"sent": 0, "failed": 0}
    
    def send_with_retry(self, key: str, value: Dict, max_retries: int = 3):
        """Send message with exponential backoff retry."""
        import time
        
        for attempt in range(max_retries):
            try:
                future = self.producer.send(
                    self.topic,
                    key=key,
                    value=value
                )
                record_metadata = future.get(timeout=10)
                self.metrics["sent"] += 1
                
                logging.info(f"Message sent to {record_metadata.topic} "
                           f"partition {record_metadata.partition} "
                           f"offset {record_metadata.offset}")
                return record_metadata
                
            except Exception as e:
                wait_time = 2 ** attempt  # Exponential backoff
                logging.warning(f"Attempt {attempt + 1} failed: {e}. "
                              f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                
                if attempt == max_retries - 1:
                    self.metrics["failed"] += 1
                    logging.error(f"Failed to send message after {max_retries} attempts")
                    raise
    
    def send_batch(self, messages: List[Dict]):
        """Send batch of messages efficiently."""
        for msg in messages:
            key = msg.get("key", str(msg.get("id", "")))
            value = msg.get("value", msg)
            self.producer.send(self.topic, key=key, value=value)
        
        # Flush to ensure all messages are sent
        self.producer.flush()
        self.metrics["sent"] += len(messages)
    
    def close(self):
        """Gracefully close producer."""
        self.producer.flush()
        self.producer.close()
        logging.info(f"Producer closed. Metrics: {self.metrics}")

# =====================================================
# 2. KAFKA CONSUMER PATTERNS
# =====================================================

class FaultTolerantKafkaConsumer:
    """
    Kafka consumer with offset management and error handling.
    """
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str):
        from kafka import KafkaConsumer
        
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            enable_auto_commit=False,  # Manual commit for exactly-once
            auto_offset_reset='earliest',
            max_poll_records=100,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )
        self.processed_count = 0
        self.error_count = 0
    
    def consume_with_checkpointing(self, process_func, checkpoint_interval: int = 100):
        """
        Consume messages with periodic checkpointing.
        """
        batch = []
        
        for message in self.consumer:
            try:
                # Add to batch
                batch.append(message)
                
                # Process batch when it reaches checkpoint interval
                if len(batch) >= checkpoint_interval:
                    self._process_batch(batch, process_func)
                    batch = []
                    
            except Exception as e:
                logging.error(f"Error processing message: {e}")
                self.error_count += 1
                # Decide whether to continue or break based on error type
                
    def _process_batch(self, batch: List, process_func):
        """Process a batch of messages and commit offsets."""
        try:
            # Process all messages in batch
            for message in batch:
                process_func(message.value)
                self.processed_count += 1
            
            # Commit offsets after successful processing
            self.consumer.commit()
            logging.info(f"Committed offsets for {len(batch)} messages")
            
        except Exception as e:
            logging.error(f"Batch processing failed: {e}")
            # Don't commit - messages will be reprocessed
            raise

# =====================================================
# 3. SPARK STRUCTURED STREAMING PATTERNS
# =====================================================

class SparkStreamingPatterns:
    """
    Common patterns for Spark Structured Streaming.
    """
    
    @staticmethod
    def create_streaming_session(app_name: str = "StreamingApp") -> SparkSession:
        """Create optimized Spark session for streaming."""
        return SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.streaming.backpressure.enabled", "true") \
            .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.sql.streaming.stateStore.maintenance.interval", "60s") \
            .getOrCreate()
    
    @staticmethod
    def read_kafka_stream(
        spark: SparkSession,
        bootstrap_servers: str,
        topic: str,
        schema: StructType
    ) -> DataFrame:
        """Read from Kafka with schema."""
        return spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", 10000) \
            .option("kafka.session.timeout.ms", "30000") \
            .load() \
            .select(
                F.col("key").cast("string"),
                F.from_json(F.col("value").cast("string"), schema).alias("data"),
                F.col("timestamp")
            ) \
            .select("key", "data.*", "timestamp")
    
    @staticmethod
    def sliding_window_aggregation(
        stream_df: DataFrame,
        timestamp_col: str,
        window_duration: str = "5 minutes",
        slide_duration: str = "1 minute",
        group_cols: List[str] = None
    ) -> DataFrame:
        """
        Sliding window aggregation pattern.
        """
        # Add watermark for late data handling
        watermarked = stream_df.withWatermark(timestamp_col, "10 minutes")
        
        # Define window
        windowed = watermarked.groupBy(
            F.window(F.col(timestamp_col), window_duration, slide_duration),
            *group_cols if group_cols else []
        )
        
        # Perform aggregations
        return windowed.agg(
            F.count("*").alias("event_count"),
            F.avg("value").alias("avg_value"),
            F.max("value").alias("max_value"),
            F.min("value").alias("min_value"),
            F.collect_list("id").alias("event_ids")
        ).select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "*"
        ).drop("window")
    
    @staticmethod
    def session_window_pattern(
        stream_df: DataFrame,
        timestamp_col: str,
        session_gap: str = "10 minutes",
        user_col: str = "user_id"
    ) -> DataFrame:
        """
        Sessionization using session windows.
        """
        return stream_df \
            .withWatermark(timestamp_col, "30 minutes") \
            .groupBy(
                F.session_window(F.col(timestamp_col), session_gap),
                F.col(user_col)
            ).agg(
                F.min(timestamp_col).alias("session_start"),
                F.max(timestamp_col).alias("session_end"),
                F.count("*").alias("event_count"),
                F.collect_list("event_type").alias("events")
            )
    
    @staticmethod
    def deduplication_pattern(
        stream_df: DataFrame,
        unique_cols: List[str],
        timestamp_col: str,
        watermark: str = "1 hour"
    ) -> DataFrame:
        """
        Deduplicate streaming data.
        """
        return stream_df \
            .withWatermark(timestamp_col, watermark) \
            .dropDuplicates(unique_cols)
    
    @staticmethod
    def stream_to_stream_join(
        stream1: DataFrame,
        stream2: DataFrame,
        join_keys: List[str],
        timestamp_col: str = "timestamp",
        watermark: str = "10 minutes"
    ) -> DataFrame:
        """
        Join two streams with watermarking.
        """
        # Add watermarks to both streams
        stream1_wm = stream1.withWatermark(timestamp_col, watermark)
        stream2_wm = stream2.withWatermark(timestamp_col, watermark)
        
        # Define join condition with time constraints
        join_condition = [stream1_wm[key] == stream2_wm[key] for key in join_keys]
        
        # Add time window constraint for join
        time_constraint = F.expr(f"""
            stream2.{timestamp_col} >= stream1.{timestamp_col} - interval 5 minutes AND
            stream2.{timestamp_col} <= stream1.{timestamp_col} + interval 5 minutes
        """)
        
        join_condition.append(time_constraint)
        
        return stream1_wm.join(
            stream2_wm,
            join_condition,
            "inner"
        )

# =====================================================
# 4. STATEFUL PROCESSING PATTERNS
# =====================================================

def stateful_deduplication(
    spark: SparkSession,
    stream_df: DataFrame,
    unique_id_col: str,
    state_timeout: str = "1 hour"
) -> DataFrame:
    """
    Stateful deduplication with timeout.
    """
    from pyspark.sql.streaming.state import GroupState
    
    def dedupe_func(key, values, state: GroupState):
        """
        State function for deduplication.
        """
        if state.hasTimedOut:
            # Clean up old state
            state.remove()
            return []
        
        if state.exists:
            # Already seen this ID, skip
            return []
        else:
            # First time seeing this ID
            state.update(True)
            state.setTimeoutDuration(state_timeout)
            return values
    
    # Apply stateful operation
    return stream_df.groupByKey(
        lambda row: row[unique_id_col]
    ).flatMapGroupsWithState(
        dedupe_func,
        outputStructType=stream_df.schema,
        stateStructType=StructType([StructField("seen", BooleanType())]),
        outputMode="append",
        timeoutConf="ProcessingTimeTimeout"
    )

# =====================================================
# 5. OUTPUT SINK PATTERNS
# =====================================================

class StreamingSinkPatterns:
    """
    Patterns for writing streaming data to various sinks.
    """
    
    @staticmethod
    def write_to_delta(
        stream_df: DataFrame,
        path: str,
        checkpoint_path: str,
        partition_cols: List[str] = None
    ) -> StreamingQuery:
        """
        Write stream to Delta Lake with exactly-once semantics.
        """
        writer = stream_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .option("mergeSchema", "true")
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        return writer.start(path)
    
    @staticmethod
    def write_to_kafka(
        stream_df: DataFrame,
        bootstrap_servers: str,
        topic: str,
        checkpoint_path: str
    ) -> StreamingQuery:
        """
        Write stream back to Kafka.
        """
        return stream_df \
            .selectExpr(
                "CAST(key AS STRING)",
                "to_json(struct(*)) AS value"
            ) \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("topic", topic) \
            .option("checkpointLocation", checkpoint_path) \
            .outputMode("append") \
            .start()
    
    @staticmethod
    def write_to_foreach_batch(
        stream_df: DataFrame,
        batch_func,
        checkpoint_path: str,
        trigger_interval: str = "1 minute"
    ) -> StreamingQuery:
        """
        Custom processing with foreachBatch.
        """
        def process_batch(batch_df, epoch_id):
            # Cache for multiple operations
            batch_df.cache()
            
            try:
                # Custom processing
                batch_func(batch_df, epoch_id)
                
                # Log metrics
                count = batch_df.count()
                logging.info(f"Processed {count} records in batch {epoch_id}")
                
            finally:
                batch_df.unpersist()
        
        return stream_df.writeStream \
            .foreachBatch(process_batch) \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(processingTime=trigger_interval) \
            .start()

# =====================================================
# 6. MONITORING AND ALERTING
# =====================================================

class StreamMonitor:
    """
    Monitor streaming job health and metrics.
    """
    def __init__(self, query: StreamingQuery):
        self.query = query
        self.alert_thresholds = {
            "processing_delay_ms": 5000,
            "input_rate": 10000,
            "error_rate": 0.01
        }
    
    def check_health(self) -> Dict[str, Any]:
        """Check streaming job health."""
        if not self.query.isActive:
            return {"status": "FAILED", "error": "Query not active"}
        
        progress = self.query.lastProgress
        
        if not progress:
            return {"status": "STARTING", "message": "No progress yet"}
        
        metrics = {
            "status": "HEALTHY",
            "input_rate": progress.get("inputRowsPerSecond", 0),
            "processing_rate": progress.get("processedRowsPerSecond", 0),
            "batch_duration": progress.get("durationMs", {}).get("triggerExecution", 0),
            "state_rows": progress.get("stateOperators", [{}])[0].get("numRowsTotal", 0)
        }
        
        # Check thresholds
        if metrics["batch_duration"] > self.alert_thresholds["processing_delay_ms"]:
            metrics["status"] = "DEGRADED"
            metrics["alert"] = "High processing delay"
        
        return metrics
    
    def get_streaming_metrics(self) -> Dict[str, Any]:
        """Get detailed streaming metrics."""
        progress = self.query.lastProgress
        
        return {
            "id": self.query.id,
            "run_id": self.query.runId,
            "batch_id": progress.get("batchId", 0),
            "timestamp": progress.get("timestamp", ""),
            "sources": progress.get("sources", []),
            "sink": progress.get("sink", {}),
            "state_operators": progress.get("stateOperators", []),
            "duration_ms": progress.get("durationMs", {})
        }

# =====================================================
# 7. COMPLETE STREAMING PIPELINE EXAMPLE
# =====================================================

def build_streaming_pipeline():
    """
    Complete example of a production streaming pipeline.
    """
    # Initialize Spark
    spark = SparkStreamingPatterns.create_streaming_session("StreamingPipeline")
    
    # Define schema
    schema = StructType([
        StructField("id", StringType()),
        StructField("user_id", StringType()),
        StructField("event_type", StringType()),
        StructField("value", DoubleType()),
        StructField("timestamp", TimestampType())
    ])
    
    # Read from Kafka
    raw_stream = SparkStreamingPatterns.read_kafka_stream(
        spark,
        "localhost:9092",
        "events",
        schema
    )
    
    # Deduplicate
    deduped = SparkStreamingPatterns.deduplication_pattern(
        raw_stream,
        ["id"],
        "timestamp",
        "1 hour"
    )
    
    # Window aggregation
    windowed = SparkStreamingPatterns.sliding_window_aggregation(
        deduped,
        "timestamp",
        "5 minutes",
        "1 minute",
        ["user_id"]
    )
    
    # Write to Delta Lake
    query = StreamingSinkPatterns.write_to_delta(
        windowed,
        "/data/delta/aggregated",
        "/checkpoints/aggregated",
        ["window_start"]
    )
    
    # Monitor
    monitor = StreamMonitor(query)
    
    # Check health periodically
    import time
    while query.isActive:
        health = monitor.check_health()
        print(f"Pipeline health: {health}")
        time.sleep(30)
    
    query.awaitTermination()

if __name__ == "__main__":
    build_streaming_pipeline()