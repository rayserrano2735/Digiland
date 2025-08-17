# Filename: spark_optimizations.py
"""
PySpark Optimization Patterns
Production-ready patterns for efficient Spark jobs
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from typing import List, Dict, Optional
import logging

# =====================================================
# 1. BROADCAST JOIN PATTERN
# =====================================================

def optimized_broadcast_join(
    spark: SparkSession,
    large_df: DataFrame,
    small_df: DataFrame,
    join_keys: List[str],
    broadcast_threshold_mb: int = 100
) -> DataFrame:
    """
    Optimize joins by broadcasting small tables.
    Use when one table is significantly smaller than the other.
    """
    # Check size of small dataframe
    small_df_size_mb = small_df.count() * len(small_df.columns) * 8 / (1024 * 1024)
    
    if small_df_size_mb < broadcast_threshold_mb:
        # Broadcast the smaller dataframe
        from pyspark.sql.functions import broadcast
        result = large_df.join(broadcast(small_df), on=join_keys, how='left')
        logging.info(f"Using broadcast join - small table size: {small_df_size_mb:.2f} MB")
    else:
        # Regular join if table is too large
        result = large_df.join(small_df, on=join_keys, how='left')
        logging.warning(f"Table too large for broadcast: {small_df_size_mb:.2f} MB")
    
    return result

# =====================================================
# 2. PARTITIONING OPTIMIZATION
# =====================================================

def optimize_partitions(
    df: DataFrame,
    target_partition_size_mb: int = 128,
    min_partitions: int = 10,
    max_partitions: int = 1000
) -> DataFrame:
    """
    Optimize number of partitions based on data size.
    Target: 128MB per partition (configurable).
    """
    # Estimate dataframe size
    row_count = df.count()
    col_count = len(df.columns)
    estimated_size_mb = (row_count * col_count * 8) / (1024 * 1024)
    
    # Calculate optimal partition count
    optimal_partitions = int(estimated_size_mb / target_partition_size_mb)
    optimal_partitions = max(min_partitions, min(optimal_partitions, max_partitions))
    
    # Current partitions
    current_partitions = df.rdd.getNumPartitions()
    
    if current_partitions != optimal_partitions:
        if optimal_partitions < current_partitions:
            df = df.coalesce(optimal_partitions)
            logging.info(f"Coalesced from {current_partitions} to {optimal_partitions} partitions")
        else:
            df = df.repartition(optimal_partitions)
            logging.info(f"Repartitioned from {current_partitions} to {optimal_partitions} partitions")
    
    return df

# =====================================================
# 3. SALTING FOR SKEWED JOINS
# =====================================================

def salted_join(
    left_df: DataFrame,
    right_df: DataFrame,
    join_key: str,
    salt_range: int = 10
) -> DataFrame:
    """
    Handle data skew in joins using salting technique.
    Useful when certain keys have disproportionate number of records.
    """
    # Add salt to left dataframe
    left_salted = left_df.withColumn(
        "salt", F.floor(F.rand() * salt_range)
    ).withColumn(
        "salted_key", F.concat(F.col(join_key), F.lit("_"), F.col("salt"))
    )
    
    # Explode salt on right dataframe
    right_exploded = right_df.select(
        F.col("*"),
        F.explode(F.array([F.lit(i) for i in range(salt_range)])).alias("salt")
    ).withColumn(
        "salted_key", F.concat(F.col(join_key), F.lit("_"), F.col("salt"))
    )
    
    # Perform the join on salted keys
    result = left_salted.join(
        right_exploded, 
        on="salted_key", 
        how="inner"
    ).drop("salt", "salted_key")
    
    return result

# =====================================================
# 4. WINDOW FUNCTION OPTIMIZATION
# =====================================================

def optimized_window_aggregation(
    df: DataFrame,
    partition_cols: List[str],
    order_col: str,
    agg_col: str
) -> DataFrame:
    """
    Optimize window functions by reducing shuffles.
    """
    # Pre-partition the dataframe to reduce shuffles
    df_partitioned = df.repartition(*partition_cols)
    
    # Define window with proper frame specification
    window_spec = Window.partitionBy(*partition_cols).orderBy(order_col)
    
    # Use specific frame bounds for better performance
    window_bounded = window_spec.rowsBetween(
        Window.unboundedPreceding, 
        Window.currentRow
    )
    
    result = df_partitioned.select(
        "*",
        F.sum(agg_col).over(window_bounded).alias("running_total"),
        F.avg(agg_col).over(window_bounded).alias("running_avg"),
        F.row_number().over(window_spec).alias("row_num"),
        F.dense_rank().over(window_spec).alias("dense_rank")
    )
    
    return result

# =====================================================
# 5. CACHING STRATEGY
# =====================================================

class SmartCacheManager:
    """
    Intelligent caching strategy for Spark DataFrames.
    """
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.cached_dfs: Dict[str, DataFrame] = {}
        
    def cache_if_reused(
        self, 
        df: DataFrame, 
        name: str, 
        min_reuse_count: int = 2
    ) -> DataFrame:
        """
        Cache dataframe if it will be reused multiple times.
        """
        if name not in self.cached_dfs:
            df.cache()
            self.cached_dfs[name] = df
            logging.info(f"Cached dataframe: {name}")
        else:
            df = self.cached_dfs[name]
            logging.info(f"Using cached dataframe: {name}")
        
        return df
    
    def unpersist_all(self):
        """
        Unpersist all cached dataframes.
        """
        for name, df in self.cached_dfs.items():
            df.unpersist()
            logging.info(f"Unpersisted dataframe: {name}")
        self.cached_dfs.clear()

# =====================================================
# 6. EFFICIENT AGGREGATION
# =====================================================

def efficient_multiple_aggregations(
    df: DataFrame,
    group_cols: List[str],
    agg_specs: Dict[str, List[str]]
) -> DataFrame:
    """
    Perform multiple aggregations in a single pass.
    
    agg_specs example:
    {
        'revenue': ['sum', 'avg', 'max'],
        'quantity': ['sum', 'count']
    }
    """
    agg_expressions = []
    
    for col, funcs in agg_specs.items():
        for func in funcs:
            if func == 'sum':
                agg_expressions.append(F.sum(col).alias(f"{col}_sum"))
            elif func == 'avg':
                agg_expressions.append(F.avg(col).alias(f"{col}_avg"))
            elif func == 'max':
                agg_expressions.append(F.max(col).alias(f"{col}_max"))
            elif func == 'min':
                agg_expressions.append(F.min(col).alias(f"{col}_min"))
            elif func == 'count':
                agg_expressions.append(F.count(col).alias(f"{col}_count"))
            elif func == 'stddev':
                agg_expressions.append(F.stddev(col).alias(f"{col}_stddev"))
    
    # Single aggregation pass
    result = df.groupBy(*group_cols).agg(*agg_expressions)
    
    return result

# =====================================================
# 7. PREDICATE PUSHDOWN
# =====================================================

def optimized_filter_and_select(
    df: DataFrame,
    select_cols: List[str],
    filter_conditions: List[str]
) -> DataFrame:
    """
    Apply filters before transformations for predicate pushdown.
    """
    # Apply filters first (pushed down to data source)
    for condition in filter_conditions:
        df = df.filter(condition)
    
    # Then select only needed columns
    df = df.select(*select_cols)
    
    # This order ensures optimal predicate pushdown
    return df

# =====================================================
# 8. AVOIDING SHUFFLES
# =====================================================

def reduce_shuffles_example(spark: SparkSession):
    """
    Demonstrate techniques to minimize shuffles.
    """
    # Bad: Multiple shuffles
    # df1.groupBy('key').agg()  # Shuffle 1
    # df2.groupBy('key').agg()  # Shuffle 2
    # df1.join(df2, 'key')      # Shuffle 3
    
    # Good: Single shuffle with pre-partitioning
    df1 = spark.range(1000000).toDF("id")
    df2 = spark.range(1000000).toDF("id")
    
    # Pre-partition both dataframes
    df1_partitioned = df1.repartition("id")
    df2_partitioned = df2.repartition("id")
    
    # Now operations on same partition scheme avoid shuffles
    result = df1_partitioned.join(df2_partitioned, "id")
    
    return result

# =====================================================
# 9. HANDLING NULL VALUES EFFICIENTLY
# =====================================================

def efficient_null_handling(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Efficient strategies for handling null values.
    """
    # Method 1: Drop nulls early to reduce data volume
    df_no_nulls = df.dropna(subset=columns)
    
    # Method 2: Fill nulls with efficient default values
    fill_values = {col: 0 for col in columns}
    df_filled = df.fillna(fill_values)
    
    # Method 3: Use when/otherwise for complex null logic
    for col in columns:
        df = df.withColumn(
            col,
            F.when(F.col(col).isNull(), F.lit(0)).otherwise(F.col(col))
        )
    
    return df

# =====================================================
# 10. CONFIGURATION OPTIMIZATIONS
# =====================================================

def get_optimized_spark_session(app_name: str = "OptimizedApp") -> SparkSession:
    """
    Create Spark session with optimized configurations.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.broadcastTimeout", "600") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.autoBroadcastJoinThreshold", "104857600") \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \
        .config("spark.default.parallelism", "400") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

# =====================================================
# USAGE EXAMPLE
# =====================================================

def main():
    """
    Example usage of optimization patterns.
    """
    spark = get_optimized_spark_session("OptimizationExample")
    
    # Create sample data
    large_df = spark.range(10000000).toDF("id") \
        .withColumn("value", F.rand() * 100)
    
    small_df = spark.range(1000).toDF("id") \
        .withColumn("category", F.round(F.rand() * 10))
    
    # 1. Optimized broadcast join
    joined_df = optimized_broadcast_join(
        spark, large_df, small_df, ["id"], broadcast_threshold_mb=50
    )
    
    # 2. Optimize partitions
    optimized_df = optimize_partitions(joined_df, target_partition_size_mb=128)
    
    # 3. Cache management
    cache_manager = SmartCacheManager(spark)
    cached_df = cache_manager.cache_if_reused(optimized_df, "main_dataset")
    
    # 4. Efficient aggregation
    agg_result = efficient_multiple_aggregations(
        cached_df,
        group_cols=["category"],
        agg_specs={
            "value": ["sum", "avg", "max", "min"],
            "id": ["count"]
        }
    )
    
    # Show results
    agg_result.show()
    
    # Cleanup
    cache_manager.unpersist_all()
    spark.stop()

if __name__ == "__main__":
    main()