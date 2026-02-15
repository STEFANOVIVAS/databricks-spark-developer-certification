# Section 4: Troubleshooting and Tuning Apache Spark DataFrame API Applications - 10%

## Overview
This section covers performance optimization, troubleshooting techniques, and monitoring strategies for Apache Spark applications to achieve optimal cluster utilization and resolve common issues.

---

## 1. Performance Tuning Strategies and Cluster Optimization

### Partitioning Fundamentals

**Understanding Partitions:**
- Partitions are basic units of parallelism in Spark
- Each partition is processed by a single task on one executor core
- Optimal partition size: 100-200MB per partition
- Default partitions: `spark.default.parallelism` or `spark.sql.shuffle.partitions`

**Check Current Partitions:**
```python
# Get number of partitions
num_partitions = df.rdd.getNumPartitions()
print(f"Number of partitions: {num_partitions}")

# View partition distribution
df.rdd.glom().map(len).collect()
```

### Repartitioning

**When to Repartition:**
- Increase parallelism for wide transformations
- Balance data across executors
- After filtering that reduces data size significantly
- Before expensive operations (joins, aggregations)

**Repartition Operations:**
```python
# Increase partitions (causes full shuffle)
df_repartitioned = df.repartition(200)

# Repartition by column (hash partitioning)
df_repartitioned = df.repartition(100, "customer_id")

# Repartition by multiple columns
df_repartitioned = df.repartition(100, "country", "region")

# Repartition with range partitioning
from pyspark.sql.functions import col
df_repartitioned = df.repartitionByRange(100, col("date"))
```

**Repartition Best Practices:**
```python
# Before: Too few partitions (poor parallelism)
df = spark.read.parquet("/large/dataset")  # 10 partitions
df.rdd.getNumPartitions()  # 10

# After: Optimal parallelism
num_cores = spark.sparkContext.defaultParallelism
df_optimized = df.repartition(num_cores * 3)  # 3-4x cores is good rule

# Repartition before expensive operations
df_filtered = df.filter(col("status") == "active")
df_filtered = df_filtered.repartition(50)  # Reduce from 200 to 50 after filter
result = df_filtered.groupBy("category").agg(sum("amount"))
```

### Coalescing

**When to Coalesce:**
- Reduce partitions after filtering
- Before writing small datasets
- Optimize small file problem
- Never increases parallelism (only decreases)

**Coalesce Operations:**
```python
# Reduce partitions without full shuffle (faster than repartition)
df_coalesced = df.coalesce(10)

# Compare: repartition vs coalesce
df.repartition(10)  # Full shuffle, evenly distributes
df.coalesce(10)     # No shuffle (or minimal), less balanced

# Use case: After filtering
large_df = spark.read.parquet("/path/to/data")  # 200 partitions
filtered_df = large_df.filter(col("year") == 2024)  # Now only 5% of data
filtered_df = filtered_df.coalesce(20)  # Reduce to 20 partitions
filtered_df.write.parquet("/output")  # Writes 20 files instead of 200
```

**Coalesce vs Repartition:**
| Operation | Shuffle | Use Case | Performance |
|-----------|---------|----------|-------------|
| `coalesce(n)` | Minimal/None | Reduce partitions | Faster |
| `repartition(n)` | Full shuffle | Increase or balance partitions | Slower but balanced |

### Identifying Data Skew

**What is Data Skew?**
- Uneven distribution of data across partitions
- Some tasks take much longer than others
- Causes stragglers and poor cluster utilization
- Common in groupBy, join operations

**Detecting Data Skew:**
```python
# Method 1: Check partition sizes
partition_sizes = df.rdd.glom().map(len).collect()
print(f"Min: {min(partition_sizes)}, Max: {max(partition_sizes)}, Avg: {sum(partition_sizes)/len(partition_sizes)}")

# Method 2: Examine data distribution
df.groupBy("partition_key").count().orderBy("count", ascending=False).show()

# Method 3: Check for null or empty keys
df.filter(col("key").isNull()).count()

# Method 4: Analyze value cardinality
from pyspark.sql.functions import countDistinct
df.select(countDistinct("key")).show()

# Visual inspection
df.groupBy("key").count().describe().show()
```

**Handling Data Skew:**

**Technique 1: Salting (Add random prefix)**
```python
from pyspark.sql.functions import rand, concat, lit, floor

# Add salt to skewed key
df_salted = df.withColumn("salt", floor(rand() * 10))
df_salted = df_salted.withColumn("salted_key", concat(col("key"), lit("_"), col("salt")))

# Perform operation on salted key
result = df_salted.groupBy("salted_key").agg(sum("amount"))

# Remove salt from result
result = result.withColumn("key", split(col("salted_key"), "_")[0])
result = result.groupBy("key").agg(sum("sum(amount)"))
```

**Technique 2: Broadcast Join for Skewed Data**
```python
from pyspark.sql.functions import broadcast

# If one side is small, broadcast it
skewed_large_df = spark.read.parquet("/skewed/data")
small_df = spark.read.parquet("/small/data")

result = skewed_large_df.join(broadcast(small_df), "key")
```

**Technique 3: Split Skewed Keys**
```python
# Identify skewed keys
skewed_keys = df.groupBy("key").count().filter(col("count") > 100000).select("key")

# Process skewed and non-skewed separately
df_skewed = df.join(skewed_keys, "key", "inner")
df_normal = df.join(skewed_keys, "key", "left_anti")

# Process with different strategies
result_normal = df_normal.groupBy("key").agg(sum("amount"))
result_skewed = df_skewed.repartition(100, "key").groupBy("key").agg(sum("amount"))

# Combine results
result = result_normal.union(result_skewed)
```

**Technique 4: Adaptive Query Execution (AQE)**
```python
# Enable AQE to handle skew automatically
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
```

### Understanding Shuffle in Spark

**Shuffle** is the process of redistributing data across partitions through the cluster nodes. It is one of the **most expensive** operations in Spark because it involves:

1. **Disk I/O** - data is serialized and written
2. **Network Transfer** - data moves between executors
3. **Reading and Deserialization** - data is reconstructed at the destination

```
Before Shuffle:                      After Shuffle:
┌─────────────────┐                  ┌─────────────────┐
│ Executor 1      │                  │ Executor 1      │
│ [A, B, A, C]    │                  │ [A, A, A, A]    │  ← all A's
└─────────────────┘                  └─────────────────┘
┌─────────────────┐      ───►        ┌─────────────────┐
│ Executor 2      │                  │ Executor 2      │
│ [B, A, C, B]    │                  │ [B, B, B]       │  ← all B's
└─────────────────┘                  └─────────────────┘
┌─────────────────┐                  ┌─────────────────┐
│ Executor 3      │                  │ Executor 3      │
│ [C, C, A, B]    │                  │ [C, C, C]       │  ← all C's
└─────────────────┘                  └─────────────────┘
```

#### Why Shuffle is Expensive

| Cost | Description |
|---|---|
| **Disk I/O** | Data is written to shuffle files |
| **Network** | Transfer between executors |
| **Serialization** | Object to bytes conversion |
| **Memory** | Buffering during transfer |
| **GC Pressure** | Object allocation/deallocation |

#### Shuffle Phases

```
┌─────────────────────────────────────────────────────────────┐
│                        SHUFFLE                               │
├────────────────────────┬────────────────────────────────────┤
│      MAP SIDE          │           REDUCE SIDE              │
│  (Shuffle Write)       │         (Shuffle Read)             │
├────────────────────────┼────────────────────────────────────┤
│ 1. Serialize data      │ 1. Fetch data from mappers         │
│ 2. Partition by key    │ 2. Deserialize                     │
│ 3. Write to disk       │ 3. Combine partitions              │
│ 4. Notify driver       │ 4. Process data                    │
└────────────────────────┴────────────────────────────────────┘
```

#### Operations that Cause Shuffle

| Operation | Reason |
|---|---|
| `groupBy()` / `groupByKey()` | Groups data by key |
| `reduceByKey()` | Reduces values by key |
| `join()` | Combines DataFrames by key |
| `distinct()` | Removes duplicates (needs to compare all) |
| `repartition()` | Redistributes data to new partitions |
| `orderBy()` / `sort()` | Sorts data globally |
| `coalesce()`  | Reduces partition count |

#### Important Shuffle Configurations

```python
# Number of partitions after shuffle (default: 200)
spark.conf.set("spark.sql.shuffle.partitions", 100)

# Shuffle buffer size
spark.conf.set("spark.shuffle.file.buffer", "64k")

# Shuffle compression (recommended)
spark.conf.set("spark.shuffle.compress", "true")
```

#### How to Minimize Shuffle

| Strategy | Description |
|---|---|
| **Broadcast Join** | Avoids shuffle by sending small DF to all nodes |
| **Data Colocation** | Use same partitioner in DataFrames to be joined |
| **Bucket Tables** | Pre-partition data at write time with fixed number of partitions|
| **Filter Early** | Reduce data before operations that cause shuffle |
| **reduceByKey vs groupByKey** | `reduceByKey` does local aggregation before shuffle |
| **Strategic Caching** | Persist data after shuffle for reuse |


**Shuffle Metrics:**
```python
# Monitor shuffle in Spark UI:
# - Stages tab → Shuffle Read/Write
# - Look for large shuffle read/write sizes
# - Check shuffle spill to disk
```

**Strategies to Reduce Shuffle:**

**Strategy 1: Broadcast Joins**
```python
from pyspark.sql.functions import broadcast

# Before: Regular join (causes shuffle)
result = large_df.join(small_df, "key")

# After: Broadcast join (no shuffle for large_df)
result = large_df.join(broadcast(small_df), "key")

# Configure broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "104857600")  # 100MB
```

**Strategy 2: Data Colocation**
```python
# Write data partitioned by join key
df.write.partitionBy("join_key").parquet("/partitioned/data")

# Read and join (Spark may optimize shuffle)
df1 = spark.read.parquet("/partitioned/data")
df2 = spark.read.parquet("/other/data")
result = df1.join(df2, "join_key")
```

**Strategy 3: Bucketing**
```python
# Write with bucketing
df.write \
    .bucketBy(100, "join_key") \
    .sortBy("join_key") \
    .saveAsTable("bucketed_table")

# Join bucketed tables (reduced shuffle)
df1 = spark.table("bucketed_table1")
df2 = spark.table("bucketed_table2")
result = df1.join(df2, "join_key")  # Optimized join
```

**Strategy 4: Filter Early**
```python
# Bad: Filter after join (unnecessary shuffle)
result = df1.join(df2, "key").filter(col("date") >= "2024-01-01")

# Good: Filter before join (less data to shuffle)
df1_filtered = df1.filter(col("date") >= "2024-01-01")
result = df1_filtered.join(df2, "key")
```

**Strategy 5: Use reduceByKey Instead of groupByKey**
```python
# Bad: groupByKey (shuffles all data)
rdd.groupByKey().mapValues(sum)

# Good: reduceByKey (combines locally first)
rdd.reduceByKey(lambda a, b: a + b)
```

**Strategy 6: Cache Intermediate Results**
```python
# If DataFrame is reused multiple times
df_filtered = df.filter(col("status") == "active")
df_filtered.cache()

# Multiple operations on cached DataFrame (no re-computation)
result1 = df_filtered.groupBy("category").count()
result2 = df_filtered.groupBy("region").agg(sum("amount"))
```
**Strategy 7: Combine Operations**
```python
# Bad: Multiple shuffles
df.groupBy("key1").agg(sum("amount")) \
  .groupBy("key2").agg(sum("sum(amount)"))  # 2 shuffles

# Better: Single groupBy if possible
df.groupBy("key1", "key2").agg(sum("amount"))  # 1 shuffle
```

### Partition Size Optimization

**Calculate Optimal Partition Count:**
```python
# Rule of thumb: 
# num_partitions = (total_data_size_mb / target_partition_size_mb)
# target_partition_size_mb = 100-200 MB

import os

# Get data size
data_size_bytes = df.rdd.map(lambda row: len(str(row))).sum()
data_size_mb = data_size_bytes / (1024 * 1024)

# Calculate optimal partitions
target_partition_size_mb = 128
optimal_partitions = int(data_size_mb / target_partition_size_mb)

print(f"Data size: {data_size_mb:.2f} MB")
print(f"Optimal partitions: {optimal_partitions}")

# Repartition
df_optimized = df.repartition(optimal_partitions)
```

**Configure Default Shuffle Partitions:**
```python
# Default is 200, adjust based on data size
spark.conf.set("spark.sql.shuffle.partitions", "100")

# For small datasets
spark.conf.set("spark.sql.shuffle.partitions", "10")

# For large datasets
spark.conf.set("spark.sql.shuffle.partitions", "500")
```

---

## 2. Adaptive Query Execution (AQE)

### What is AQE?

Adaptive Query Execution is a query re-optimization that occurs during query execution based on runtime statistics.

**Key Features:**
1. **Dynamically coalesce shuffle partitions**
2. **Dynamically switch join strategies**
3. **Dynamically optimize skew joins**

### Enabling AQE

```python
# Enable AQE (default in Spark 3.2+)
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Check if enabled
spark.conf.get("spark.sql.adaptive.enabled")
```

### AQE Feature 1: Coalescing Shuffle Partitions

**Problem:** Too many small shuffle partitions waste resources

**Solution:** AQE automatically combines small partitions

```python
# Configure coalescing
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "200")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1MB")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")

# Example: Start with 200 partitions, AQE reduces to optimal number
df = spark.read.parquet("/data")
result = df.groupBy("key").count()  # AQE will optimize partition count
```

**Benefits:**
- Reduces overhead of managing many small tasks
- Improves performance for queries with filters
- Automatic optimization without manual tuning

### AQE Feature 2: Dynamic Join Strategy

**Problem:** Query planner may choose suboptimal join strategy before knowing data size

**Solution:** AQE switches to broadcast join if applicable after seeing runtime statistics

```python
# Enable dynamic join strategy
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "10MB")

# Example: Sort-merge join may convert to broadcast join at runtime
df1 = spark.read.parquet("/large/data")
df2 = spark.read.parquet("/unknown/size/data").filter(col("active") == True)

# If df2 after filter is < 10MB, AQE converts to broadcast join
result = df1.join(df2, "key")
```

**Benefits:**
- Optimal join strategy based on actual data size
- Eliminates expensive shuffles when possible
- Better performance for filtered joins

### AQE Feature 3: Skew Join Optimization

**Problem:** Data skew causes some tasks to take much longer

**Solution:** AQE detects and splits skewed partitions into smaller sub-partitions

```python
# Enable skew join handling
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

# Example: Skewed partition automatically handled
skewed_df = spark.read.parquet("/skewed/data")
result = skewed_df.join(other_df, "key")
# AQE detects skew and splits large partitions
```

**Skew Detection Criteria:**
- Partition size > threshold (default 256MB)
- Partition size > median * factor (default 5x)

**Benefits:**
- Automatic skew handling without manual intervention
- Better resource utilization
- Eliminates stragglers

### AQE Configuration Summary

```python
# Complete AQE configuration
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Coalesce partitions
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "200")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1MB")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")

# Dynamic join
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "10485760")  # 10MB

# Skew join
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "268435456")  # 256MB
```

### Monitoring AQE in Spark UI

**Check AQE Optimizations:**
1. Open Spark UI → SQL tab
2. Click on query
3. Look for "AQE" markers in query plan
4. Check "Adaptive" stages
5. Review optimizations applied:
   - Coalesced partitions
   - Converted joins
   - Optimized skews

---

## 3. Logging and Monitoring Spark Applications

### Understanding Spark Logging Architecture

**Components:**
- **Driver Logs**: Application master, DAG scheduler, task scheduling
- **Executor Logs**: Task execution, data processing, errors
- **Log Levels**: ERROR, WARN, INFO, DEBUG, TRACE

### Configuring Log Levels

**Method 1: Using SparkContext**
```python
# Set log level programmatically
spark.sparkContext.setLogLevel("WARN")  # ERROR, WARN, INFO, DEBUG, TRACE

# Common settings:
spark.sparkContext.setLogLevel("ERROR")  # Production (only errors)
spark.sparkContext.setLogLevel("WARN")   # Default (warnings and errors)
spark.sparkContext.setLogLevel("INFO")   # Development (detailed info)
spark.sparkContext.setLogLevel("DEBUG")  # Troubleshooting (very verbose)
```

**Method 2: Using log4j.properties**
```properties
# Create log4j.properties file
log4j.rootCategory=WARN, console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

# Set specific loggers
log4j.logger.org.apache.spark=WARN
log4j.logger.org.apache.spark.sql=INFO
log4j.logger.org.apache.hadoop=WARN

# Custom application logger
log4j.logger.com.myapp=DEBUG
```

**Method 3: Spark Configuration**
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties") \
    .config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties") \
    .getOrCreate()
```

### Accessing Driver Logs

**Local Mode:**
```python
# Logs printed to console/stdout
# Redirect to file:
# spark-submit app.py > driver.log 2>&1
```

**Cluster Mode (YARN):**
```bash
# View live driver logs
yarn logs -applicationId application_1234567890_0001

# View specific container logs
yarn logs -applicationId application_1234567890_0001 -containerId container_1234567890_0001_01_000001

# Download all logs
yarn logs -applicationId application_1234567890_0001 > app_logs.txt
```

**Cluster Mode (Databricks):**
```python
# Access from Databricks UI:
# 1. Clusters → Select cluster
# 2. Event Log → Driver Logs
# 3. Download or view in browser

# Programmatic access
dbutils.fs.ls("dbfs:/cluster-logs/")
```

### Accessing Executor Logs

**View in Spark UI:**
1. Open Spark UI → Executors tab
2. Click on executor ID
3. View stdout and stderr logs

**Download Executor Logs (YARN):**
```bash
# All executor logs
yarn logs -applicationId application_1234567890_0001 | grep "Container:"

# Specific executor
yarn logs -applicationId application_1234567890_0001 -containerId container_e01_1234567890_0001_01_000002
```

### Custom Logging in Application

**Python Logging:**
```python
import logging

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Use in application
def process_data(df):
    logger.info(f"Processing DataFrame with {df.count()} rows")
    
    try:
        result = df.groupBy("key").count()
        logger.info(f"Aggregation completed: {result.count()} groups")
        return result
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}", exc_info=True)
        raise

# Log DataFrame operations
df = spark.read.parquet("/data")
logger.info(f"Loaded DataFrame: {df.count()} rows, {len(df.columns)} columns")
```

**Log4j in Scala/Java:**
```scala
import org.apache.log4j.Logger

object MyApp {
  @transient lazy val log = Logger.getLogger(getClass.getName)
  
  def main(args: Array[String]): Unit = {
    log.info("Application started")
    
    try {
      // Application logic
      log.debug("Processing data")
    } catch {
      case e: Exception =>
        log.error("Error occurred", e)
    }
  }
}
```

### Diagnosing Out-of-Memory Errors

**Types of OOM Errors:**

**1. Driver OOM:**
```
java.lang.OutOfMemoryError: Java heap space (Driver)
```

**Symptoms:**
- Collect operations on large datasets
- Broadcasting large variables
- Accumulating too much data in driver

**Solutions:**
```python
# Increase driver memory
spark = SparkSession.builder \
    .config("spark.driver.memory", "8g") \
    .config("spark.driver.maxResultSize", "4g") \
    .getOrCreate()

# Avoid collecting large datasets
# Bad:
large_list = df.collect()  # OOM!

# Good:
df.write.parquet("/output")  # Write to storage

# Use sampling for analysis
sample_df = df.sample(0.01)
sample_df.show()

# Use take() instead of collect()
first_rows = df.take(100)  # Limits data to driver
```

**2. Executor OOM:**
```
java.lang.OutOfMemoryError: Java heap space (Executor)
```

**Symptoms:**
- Large shuffles
- Skewed data
- Too few partitions
- Memory-intensive operations (joins, aggregations)

**Solutions:**
```python
# Increase executor memory
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.memoryOverhead", "2g")

# Increase partitions to reduce partition size
spark.conf.set("spark.sql.shuffle.partitions", "500")

# Repartition before expensive operations
df = df.repartition(200)

# Handle data skew
df_salted = df.withColumn("salt", floor(rand() * 10))

# Use disk spilling for large shuffles
spark.conf.set("spark.shuffle.spill.compress", "true")
spark.conf.set("spark.shuffle.compress", "true")
```

**3. GC Overhead Limit Exceeded:**
```
java.lang.OutOfMemoryError: GC overhead limit exceeded
```

**Solutions:**
```python
# Tune garbage collection
spark = SparkSession.builder \
    .config("spark.executor.extraJavaOptions", 
            "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35") \
    .getOrCreate()

# Increase memory fraction for execution
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.3")

# Clear cache if not needed
df.unpersist()
```

**4. Container Killed by YARN:**
```
Container killed by YARN for exceeding memory limits
```

**Solutions:**
```python
# Increase memory overhead
spark.conf.set("spark.executor.memoryOverhead", "2048")  # MB
spark.conf.set("spark.driver.memoryOverhead", "1024")

# Monitor memory usage
# Check Spark UI → Executors → Memory metrics
```

### Diagnosing Cluster Underutilization

**Symptoms:**
- Low CPU usage across executors
- Many executors idle
- Long running tasks on few executors
- Low shuffle read/write

**Causes and Solutions:**

**1. Too Few Partitions:**
```python
# Check partitions
num_partitions = df.rdd.getNumPartitions()
num_cores = spark.sparkContext.defaultParallelism

print(f"Partitions: {num_partitions}, Cores: {num_cores}")

# Solution: Increase partitions
if num_partitions < num_cores * 3:
    df = df.repartition(num_cores * 3)
```

**2. Data Skew:**
```python
# Detect skew
partition_distribution = df.rdd.glom().map(len).collect()
print(f"Min: {min(partition_distribution)}, Max: {max(partition_distribution)}")

# Solution: Apply salting or use AQE
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

**3. Small Files Problem:**
```python
# Check number of files
file_count = spark.read.parquet("/data").rdd.getNumPartitions()

# Solution: Coalesce before writing
df.coalesce(50).write.parquet("/output")

# Or repartition for optimal file size
target_file_size_mb = 128
num_files = data_size_mb / target_file_size_mb
df.repartition(int(num_files)).write.parquet("/output")
```

**4. Insufficient Resources:**
```python
# Request more executors
spark = SparkSession.builder \
    .config("spark.executor.instances", "20") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()

# Dynamic allocation
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "5")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "50")
spark.conf.set("spark.dynamicAllocation.initialExecutors", "10")
```

### Monitoring with Spark UI

**Key Metrics to Monitor:**

**1. Jobs Tab:**
- Duration
- Number of stages
- Number of tasks

**2. Stages Tab:**
- Task duration distribution
- Shuffle read/write
- Input/output size
- GC time percentage

**3. Storage Tab:**
- Cached RDDs/DataFrames
- Memory usage
- Disk spill

**4. Executors Tab:**
- Active tasks
- Failed tasks
- Memory usage (storage, execution)
- GC time
- Shuffle read/write

**5. SQL Tab:**
- Query execution plan
- Duration breakdown
- Metrics (rows scanned, shuffle)

### Using Spark History Server

```bash
# Start history server
$SPARK_HOME/sbin/start-history-server.sh

# Configure history logging
spark.eventLog.enabled=true
spark.eventLog.dir=hdfs://namenode/spark-logs
spark.history.fs.logDirectory=hdfs://namenode/spark-logs

# Access UI
# http://localhost:18080
```

### Programmatic Monitoring

**Using StatusTracker:**
```python
from pyspark import StatusTracker

tracker = spark.sparkContext.statusTracker()

# Get active stages
active_stages = tracker.getActiveStageIds()

# Get stage info
for stage_id in active_stages:
    stage_info = tracker.getStageInfo(stage_id)
    print(f"Stage {stage_id}: {stage_info.numTasks} tasks, {stage_info.numActiveTasks} active")

# Monitor job progress
def monitor_job(job_id):
    tracker = spark.sparkContext.statusTracker()
    job_info = tracker.getJobInfo(job_id)
    
    while job_info.status == "RUNNING":
        print(f"Job {job_id} running...")
        time.sleep(5)
        job_info = tracker.getJobInfo(job_id)
    
    print(f"Job {job_id} finished with status: {job_info.status}")
```

**Using Spark Listeners:**
```python
from pyspark import SparkContext
from pyspark.streaming import StreamingListener

class CustomSparkListener(StreamingListener):
    def onStageCompleted(self, stageCompleted):
        stage = stageCompleted.stageInfo()
        print(f"Stage {stage.stageId()} completed in {stage.taskMetrics().executorRunTime()}ms")
    
    def onTaskEnd(self, taskEnd):
        print(f"Task completed: {taskEnd.taskInfo().taskId()}")

# Add listener
sc = spark.sparkContext
sc.addSparkListener(CustomSparkListener())
```

---

## Key Troubleshooting Checklist

### Performance Issues

- [ ] Check partition count (3-4x number of cores)
- [ ] Verify partition size (100-200MB optimal)
- [ ] Look for data skew in Spark UI
- [ ] Check shuffle read/write size
- [ ] Review GC time (< 10% is good)
- [ ] Enable AQE for automatic optimization
- [ ] Use broadcast joins for small tables
- [ ] Cache frequently used DataFrames
- [ ] Avoid unnecessary shuffles

### Memory Issues

- [ ] Check executor memory configuration
- [ ] Review memory overhead settings
- [ ] Look for large collect() operations
- [ ] Identify memory-intensive operations
- [ ] Check for cached data not being used
- [ ] Monitor GC activity
- [ ] Review shuffle spill metrics
- [ ] Verify partition sizes

### Cluster Utilization

- [ ] Check number of active executors
- [ ] Verify core allocation per executor
- [ ] Review task parallelism
- [ ] Look for stragglers in stages
- [ ] Check for small files problem
- [ ] Verify dynamic allocation settings
- [ ] Monitor executor idle time

---

## Best Practices Summary

### Partitioning
1. Use 3-4x partitions per core for optimal parallelism
2. Keep partitions between 100-200MB
3. Use `coalesce()` to reduce, `repartition()` to increase
4. Partition by frequently filtered columns

### Performance
1. Enable AQE for automatic optimizations
2. Use broadcast joins for small tables (< 10MB)
3. Filter data early in pipeline
4. Cache intermediate results when reused
5. Avoid collect() on large datasets

### Monitoring
1. Regularly check Spark UI for bottlenecks
2. Monitor shuffle operations
3. Track GC time percentage
4. Review task duration distribution
5. Use custom logging for debugging

### Memory Management
1. Configure appropriate executor memory
2. Set memory overhead (10-15% of executor memory)
3. Tune memory fractions
4. Monitor and unpersist unused cached data
5. Handle data skew to prevent hotspots

---

## Common Exam Topics

- Difference between repartition and coalesce
- When to use broadcast joins
- Identifying and handling data skew
- AQE features and benefits
- Configuring shuffle partitions
- OOM error diagnosis and solutions
- Executor vs driver memory
- Monitoring Spark applications
- Log levels and configuration
- Cluster utilization optimization

---

## Practice Questions

1. **When should you use coalesce() instead of repartition()?**
   - When reducing partitions and full shuffle is not needed; coalesce is more efficient.

2. **What are the three main features of Adaptive Query Execution?**
   - Coalescing shuffle partitions, dynamic join strategy switching, and skew join optimization.

3. **How do you diagnose data skew?**
   - Check partition sizes, examine data distribution by key, look for stragglers in Spark UI stages.

4. **What's the optimal partition size?**
   - 100-200MB per partition for best performance.

5. **How do you reduce shuffle operations?**
   - Use broadcast joins, partition data appropriately, filter early, use bucketing, combine operations.

6. **What causes driver OOM errors?**
   - collect() on large datasets, broadcasting large variables, accumulating data in driver.

7. **How does AQE help with skewed data?**
   - Automatically detects and splits skewed partitions into smaller sub-partitions.

---

## Additional Resources

- [Spark Performance Tuning Guide](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html)
- [Spark Monitoring](https://spark.apache.org/docs/latest/monitoring.html)
- [Adaptive Query Execution](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)
