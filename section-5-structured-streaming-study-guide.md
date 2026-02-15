# Section 5: Structured Streaming - 10%

## Overview
Structured Streaming is a scalable and fault-tolerant stream processing engine built on the Spark SQL engine. It treats streaming data as a table being continuously appended.

---

## 1. Structured Streaming Engine Fundamentals

### What is Structured Streaming?

**Key Concepts:**
- Stream processing built on Spark SQL engine
- Treats streaming data as unbounded table
- Same DataFrame/Dataset API as batch processing
- Incremental query execution
- End-to-end exactly-once semantics
- Built-in fault tolerance

**Core Philosophy:**
```
Input Stream → Unbounded Table → Query → Result Table → Output Sink
```

### Programming Model

**Conceptual Model:**
```python
# Streaming is viewed as continuous append to table
# Query runs incrementally on new data
# Results updated to output sink

# Input Table (unbounded)
# ┌──────────┬─────────┬────────┐
# │ timestamp│ user_id │ action │
# ├──────────┼─────────┼────────┤
# │ 10:00:00 │ user1   │ click  │
# │ 10:00:01 │ user2   │ view   │
# │ 10:00:02 │ user1   │ buy    │  ← New rows continuously added
# │ ...      │ ...     │ ...    │
# └──────────┴─────────┴────────┘
```

**Programming Example:**
```python
# Read streaming data (looks like batch DataFrame)
streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "events") \
    .load()

# Apply transformations (same as batch)
processed_df = streaming_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Write streaming output
query = processed_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/output") \
    .option("checkpointLocation", "/checkpoint") \
    .start()

query.awaitTermination()
```

### Micro-Batch Processing

**How it Works:**
1. Data arrives continuously from source
2. Spark processes data in small batches (micro-batches)
3. Default trigger interval: as fast as possible
4. Each micro-batch is a small DataFrame
5. Results written to sink after each batch

**Trigger Types:**

```python
from pyspark.sql.streaming import Trigger

# 1. Default: Process as fast as possible
query = df.writeStream \
    .trigger(availableNow=True) \
    .start()

# 2. Fixed interval (micro-batch every N seconds)
query = df.writeStream \
    .trigger(processingTime="10 seconds") \
    .start()

# 3. One-time trigger (process all available data once)
query = df.writeStream \
    .trigger(once=True) \
    .start()

# 4. Continuous processing (experimental, low latency ~1ms)
query = df.writeStream \
    .trigger(continuous="1 second") \
    .start()
```

**Micro-Batch Advantages:**
- Balance between latency and throughput
- Efficient resource utilization
- Easier fault recovery
- Consistent with batch processing model

### Exactly-Once Semantics

**Guarantees:**
- Each input record processed exactly once
- No duplicates in output (with idempotent sinks)
- No data loss
- Consistent results even with failures

**How it's Achieved:**

**1. Replayable Sources:**
```python
# Sources must support offset tracking
# Examples: Kafka, File sources, Kinesis

# Kafka with offset management
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic") \
    .option("startingOffsets", "earliest")  # or "latest" or specific offsets
    .load()
```

**2. Idempotent Sinks:**
```python
# Sink operations can be replayed without duplicates
# Achieved through:
# - Batch ID tracking
# - Transactional writes
# - Checkpoint mechanism

query = df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "/checkpoint")  # Critical for exactly-once
    .start()
```

**3. Checkpointing:**
```python
# Checkpoint stores:
# - Offset ranges processed
# - State information
# - Metadata about stream

# Always specify checkpoint location
query = df.writeStream \
    .option("checkpointLocation", "/checkpoint/my_stream") \
    .start()

# Checkpoint structure:
# /checkpoint/my_stream/
#   - commits/      (batch metadata)
#   - offsets/      (source offsets)
#   - sources/      (source info)
#   - state/        (stateful operation state)
```

### Fault Tolerance Mechanisms

**1. Checkpoint-Based Recovery:**
```python
# On failure, restart from checkpoint
# Same checkpoint location = resume from where it stopped

# First run
query = df.writeStream \
    .option("checkpointLocation", "/checkpoint/app1") \
    .start()

# After failure and restart (uses same checkpoint)
query = df.readStream \
    .format("kafka") \
    .load() \
    .writeStream \
    .option("checkpointLocation", "/checkpoint/app1")  # Same location
    .start()
# Automatically resumes from last committed batch
```

**2. Write-Ahead Logs (WAL):**
- Offsets written before processing
- State changes logged before committing
- Enables recovery to last consistent state

**3. State Store:**
```python
# For stateful operations (aggregations, joins)
# State automatically checkpointed and recovered

df.groupBy("key").count()  # State stored in checkpoint
```

**4. Failure Scenarios Handled:**

| Failure Type | Recovery Mechanism |
|--------------|-------------------|
| Executor failure | Task re-executed on different executor |
| Driver failure | Restart with checkpoint, resume from last batch |
| Source unavailable | Retry with exponential backoff |
| Sink unavailable | Buffer and retry writes |

**Recovery Example:**
```python
import time

# Resilient streaming query
def create_streaming_query():
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "events") \
        .option("failOnDataLoss", "false")  # Handle data loss gracefully
        .load()
    
    query = df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "/output") \
        .option("checkpointLocation", "/checkpoint") \
        .start()
    
    return query

# Auto-restart on failure
while True:
    try:
        query = create_streaming_query()
        query.awaitTermination()
    except Exception as e:
        print(f"Query failed: {e}")
        print("Restarting in 10 seconds...")
        time.sleep(10)
```

---

## 2. Creating and Writing Streaming DataFrames

### Reading from Streaming Sources

**File Source:**
```python
# Read from directory (monitors for new files)
streaming_df = spark.readStream \
    .format("csv") \
    .schema(schema) \
    .option("header", "true") \
    .option("maxFilesPerTrigger", 1) \
    .load("/input/directory")

# JSON files
streaming_df = spark.readStream \
    .format("json") \
    .schema(schema) \
    .load("/input/directory")

# Parquet files
streaming_df = spark.readStream \
    .format("parquet") \
    .schema(schema) \
    .load("/input/directory")
```

**Kafka Source:**
```python
# Subscribe to single topic
streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic_name") \
    .load()

# Subscribe to multiple topics
streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic1,topic2,topic3") \
    .load()

# Subscribe to pattern
streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribePattern", "topic_.*") \
    .load()

# Parse Kafka messages
from pyspark.sql.functions import from_json, col

kafka_df = spark.readStream.format("kafka").load()

parsed_df = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")
```

**Socket Source (for testing):**
```python
streaming_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()
```

**Rate Source (for testing):**
```python
# Generate test data at specified rate
streaming_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 100) \
    .option("numPartitions", 10) \
    .load()
```

### Output Modes

**1. Append Mode (default):**
```python
# Only new rows added to result table are written
# Works with: queries without aggregation, or aggregations with watermark

query = df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .start()

# Use case: Raw event logging, filtering, transformations without state
```

**2. Complete Mode:**
```python
# Entire result table written to sink after each trigger
# Only for aggregation queries
# Memory intensive (stores entire result)

query = df.groupBy("category").count() \
    .writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("category_counts") \
    .start()

# Use case: Dashboards, real-time metrics, small result tables
```

**3. Update Mode:**
```python
# Only rows updated since last trigger are written
# Works with: aggregation queries

query = df.groupBy("user_id").count() \
    .writeStream \
    .outputMode("update") \
    .format("parquet") \
    .start()

# Use case: Incremental aggregations, large result tables
```

**Mode Comparison:**

| Output Mode | Writes | Use Case | State Size |
|-------------|--------|----------|------------|
| Append | New rows only | Event logs, ETL | Small (watermark-bounded) |
| Complete | All rows | Dashboards, metrics | Large (entire result) |
| Update | Changed rows only | Incremental aggregations | Medium (partial result) |

### Output Sinks

**1. File Sink:**
```python
# Parquet (most common)
query = df.writeStream \
    .format("parquet") \
    .option("path", "/output/path") \
    .option("checkpointLocation", "/checkpoint/path") \
    .partitionBy("date", "hour") \
    .start()

# CSV
query = df.writeStream \
    .format("csv") \
    .option("path", "/output/csv") \
    .option("checkpointLocation", "/checkpoint/csv") \
    .option("header", "true") \
    .start()

# JSON
query = df.writeStream \
    .format("json") \
    .option("path", "/output/json") \
    .option("checkpointLocation", "/checkpoint/json") \
    .start()
```

**2. Kafka Sink:**
```python
# Write to Kafka topic
query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "output_topic") \
    .option("checkpointLocation", "/checkpoint/kafka") \
    .start()

# Dynamic topic selection
query = df.selectExpr(
    "CAST(user_id AS STRING) as key",
    "CAST(event AS STRING) as value",
    "topic_name as topic"  # Dynamic topic
) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("checkpointLocation", "/checkpoint/kafka") \
    .start()
```

**3. Console Sink (for debugging):**
```python
# Print to console
query = df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("numRows", 20) \
    .start()
```

**4. Memory Sink (for testing):**
```python
# Store in memory as table
query = df.writeStream \
    .format("memory") \
    .queryName("streaming_table") \
    .outputMode("complete") \
    .start()

# Query the table
spark.sql("SELECT * FROM streaming_table").show()
```

**5. ForeachBatch Sink (custom logic):**
```python
def write_batch(batch_df, batch_id):
    # Custom processing for each micro-batch
    print(f"Processing batch: {batch_id}")
    
    # Write to multiple sinks
    batch_df.write.format("parquet").mode("append").save(f"/output/batch_{batch_id}")
    batch_df.write.format("jdbc").mode("append").save()
    
    # Custom logic
    if batch_df.count() > 1000:
        batch_df.write.mode("append").saveAsTable("large_batches")

query = df.writeStream \
    .foreachBatch(write_batch) \
    .start()
```

**6. Foreach Sink (row-by-row processing):**
```python
class CustomWriter:
    def open(self, partition_id, epoch_id):
        # Initialize connection
        self.connection = create_connection()
        return True
    
    def process(self, row):
        # Process each row
        self.connection.write(row)
    
    def close(self, error):
        # Cleanup
        if self.connection:
            self.connection.close()

query = df.writeStream \
    .foreach(CustomWriter()) \
    .start()
```

**7. Delta Lake Sink:**
```python
# Write to Delta table (ACID transactions)
query = df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoint/delta") \
    .outputMode("append") \
    .table("delta_table_name")

# Or with path
query = df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoint/delta") \
    .option("path", "/delta/path") \
    .start()
```

### Managing Streaming Queries

```python
# Start query
query = df.writeStream.format("console").start()

# Query properties
query.id           # Unique query ID
query.name         # Query name
query.status       # Current status
query.recentProgress  # Recent progress reports

# Wait for termination
query.awaitTermination()  # Block forever
query.awaitTermination(timeout=30)  # Block for 30 seconds

# Stop query
query.stop()

# Check if active
if query.isActive:
    print("Query is running")

# Get exception if failed
if query.exception():
    print(f"Query failed: {query.exception()}")

# Manage multiple queries
active_queries = spark.streams.active
for q in active_queries:
    print(f"Query: {q.name}, Status: {q.status}")

# Stop all queries
for q in spark.streams.active:
    q.stop()
```

---

## 3. Operations on Streaming DataFrames

### Selection and Projection

```python
from pyspark.sql.functions import col, lit, concat, upper

streaming_df = spark.readStream.format("kafka").load()

# Select columns
selected_df = streaming_df.select("timestamp", "value")

# Select with expressions
projected_df = streaming_df.select(
    col("timestamp"),
    col("value").cast("string").alias("event_data"),
    lit("processed").alias("status")
)

# Add computed columns
enriched_df = streaming_df.withColumn("processed_time", current_timestamp()) \
    .withColumn("upper_value", upper(col("value")))

# Filter rows
filtered_df = streaming_df.filter(col("value") > 100)
filtered_df = streaming_df.where("category = 'important'")
```

### Window Operations

**Tumbling Windows (non-overlapping):**
```python
from pyspark.sql.functions import window, col

# 10-minute tumbling windows
windowed_df = streaming_df \
    .groupBy(window(col("timestamp"), "10 minutes")) \
    .count()

# With additional grouping key
windowed_df = streaming_df \
    .groupBy(
        window(col("timestamp"), "10 minutes"),
        col("user_id")
    ) \
    .sum("amount")
```

**Sliding Windows (overlapping):**
```python
# 10-minute window, sliding every 5 minutes
sliding_df = streaming_df \
    .groupBy(
        window(col("timestamp"), "10 minutes", "5 minutes")
    ) \
    .count()

# Example timeline:
# Window 1: 10:00 - 10:10
# Window 2: 10:05 - 10:15 (overlaps with Window 1)
# Window 3: 10:10 - 10:20
```

**Session Windows:**
```python
from pyspark.sql.functions import session_window

# Group events by session (max 30-minute gap)
session_df = streaming_df \
    .groupBy(
        session_window(col("timestamp"), "30 minutes"),
        col("user_id")
    ) \
    .count()
```

**Window with Watermark:**
```python
# Define watermark (handle late data)
windowed_df = streaming_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("category")
    ) \
    .count()

# Watermark explanation:
# - Events up to 10 minutes late are processed
# - Events older than watermark are dropped
# - Allows state cleanup for old windows
```

### Aggregations

**Basic Aggregations:**
```python
from pyspark.sql.functions import sum, avg, min, max, count

# Simple aggregation
agg_df = streaming_df.groupBy("category").count()

# Multiple aggregations
agg_df = streaming_df.groupBy("user_id").agg(
    count("*").alias("event_count"),
    sum("amount").alias("total_amount"),
    avg("amount").alias("avg_amount"),
    min("timestamp").alias("first_event"),
    max("timestamp").alias("last_event")
)
```

**Time-Based Aggregations:**
```python
# Count events per 5-minute window
time_agg = streaming_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(window(col("timestamp"), "5 minutes")) \
    .agg(
        count("*").alias("event_count"),
        sum("value").alias("total_value")
    )
```

**Multiple Group Keys:**
```python
# Group by multiple dimensions
multi_group = streaming_df.groupBy(
    "category",
    "region",
    window(col("timestamp"), "1 hour")
).agg(
    sum("sales").alias("total_sales"),
    count("*").alias("num_transactions")
)
```

**Aggregations with Output Modes:**
```python
# Complete mode: Full result table
query = streaming_df.groupBy("category").count() \
    .writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("counts") \
    .start()

# Update mode: Only updated rows
query = streaming_df.groupBy("category").count() \
    .writeStream \
    .outputMode("update") \
    .format("parquet") \
    .start()

# Append mode: Requires watermark for aggregations
query = streaming_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(window(col("timestamp"), "5 minutes")) \
    .count() \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .start()
```

### Joins

**Stream-Static Join:**
```python
# Streaming data
streaming_orders = spark.readStream.format("kafka").load()

# Static data (dimension table)
static_products = spark.read.parquet("/data/products")

# Join streaming with static
enriched_orders = streaming_orders.join(
    static_products,
    streaming_orders.product_id == static_products.id
)
```

**Stream-Stream Join:**
```python
# Two streaming sources
stream1 = spark.readStream.format("kafka").option("subscribe", "topic1").load()
stream2 = spark.readStream.format("kafka").option("subscribe", "topic2").load()

# Inner join with watermark (required)
joined = stream1 \
    .withWatermark("timestamp", "10 minutes") \
    .join(
        stream2.withWatermark("timestamp", "20 minutes"),
        expr("""
            stream1.id = stream2.id AND
            stream1.timestamp >= stream2.timestamp AND
            stream1.timestamp <= stream2.timestamp + interval 1 hour
        """)
    )
```

---

## 4. Streaming Deduplication

### Deduplication Without Watermark

**Simple Deduplication:**
```python
# Deduplicate on key columns (maintains state indefinitely)
deduplicated_df = streaming_df.dropDuplicates(["user_id", "event_id"])

# Deduplicate on all columns
deduplicated_df = streaming_df.dropDuplicates()

# Warning: State grows unbounded without watermark!
```

**Use Case:**
```python
# Remove duplicate events
events_df = spark.readStream \
    .format("kafka") \
    .option("subscribe", "events") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Deduplicate by event_id
unique_events = events_df.dropDuplicates(["event_id"])

query = unique_events.writeStream \
    .format("parquet") \
    .option("path", "/output/unique_events") \
    .option("checkpointLocation", "/checkpoint/unique") \
    .start()
```

**Limitations Without Watermark:**
- State size grows indefinitely
- Memory issues with long-running streams
- No cleanup of old state
- Not suitable for production with large datasets

### Deduplication With Watermark

**Watermarked Deduplication:**
```python
# Deduplicate with watermark (state cleanup after threshold)
deduplicated_df = streaming_df \
    .withWatermark("timestamp", "24 hours") \
    .dropDuplicates(["user_id", "event_id", "timestamp"])

# State for records older than watermark is automatically removed
```

**How Watermark Works:**
```
Current event time: 12:00:00
Watermark: 10 minutes
Watermark threshold: 11:50:00

Events with timestamp >= 11:50:00: Processed and deduplicated
Events with timestamp < 11:50:00: Dropped (considered too late)
State for windows < 11:50:00: Removed
```

**Complete Example:**
```python
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

# Define schema
schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("action", StringType(), True),
    StructField("amount", DoubleType(), True)
])

# Read streaming data
events_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_events") \
    .load()

# Parse JSON and extract fields
parsed_df = events_df \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Deduplicate with watermark
deduplicated_df = parsed_df \
    .withWatermark("timestamp", "1 hour") \
    .dropDuplicates(["event_id", "user_id", "timestamp"])

# Write to sink
query = deduplicated_df.writeStream \
    .format("parquet") \
    .option("path", "/output/deduplicated") \
    .option("checkpointLocation", "/checkpoint/deduplicated") \
    .outputMode("append") \
    .start()

query.awaitTermination()
```

### Deduplication with Aggregation

```python
# Deduplicate before aggregation
result = streaming_df \
    .withWatermark("timestamp", "10 minutes") \
    .dropDuplicates(["event_id", "timestamp"]) \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("category")
    ) \
    .agg(
        count("*").alias("unique_event_count"),
        sum("amount").alias("total_amount")
    )
```

### Comparing Deduplication Approaches

| Approach | State Growth | Use Case | Performance |
|----------|--------------|----------|-------------|
| Without Watermark | Unbounded | Short-lived streams, small datasets | Good initially, degrades over time |
| With Watermark | Bounded | Production, long-running streams | Consistent, good for large datasets |

**Best Practices:**
```python
# ✅ Good: Watermark for production
deduplicated = df \
    .withWatermark("timestamp", "2 hours") \
    .dropDuplicates(["id", "timestamp"])

# ❌ Bad: No watermark in production
deduplicated = df.dropDuplicates(["id"])  # State grows forever!

# ✅ Good: Include timestamp in deduplication
deduplicated = df \
    .withWatermark("timestamp", "1 day") \
    .dropDuplicates(["id", "timestamp"])

# ❌ Bad: Missing timestamp
deduplicated = df \
    .withWatermark("timestamp", "1 day") \
    .dropDuplicates(["id"])  # Watermark not applied!
```

### Handling Late Data

```python
# Configure late data handling
deduplicated = streaming_df \
    .withWatermark("event_time", "15 minutes") \
    .dropDuplicates(["event_id", "event_time"])

# Late data scenarios:
# 1. Event arrives within watermark (15 min): Processed
# 2. Event arrives after watermark: Dropped
# 3. Duplicate within watermark: Removed
# 4. Duplicate after watermark: May be processed (state removed)
```

### Monitoring Deduplication

```python
# Monitor deduplication effectiveness
def monitor_deduplication(batch_df, batch_id):
    input_count = batch_df.count()
    
    # After deduplication
    unique_count = batch_df.dropDuplicates(["event_id"]).count()
    duplicate_count = input_count - unique_count
    
    print(f"Batch {batch_id}: Input={input_count}, Unique={unique_count}, Duplicates={duplicate_count}")
    
    # Write deduplicated data
    batch_df.write.format("parquet").mode("append").save("/output")

query = streaming_df \
    .withWatermark("timestamp", "10 minutes") \
    .dropDuplicates(["event_id", "timestamp"]) \
    .writeStream \
    .foreachBatch(monitor_deduplication) \
    .start()
```

---

## Key Concepts Summary

### Structured Streaming Engine
- **Programming Model**: Unbounded table abstraction
- **Execution**: Micro-batch processing
- **Semantics**: Exactly-once guarantee
- **Fault Tolerance**: Checkpoint-based recovery

### Output Modes
- **Append**: New rows only (default)
- **Complete**: Entire result table
- **Update**: Changed rows only

### Watermark
- **Purpose**: Handle late data and enable state cleanup
- **Configuration**: `withWatermark(column, threshold)`
- **Required For**: Append mode with aggregations, stateful operations

### Deduplication
- **Without Watermark**: Unbounded state growth
- **With Watermark**: Bounded state, production-ready
- **Key Requirement**: Include timestamp in deduplication columns

---

## Best Practices

### General
1. Always specify checkpoint location
2. Use watermarks for stateful operations
3. Choose appropriate output mode
4. Monitor query progress and status
5. Handle failures with retry logic

### Performance
1. Set appropriate trigger intervals
2. Partition output data
3. Use compact file formats (Parquet, Delta)
4. Tune shuffle partitions
5. Enable AQE for optimization

### Deduplication
1. Always use watermark in production
2. Include timestamp in deduplication keys
3. Set reasonable watermark threshold
4. Monitor state size
5. Test late data handling

### Fault Tolerance
1. Use unique checkpoint locations per query
2. Test failure recovery
3. Implement monitoring and alerting
4. Use idempotent sinks when possible
5. Version checkpoint directories

---

## Common Exam Topics

- Structured Streaming programming model
- Micro-batch vs continuous processing
- Exactly-once semantics implementation
- Output modes (append, complete, update)
- Trigger types and configurations
- Checkpoint mechanism
- Watermark concept and usage
- Window operations (tumbling, sliding, session)
- Stream deduplication with and without watermark
- Fault tolerance mechanisms
- Supported sources and sinks

---

## Practice Questions

1. **What are the three output modes in Structured Streaming?**
   - Append (new rows), Complete (all rows), Update (changed rows)

2. **Why is checkpoint location important?**
   - Enables exactly-once semantics, fault recovery, and offset tracking

3. **What's the difference between deduplication with and without watermark?**
   - Without watermark: unbounded state growth. With watermark: bounded state with automatic cleanup.

4. **When should you use Complete output mode?**
   - For aggregation queries with small result tables, dashboards, real-time metrics

5. **What is a watermark?**
   - Threshold for handling late data and enabling state cleanup based on event time

6. **Which output mode requires watermark for aggregations?**
   - Append mode requires watermark for aggregation queries

7. **What happens to late data beyond the watermark?**
   - It's dropped and not processed

8. **What are the main benefits of micro-batch processing?**
   - Balance latency/throughput, efficient resources, easier fault recovery, consistency

---

## Complete Example: End-to-End Streaming Application

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create Spark session
spark = SparkSession.builder \
    .appName("E2E Streaming App") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Define schema
schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("category", StringType(), True),
    StructField("amount", DoubleType(), True)
])

# Read from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

# Parse and transform
transactions = raw_stream \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*") \
    .withWatermark("timestamp", "10 minutes") \
    .dropDuplicates(["transaction_id", "timestamp"])

# Aggregate by window and category
aggregated = transactions \
    .groupBy(
        window(col("timestamp"), "5 minutes", "1 minute"),
        col("category")
    ) \
    .agg(
        count("*").alias("transaction_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount")
    )

# Write to multiple sinks
def write_to_sinks(batch_df, batch_id):
    # Parquet for analytics
    batch_df.write \
        .format("parquet") \
        .mode("append") \
        .partitionBy("category") \
        .save("/output/analytics")
    
    # Delta for ACID
    batch_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("transaction_metrics")

# Start query
query = aggregated.writeStream \
    .foreachBatch(write_to_sinks) \
    .option("checkpointLocation", "/checkpoint/e2e") \
    .trigger(processingTime="30 seconds") \
    .start()

# Monitor
while query.isActive:
    progress = query.lastProgress
    if progress:
        print(f"Batch: {progress['batchId']}, Rows: {progress['numInputRows']}")
    query.awaitTermination(timeout=10)
```

---

## Additional Resources

- [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Structured Streaming + Kafka Integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- [Watermarking Documentation](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking)
