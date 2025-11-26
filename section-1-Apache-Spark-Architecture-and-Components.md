# Section 1: Apache Spark Architecture and Components - Study Guide

## Table of Contents
1. [Advantages and Challenges of Spark](#1-advantages-and-challenges-of-spark)
2. [Core Components of Apache Spark Architecture](#2-core-components-of-apache-spark-architecture)
3. [Spark Architecture Deep Dive](#3-spark-architecture-deep-dive)
4. [Execution Hierarchy](#4-execution-hierarchy)
5. [Partitioning and Shuffling](#5-partitioning-and-shuffling)
6. [Execution Patterns](#6-execution-patterns)
7. [Apache Spark Modules](#7-apache-spark-modules)

---

## 1. Advantages and Challenges of Spark

### Advantages ✓

**Speed and Performance**
- **In-Memory Processing**: Spark processes data in-memory (RAM), making it up to 100x faster than MapReduce for certain workloads
- **DAG Execution Engine**: Optimizes the execution plan before running jobs
- **Lazy Evaluation**: Optimizes the entire pipeline before execution

**Ease of Use**
- **Multiple Language Support**: Scala, Python, Java, R, and SQL
- **Rich APIs**: High-level APIs for DataFrames and Datasets
- **Interactive Shell**: REPL for Scala and Python for quick prototyping

**Unified Engine**
- **Multiple Workloads**: Batch processing, interactive queries, streaming, machine learning, and graph processing
- **Single Platform**: No need to maintain multiple systems

**Fault Tolerance**
- **RDD Lineage**: Automatic recovery through DAG lineage
- **Data Replication**: Configurable replication for resilience

**Scalability**
- **Horizontal Scaling**: Add more nodes to the cluster
- **Handle Big Data**: Process petabytes of data

### Challenges ✗

**Memory Management**
- **Memory Intensive**: Requires significant RAM for optimal performance
- **OOM Errors**: Out of Memory errors with large datasets
- **Garbage Collection**: Can cause performance bottlenecks

**Complexity**
- **Learning Curve**: Understanding distributed computing concepts
- **Tuning Requirements**: Multiple configuration parameters to optimize
- **Debugging Difficulty**: Distributed execution makes debugging harder

**Cost**
- **Infrastructure Costs**: Requires substantial compute resources
- **Memory Requirements**: High RAM requirements increase costs

**Small File Problem**
- **Overhead**: Many small files create excessive tasks and metadata
- **Performance Degradation**: Inefficient processing of small files

**Limited Support for Real-Time**
- **Micro-Batch**: Structured Streaming uses micro-batches, not true real-time
- **Latency**: Minimum latency in the order of seconds

---

## 2. Core Components of Apache Spark Architecture

### Cluster Components

```
┌────────────────────────────────────────────────────┐
│                   CLUSTER                          │
│  ┌──────────────┐         ┌────────────────────┐   │
│  │    DRIVER    │         │   WORKER NODE 1    │   │
│  │    NODE      │──────>  │  ┌──────────────┐  │   │
│  │              │         │  │  EXECUTOR 1  │  │   │
│  │SparkContext  │         │  │  - Tasks     │  │   │
│  │DAG Scheduler │         │  │  - Cache     │  │   │
│  │Task Scheduler│         │  │  - CPU Cores │  │   │
│  └──────────────┘         │  └──────────────┘  │   │
│                           └────────────────────┘   │
│                           ┌────────────────────┐   │
│                           │   WORKER NODE 2    │   │
│                           │  ┌──────────────┐  │   │
│                           │  │  EXECUTOR 2  │  │   │
│                           │  │  - Tasks     │  │   │
│                           │  │  - Cache     │  │   │
│                           │  │  - CPU Cores │  │   │
│                           │  └──────────────┘  │   │
│                           └────────────────────┘   │
└────────────────────────────────────────────────────┘
```

### 1. Cluster
- **Definition**: Collection of machines (nodes) working together
- **Purpose**: Distributed computation and storage
- **Types**: Standalone, YARN, Mesos, Kubernetes

### 2. Driver Node
- **Definition**: The process that runs the main() function and creates SparkContext
- **Responsibilities**:
  - Converts user program into tasks
  - Schedules tasks on executors
  - Maintains metadata about RDD/DataFrame partitions
  - Responds to user program
  - Analyzes, distributes, and schedules work across executors

**Key Components in Driver**:
- **SparkContext**: Entry point for Spark functionality
- **DAG Scheduler**: Converts logical execution plan into physical execution plan
- **Task Scheduler**: Assigns tasks to executors
- **SparkSession**: Unified entry point (Spark 2.0+)

### 3. Worker Nodes
- **Definition**: Machines in the cluster that execute tasks
- **Purpose**: Provide resources (CPU, memory, storage) for computation
- **Contains**: One or more executors

### 4. Executors
- **Definition**: JVM processes running on worker nodes
- **Lifespan**: Lives for the entire duration of the Spark application
- **Responsibilities**:
  - Run tasks assigned by driver
  - Store computed data in memory or disk
  - Return results to driver
  - Provide in-memory storage for cached RDDs/DataFrames

**Executor Components**:
- **Task Slots**: Number of concurrent tasks (= number of cores)
- **Cache/Storage**: Memory for caching data
- **Heap Memory**: JVM memory for task execution

### 5. CPU Cores
- **Definition**: Processing units available to executors
- **Purpose**: Determines parallelism level
- **Configuration**: `spark.executor.cores`
- **Impact**: 
  - More cores = more parallel tasks per executor
  - Total tasks = cores × executors

### 6. Memory
- **Types of Memory in Spark**:

**Executor Memory** (`spark.executor.memory`):
- **Execution Memory** (60% by default): For computations (shuffles, joins, sorts, aggregations)
- **Storage Memory** (40% by default): For caching and broadcast variables
- **User Memory** (40% of total): For user data structures
- **Reserved Memory** (300MB): For Spark internal objects

**Driver Memory** (`spark.driver.memory`):
- Stores metadata about partitions
- Collects results from executors
- Runs the main application

---

## 3. Spark Architecture Deep Dive

### DataFrame and Dataset Concepts

#### DataFrames
```python
# DataFrame is a distributed collection of data organized into named columns
# Similar to a table in relational database

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Example").getOrCreate()

# Creating a DataFrame
df = spark.createDataFrame([
    (1, "Alice", 25),
    (2, "Bob", 30),
    (3, "Charlie", 35)
], ["id", "name", "age"])

df.show()
# +---+-------+---+
# | id|   name|age|
# +---+-------+---+
# |  1|  Alice| 25|
# |  2|    Bob| 30|
# |  3|Charlie| 35|
# +---+-------+---+
```

**Key Characteristics**:
- **Schema**: Structured with column names and types
- **Immutable**: Cannot be changed after creation
- **Lazy Evaluation**: Transformations are not executed immediately
- **Optimized**: Catalyst optimizer generates efficient execution plans
- **Language Agnostic**: Same performance in Python, Scala, Java, R

#### Datasets
```scala
// Dataset is a type-safe version of DataFrame (Scala/Java only)
// Provides compile-time type safety

case class Person(id: Int, name: String, age: Int)

val ds = spark.createDataset(Seq(
  Person(1, "Alice", 25),
  Person(2, "Bob", 30),
  Person(3, "Charlie", 35)
))

// Type-safe operations
val adults = ds.filter(_.age >= 18)
```

**Key Characteristics**:
- **Type-Safe**: Compile-time type checking
- **Object-Oriented**: Work with JVM objects
- **Performance**: Combines DataFrame optimization with type safety
- **Encoders**: Serialize objects efficiently

**DataFrame vs Dataset**:
- DataFrame = Dataset[Row] (untyped)
- Dataset provides type safety at compile time
- DataFrame is available in all languages; Dataset only in Scala/Java

### SparkSession Lifecycle

```python
# 1. Creating SparkSession (Entry Point)
spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# 2. Using SparkSession
df = spark.read.csv("data.csv")
df.createOrReplaceTempView("temp_view")
result = spark.sql("SELECT * FROM temp_view")

# 3. Stopping SparkSession
spark.stop()
```

**Lifecycle Stages**:
1. **Creation**: `SparkSession.builder.getOrCreate()`
2. **Active**: Execute transformations and actions
3. **Termination**: `spark.stop()` releases resources

**Important Notes**:
- One SparkSession per application (singleton pattern)
- `getOrCreate()` reuses existing session or creates new one
- SparkSession encapsulates SparkContext, SQLContext, HiveContext
- Stopping SparkSession stops the entire Spark application

### Caching and Persistence

#### Why Cache?
- Avoid recomputing expensive transformations
- Speed up iterative algorithms (ML)
- Reuse intermediate results

#### Storage Levels

```python
from pyspark import StorageLevel

# Different storage levels
df.persist(StorageLevel.MEMORY_ONLY)         # Default for cache()
df.persist(StorageLevel.MEMORY_AND_DISK)     # Spill to disk if needed
df.persist(StorageLevel.MEMORY_ONLY_SER)     # Serialized in memory
df.persist(StorageLevel.DISK_ONLY)           # Only on disk
df.persist(StorageLevel.MEMORY_AND_DISK_SER) # Serialized, spill to disk
df.persist(StorageLevel.OFF_HEAP)            # Off-heap memory

# Simple caching (= MEMORY_ONLY)
df.cache()

# Remove from cache
df.unpersist()
```

**Storage Level Options**:

| Storage Level | Space Used | CPU Time | In Memory | On Disk | Serialized | Description |
|--------------|------------|----------|-----------|---------|------------|-------------|
| MEMORY_ONLY | High | Low | Yes | No | No | Fastest, may lose data if not enough RAM |
| MEMORY_AND_DISK | High | Medium | Yes | Yes | No | Spills to disk, slower than MEMORY_ONLY |
| MEMORY_ONLY_SER | Low | High | Yes | No | Yes | More CPU, less space |
| DISK_ONLY | Low | High | No | Yes | Yes | Slowest, saves memory |
| MEMORY_AND_DISK_SER | Low | High | Yes | Yes | Yes | Balanced option |

**Best Practices**:
- Cache only frequently reused DataFrames
- Monitor memory usage (Spark UI)
- Unpersist when no longer needed
- Use MEMORY_AND_DISK for large datasets
- Consider serialization for memory constraints

### Garbage Collection

**Impact on Spark Performance**:
- Long GC pauses freeze executors
- Frequent GC reduces throughput
- Can cause "straggler" tasks

**Types of GC Issues**:
1. **Minor GC**: Collects young generation (frequent, short)
2. **Major GC**: Collects entire heap (infrequent, long pauses)

**Optimization Strategies**:

```python
# 1. Use efficient data structures
# Instead of Python objects, use DataFrames/Datasets

# 2. Reduce object creation
# Use map operations efficiently
rdd.mapPartitions(lambda iter: process_partition(iter))  # Better
# vs
rdd.map(lambda x: process_single(x))  # Creates more objects

# 3. Configure GC
spark = SparkSession.builder \
    .config("spark.executor.memory", "4g") \
    .config("spark.memory.fraction", "0.6") \
    .config("spark.memory.storageFraction", "0.5") \
    .getOrCreate()
```

**GC Tuning Configurations**:
- `spark.executor.memory`: Total executor memory
- `spark.memory.fraction`: Fraction for execution and storage (default: 0.6)
- `spark.memory.storageFraction`: Fraction of spark.memory.fraction for storage (default: 0.5)
- Use G1GC for Java 8+: `-XX:+UseG1GC`

**Monitoring GC**:
- Check Spark UI → Executors tab → GC Time
- GC time should be < 10% of task time
- Look for "stop-the-world" pauses

---

## 4. Execution Hierarchy

```
Application
    ├── Job (triggered by action)
    │   ├── Stage (divided by shuffle boundaries)
    │   │   ├── Task (one per partition)
    │   │   ├── Task
    │   │   └── Task
    │   └── Stage
    │       ├── Task
    │       └── Task
    └── Job
        └── Stage
            └── Task
```

### Hierarchy Levels

#### 1. Application
- **Definition**: Complete Spark program from start to stop
- **Lifespan**: From SparkSession creation to stop
- **Contains**: One or more jobs
- **Example**: Entire ETL pipeline script

#### 2. Job
- **Definition**: Set of stages triggered by an action
- **Trigger**: Each action (count, collect, save, show) creates a job
- **Contains**: One or more stages
- **DAG**: Represented as Directed Acyclic Graph

```python
# This creates 2 jobs (2 actions)
df = spark.read.csv("data.csv")
df_filtered = df.filter(col("age") > 25)
df_filtered.count()  # Job 1
df_filtered.write.parquet("output")  # Job 2
```

#### 3. Stage
- **Definition**: Set of tasks that can be executed in parallel
- **Division**: Stages are divided by shuffle boundaries
- **Contains**: Multiple tasks (one per partition)
- **Types**:
  - **ShuffleMapStage**: Produces output for shuffle
  - **ResultStage**: Computes final result

**Shuffle Boundaries** (create new stages):
- `groupBy`, `reduceByKey`, `join`
- `repartition`, `coalesce` (with shuffle)
- `sortBy`, `distinct`

```python
# This creates 2 stages
df1 = spark.read.csv("data1.csv")      # Stage 0
df2 = spark.read.csv("data2.csv")      # Stage 0
result = df1.join(df2, "id")           # Stage 1 (after shuffle)
result.write.parquet("output")
```

#### 4. Task
- **Definition**: Smallest unit of work sent to an executor
- **Granularity**: One task per partition
- **Execution**: Runs on a single core
- **Locality**: Prefers data-local execution

**Task Locality Levels**:
1. **PROCESS_LOCAL**: Data in same JVM
2. **NODE_LOCAL**: Data on same node
3. **RACK_LOCAL**: Data on same rack
4. **ANY**: Data anywhere in cluster

**Formula**: Number of Tasks = Number of Partitions

---

## 5. Partitioning and Shuffling

### Partitioning

#### What is a Partition?
- **Definition**: Logical chunk of data distributed across cluster
- **Purpose**: Enable parallel processing
- **Storage**: Each partition fits in memory of one executor

#### Default Partitioning
```python
# Reading files
df = spark.read.csv("data.csv")
# Default partitions = number of input blocks (typically 128MB per block)

# Parallelizing collections
rdd = spark.sparkContext.parallelize(data, numSlices=10)
# Explicit partition count

# After shuffle operations
# Default: spark.sql.shuffle.partitions = 200
```

#### Controlling Partitions

```python
# 1. repartition() - Increases or decreases partitions (full shuffle)
df_repartitioned = df.repartition(100)
df_repartitioned = df.repartition("column_name")  # Hash partitioning

# 2. coalesce() - Only decreases partitions (no full shuffle)
df_coalesced = df.coalesce(10)  # Efficient for reducing partitions

# 3. repartitionByRange() - Range partitioning with sorting
df_range = df.repartitionByRange(50, "age")

# 4. partitionBy() - For writing partitioned data
df.write.partitionBy("year", "month").parquet("output")
```

**repartition() vs coalesce()**:
| Feature | repartition() | coalesce() |
|---------|---------------|------------|
| Shuffle | Yes (full) | Minimal or none |
| Direction | Increase/Decrease | Decrease only |
| Performance | Slower | Faster |
| Use Case | Increase parallelism | Reduce before write |

#### Partition Best Practices

**Optimal Partition Size**:
- **Target**: 128MB - 1GB per partition
- **Too Small**: High overhead, too many tasks
- **Too Large**: Memory issues, spills to disk, stragglers

**Partition Count**:
- **Rule of Thumb**: 2-4 partitions per CPU core
- **Formula**: `partitions = executors × cores × 2-4`
- **Example**: 10 executors × 4 cores = 80-160 partitions

```python
# Configure shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "100")

# Check current partitions
print(df.rdd.getNumPartitions())

# Calculate optimal partitions
import math
data_size_gb = 100
target_partition_size_mb = 128
optimal_partitions = math.ceil(data_size_gb * 1024 / target_partition_size_mb)
```

### Shuffling

#### What is a Shuffle?
- **Definition**: Redistribution of data across partitions
- **Impact**: Most expensive operation in Spark
- **Involves**: Disk I/O, network I/O, serialization

#### Operations that Cause Shuffles

**1. Aggregations**:
```python
df.groupBy("category").count()
df.agg({"sales": "sum"})
```

**2. Joins**:
```python
df1.join(df2, "key")  # Shuffle join
```

**3. Repartitioning**:
```python
df.repartition(100)
df.repartitionByRange("column")
```

**4. Distinct/Dedupe**:
```python
df.distinct()
df.dropDuplicates(["key"])
```

**5. Sorting**:
```python
df.orderBy("column")
df.sortWithinPartitions("column")  # No shuffle
```

**6. Set Operations**:
```python
df1.union(df2)  # No shuffle
df1.intersect(df2)  # Shuffle
df1.subtract(df2)  # Shuffle
```

#### Shuffle Process

```
Executor 1 Partition A    →  [SHUFFLE WRITE]  →  Network  →  [SHUFFLE READ]  →  Executor 2 Partition X
Executor 1 Partition B    →  [SHUFFLE WRITE]  →  Network  →  [SHUFFLE READ]  →  Executor 2 Partition Y
Executor 2 Partition C    →  [SHUFFLE WRITE]  →  Network  →  [SHUFFLE READ]  →  Executor 3 Partition Z
```

**Steps**:
1. **Map Side**: Write shuffle data to local disk
2. **Transfer**: Send data over network
3. **Reduce Side**: Read and process data

#### Shuffle Optimization

**1. Avoid Unnecessary Shuffles**:
```python
# Bad: Multiple shuffles
df.repartition(100).groupBy("key").count()

# Good: Single shuffle
df.groupBy("key").count()  # Already optimized
```

**2. Broadcast Joins** (for small tables):
```python
from pyspark.sql.functions import broadcast

# Small table < 10MB, broadcast to all executors
large_df.join(broadcast(small_df), "key")  # No shuffle for large_df
```

**3. Partition Pruning**:
```python
# Write with partitioning
df.write.partitionBy("year", "month").parquet("output")

# Read with filter (only reads needed partitions)
spark.read.parquet("output").filter(col("year") == 2023)
```

**4. Shuffle Partition Configuration**:
```python
# Adjust based on data size
spark.conf.set("spark.sql.shuffle.partitions", "100")

# Adaptive Query Execution (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

**5. Salting for Skewed Joins**:
```python
# Add salt to skewed keys
from pyspark.sql.functions import rand, concat, lit

df_salted = df.withColumn("salted_key", concat(col("key"), lit("_"), (rand() * 10).cast("int")))
```

#### Monitoring Shuffles
- **Spark UI → SQL tab**: See shuffle read/write bytes
- **Stage Details**: Shuffle metrics per stage
- **Look for**: 
  - High shuffle read/write times
  - Data skew (some tasks much slower)
  - Spill to disk

---

## 6. Execution Patterns

### Transformations vs Actions

#### Transformations
- **Definition**: Operations that create new RDD/DataFrame from existing one
- **Execution**: Lazy (not executed immediately)
- **Return**: New RDD/DataFrame
- **Purpose**: Build computation pipeline

**Narrow Transformations** (No Shuffle):
```python
# Each input partition contributes to at most one output partition
df.select("column")
df.filter(col("age") > 25)
df.withColumn("new_col", col("old_col") * 2)
df.drop("column")
df.map(lambda x: x * 2)  # RDD
df.flatMap(lambda x: x.split())  # RDD
df.union(df2)
```

**Wide Transformations** (Shuffle Required):
```python
# Input partitions contribute to multiple output partitions
df.groupBy("key").count()
df.join(df2, "key")
df.distinct()
df.repartition(100)
df.sortBy("column")
df.reduceByKey(lambda a, b: a + b)  # RDD
```

**Common Transformations**:

| Transformation | Type | Description | Example |
|---------------|------|-------------|---------|
| select | Narrow | Select columns | `df.select("name", "age")` |
| filter / where | Narrow | Filter rows | `df.filter(col("age") > 25)` |
| withColumn | Narrow | Add/modify column | `df.withColumn("age2", col("age") * 2)` |
| drop | Narrow | Remove columns | `df.drop("column")` |
| groupBy | Wide | Group rows | `df.groupBy("key").count()` |
| join | Wide | Join DataFrames | `df1.join(df2, "key")` |
| orderBy / sort | Wide | Sort rows | `df.orderBy("age")` |
| distinct | Wide | Remove duplicates | `df.distinct()` |
| union | Narrow | Combine DataFrames | `df1.union(df2)` |
| repartition | Wide | Change partitions | `df.repartition(100)` |

#### Actions
- **Definition**: Operations that trigger computation and return results
- **Execution**: Eager (executes immediately)
- **Return**: Values, files, or output
- **Trigger**: Creates a job

**Common Actions**:

```python
# 1. Collect results to driver
df.collect()  # Returns list of Rows (use cautiously!)
df.take(10)   # Returns first 10 rows
df.first()    # Returns first row
df.head(5)    # Returns first 5 rows

# 2. Count operations
df.count()    # Returns number of rows
df.countByKey()  # RDD: counts per key

# 3. Display operations
df.show()     # Prints rows (default 20)
df.show(50, truncate=False)  # Show 50 rows, full width

# 4. Write operations
df.write.parquet("output")
df.write.csv("output.csv")
df.write.mode("overwrite").saveAsTable("table_name")

# 5. Aggregate actions
df.reduce(lambda a, b: a + b)  # RDD
df.fold(0, lambda a, b: a + b)  # RDD

# 6. Foreach operations
df.foreach(lambda row: print(row))  # Execute on executors
df.foreachPartition(lambda partition: process(partition))

# 7. Save actions
df.saveAsTextFile("output.txt")  # RDD
```

**Important Actions**:

| Action | Return Type | Description | Use Case |
|--------|-------------|-------------|----------|
| collect() | List | All rows to driver | Small results only |
| count() | Integer | Number of rows | Validation, triggers execution |
| show() | None | Display rows | Development, debugging |
| take(n) | List | First n rows | Preview data |
| write.*() | None | Save to storage | Persist results |
| foreach() | None | Apply function | Side effects |

### Lazy Evaluation

#### What is Lazy Evaluation?
- **Definition**: Transformations are not executed until an action is called
- **Purpose**: Optimize execution plan
- **Benefit**: Avoid unnecessary computations

#### How It Works

```python
# Step 1: Create DataFrame (lazy)
df = spark.read.csv("data.csv")  # No execution yet

# Step 2: Apply transformations (lazy)
df_filtered = df.filter(col("age") > 25)  # No execution yet
df_selected = df_filtered.select("name", "age")  # No execution yet
df_sorted = df_selected.orderBy("age")  # No execution yet

# Step 3: Action triggers execution
result = df_sorted.collect()  # NOW everything executes!
```

**Execution Flow**:
```
Transformations Build DAG → Action Triggered → Catalyst Optimizer → 
Physical Plan → Execute on Cluster → Return Results
```

#### Benefits of Lazy Evaluation

**1. Optimization Opportunities**:
```python
# Spark can optimize this pipeline
df.filter(col("age") > 25) \
  .filter(col("name").startswith("A")) \
  .select("name")

# Optimized to: "SELECT name FROM table WHERE age > 25 AND name LIKE 'A%'"
```

**2. Predicate Pushdown**:
```python
# Reading Parquet with filter
df = spark.read.parquet("data.parquet") \
    .filter(col("year") == 2023)  # Filter pushed to file read
```

**3. Column Pruning**:
```python
# Only read required columns
df = spark.read.parquet("data.parquet") \
    .select("name", "age")  # Only these columns read from file
```

**4. Avoid Unnecessary Work**:
```python
# If you only need 10 rows
df.filter(col("age") > 25) \
  .select("name") \
  .take(10)  # Doesn't process entire dataset
```

#### Catalyst Optimizer

**Optimization Phases**:
1. **Analysis**: Resolve column names, types
2. **Logical Optimization**: Apply rules (predicate pushdown, constant folding)
3. **Physical Planning**: Choose best physical plan
4. **Code Generation**: Generate Java bytecode

```python
# View execution plan
df.explain()  # Shows physical plan
df.explain(True)  # Shows all phases
df.explain("extended")  # Detailed plan
df.explain("formatted")  # Pretty print
```

**Example Optimization**:
```python
# Original query
df.filter(col("age") > 25) \
  .select("name", "age") \
  .filter(col("name").startswith("A"))

# Optimized by Catalyst
df.select("name", "age") \
  .filter((col("age") > 25) & col("name").startswith("A"))
```

#### Best Practices

**DO**:
- ✓ Chain transformations before action
- ✓ Use DataFrame API (more optimization)
- ✓ Cache intermediate results if reused
- ✓ Use `explain()` to check execution plan

**DON'T**:
- ✗ Call actions in loops
- ✗ Use `collect()` on large DataFrames
- ✗ Break computation into many small jobs

```python
# Bad: Multiple actions
for category in categories:
    df.filter(col("category") == category).count()  # Many jobs

# Good: Single action
df.groupBy("category").count().collect()  # One job
```

---

## 7. Apache Spark Modules

### Overview

```
┌─────────────────────────────────────────────────────┐
│                  Apache Spark                        │
├─────────────────────────────────────────────────────┤
│                   Spark Core                         │
│            (RDD, Transformations, Actions)           │
├──────────────┬──────────────┬──────────────────────┤
│  Spark SQL   │  Structured  │       MLlib          │
│  DataFrames  │  Streaming   │  (Machine Learning)  │
│  Datasets    │              │                      │
├──────────────┼──────────────┼──────────────────────┤
│  Pandas API  │   GraphX     │    SparkR            │
│  on Spark    │   (Graphs)   │   (R API)            │
└──────────────┴──────────────┴──────────────────────┘
```

### 1. Spark Core

**Foundation of Spark**:
- **RDD API**: Resilient Distributed Datasets
- **Task Scheduling**: Distribute work across cluster
- **Memory Management**: In-memory computation
- **Fault Recovery**: Lineage-based recovery

**RDD (Resilient Distributed Dataset)**:
```python
# Create RDD
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

# Transformations
rdd_mapped = rdd.map(lambda x: x * 2)
rdd_filtered = rdd.filter(lambda x: x > 2)

# Actions
result = rdd.collect()
count = rdd.count()
```

**Key Features**:
- Immutable distributed collections
- Lazy evaluation
- Fault-tolerant through lineage
- In-memory computation
- Partitioned across cluster

**Use Cases**:
- Low-level control needed
- Unstructured data processing
- Complex transformations not available in DataFrame API

### 2. Spark SQL

**SQL Interface for Spark**:
- Query structured data using SQL
- Integrate with Hive
- Catalyst optimizer
- Support for multiple data formats

**Features**:
```python
# Create table/view
df.createOrReplaceTempView("people")

# SQL queries
result = spark.sql("""
    SELECT name, age 
    FROM people 
    WHERE age > 25 
    ORDER BY age DESC
""")

# Hive integration
spark.sql("CREATE TABLE users (id INT, name STRING)")
spark.sql("INSERT INTO users VALUES (1, 'Alice')")

# Read from various sources
df = spark.read.format("parquet").load("data.parquet")
df = spark.read.jdbc(url, table, properties)
```

**Supported Data Sources**:
- Parquet, ORC, JSON, CSV
- Hive Tables
- JDBC (databases)
- Avro, Delta Lake
- Custom data sources

**Use Cases**:
- Business intelligence
- Data warehousing
- ETL pipelines
- Ad-hoc analysis

### 3. DataFrames

**High-Level API for Structured Data**:
- Distributed collection of data organized into columns
- Immutable and lazy
- Schema enforcement
- Catalyst optimizer

**Creating DataFrames**:
```python
# From files
df = spark.read.csv("data.csv", header=True, inferSchema=True)
df = spark.read.json("data.json")
df = spark.read.parquet("data.parquet")

# From RDD
rdd = spark.sparkContext.parallelize([(1, "Alice"), (2, "Bob")])
df = rdd.toDF(["id", "name"])

# From collections
data = [(1, "Alice", 25), (2, "Bob", 30)]
df = spark.createDataFrame(data, ["id", "name", "age"])
```

**Common Operations**:
```python
# Selection
df.select("name", "age")
df.selectExpr("name", "age + 1 as age_next")

# Filtering
df.filter(col("age") > 25)
df.where("age > 25")

# Aggregation
df.groupBy("department").agg({"salary": "avg"})
df.groupBy("dept").count()

# Joins
df1.join(df2, "key")
df1.join(df2, df1.id == df2.user_id, "left")

# Window functions
from pyspark.sql.window import Window
windowSpec = Window.partitionBy("dept").orderBy("salary")
df.withColumn("rank", rank().over(windowSpec))
```

**Use Cases**:
- Structured data processing
- ETL pipelines
- Data cleaning and transformation
- Analytics

### 4. Datasets (Scala/Java Only)

**Type-Safe DataFrames**:
- Compile-time type safety
- Object-oriented programming
- Encoder-based serialization

**Example (Scala)**:
```scala
case class Person(name: String, age: Int)

val ds = spark.read
  .json("people.json")
  .as[Person]

// Type-safe operations
val adults = ds.filter(_.age >= 18)
val names = ds.map(_.name)
```

**Benefits**:
- Catch errors at compile time
- IDE autocomplete
- Refactoring support
- Same performance as DataFrames

### 5. Pandas API on Spark

**Pandas Compatibility on Spark**:
- Use familiar Pandas syntax
- Scale to big data
- Distributed execution

**Example**:
```python
import pyspark.pandas as ps

# Create Pandas-on-Spark DataFrame
psdf = ps.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35]
})

# Pandas-like operations
psdf['age_group'] = psdf['age'] // 10
filtered = psdf[psdf['age'] > 25]
grouped = psdf.groupby('age_group').mean()

# Convert to/from Spark DataFrame
sdf = psdf.to_spark()
psdf = ps.from_spark(sdf)

# Convert to Pandas (collect to driver)
pdf = psdf.to_pandas()
```

**Differences from Pandas**:
- Distributed (not single machine)
- Lazy evaluation (some operations)
- Some functions not supported
- Different performance characteristics

**Use Cases**:
- Migrate Pandas code to Spark
- Scale existing analytics
- Data scientists familiar with Pandas

### 6. Structured Streaming

**Stream Processing with DataFrame API**:
- Micro-batch processing
- Exactly-once semantics
- Fault-tolerant
- Same API as batch

**Basic Example**:
```python
# Read stream
streamDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host:port") \
    .option("subscribe", "topic") \
    .load()

# Process stream
processedDF = streamDF \
    .select("value") \
    .groupBy("category") \
    .count()

# Write stream
query = processedDF.writeStream \
    .format("parquet") \
    .option("path", "output") \
    .option("checkpointLocation", "checkpoint") \
    .outputMode("append") \
    .start()

query.awaitTermination()
```

**Output Modes**:
- **Append**: Only new rows (default)
- **Complete**: All rows (aggregations)
- **Update**: Only updated rows

**Supported Sources**:
- Kafka
- Files (CSV, JSON, Parquet)
- Sockets
- Rate source (testing)

**Supported Sinks**:
- Files
- Kafka
- Console (testing)
- Foreach/ForeachBatch (custom)
- Memory (testing)

**Use Cases**:
- Real-time analytics
- ETL pipelines
- Fraud detection
- IoT data processing

### 7. MLlib (Machine Learning Library)

**Distributed Machine Learning**:
- Classification, regression
- Clustering
- Dimensionality reduction
- Feature engineering
- Model evaluation

**ML Pipeline Example**:
```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression

# Feature engineering
assembler = VectorAssembler(
    inputCols=["age", "income"],
    outputCol="features"
)

scaler = StandardScaler(
    inputCol="features",
    outputCol="scaled_features"
)

# Model
lr = LogisticRegression(
    featuresCol="scaled_features",
    labelCol="label"
)

# Create pipeline
pipeline = Pipeline(stages=[assembler, scaler, lr])

# Train
model = pipeline.fit(training_data)

# Predict
predictions = model.transform(test_data)
```

**Algorithms**:

**Classification**:
- Logistic Regression
- Decision Trees
- Random Forests
- Gradient Boosted Trees
- Naive Bayes

**Regression**:
- Linear Regression
- Decision Tree Regression
- Random Forest Regression
- Gradient Boosted Tree Regression

**Clustering**:
- K-Means
- Gaussian Mixture
- LDA (topic modeling)

**Feature Transformers**:
- VectorAssembler
- StandardScaler, MinMaxScaler
- StringIndexer, OneHotEncoder
- PCA
- Tokenizer, HashingTF, IDF

**Use Cases**:
- Predictive modeling
- Recommendation systems
- Customer segmentation
- Fraud detection

---

## Practice Questions

### Question 1: Advantages and Challenges
**Q**: What are the main advantages of using Spark over traditional MapReduce?

<details>
<summary>Answer</summary>

1. **Speed**: 100x faster with in-memory processing
2. **Ease of Use**: High-level APIs in multiple languages
3. **Unified Engine**: Batch, streaming, ML, graph in one platform
4. **Advanced Analytics**: MLlib and GraphX built-in
5. **Lazy Evaluation**: Optimizes execution plan
</details>

### Question 2: Architecture Components
**Q**: Explain the role of the Driver node in Spark architecture.

<details>
<summary>Answer</summary>

The Driver node:
- Runs the main() function
- Creates SparkContext/SparkSession
- Converts user program into tasks (DAG)
- Schedules tasks on executors via Task Scheduler
- Maintains RDD/DataFrame metadata
- Collects results from executors
</details>

### Question 3: Memory Management
**Q**: What is the default split of executor memory in Spark 2.x+?

<details>
<summary>Answer</summary>

- **Execution Memory (60%)**: 36% of total (60% of 60%)
- **Storage Memory (40%)**: 24% of total (40% of 60%)
- **User Memory**: 40% of total
- **Reserved Memory**: 300MB

The 60/40 split is unified and can dynamically adjust between execution and storage.
</details>

### Question 4: Partitioning
**Q**: When should you use `repartition()` vs `coalesce()`?

<details>
<summary>Answer</summary>

**Use repartition()** when:
- Increasing number of partitions
- Need even distribution (hash partitioning)
- Performance penalty of shuffle is acceptable

**Use coalesce()** when:
- Decreasing number of partitions
- Avoiding shuffle for performance
- Before writing to reduce output files
</details>

### Question 5: Transformations vs Actions
**Q**: Classify these operations as narrow transformation, wide transformation, or action:
- `filter()`
- `groupBy()`
- `count()`
- `join()`

<details>
<summary>Answer</summary>

- `filter()`: **Narrow Transformation**
- `groupBy()`: **Wide Transformation** (shuffle)
- `count()`: **Action** (triggers execution)
- `join()`: **Wide Transformation** (shuffle)
</details>

### Question 6: Caching
**Q**: What's the difference between `cache()` and `persist(StorageLevel.MEMORY_AND_DISK)`?

<details>
<summary>Answer</summary>

- `cache()`: Shorthand for `persist(StorageLevel.MEMORY_ONLY)`
  - Stores in memory only
  - Loses data if not enough RAM
  
- `persist(StorageLevel.MEMORY_AND_DISK)`:
  - Tries memory first
  - Spills to disk if insufficient memory
  - More resilient but potentially slower
</details>

### Question 7: Lazy Evaluation
**Q**: In the following code, when does execution actually occur?
```python
df = spark.read.csv("data.csv")
df2 = df.filter(col("age") > 25)
df3 = df2.select("name")
result = df3.collect()
```

<details>
<summary>Answer</summary>

Execution occurs at line 4 with `collect()` (action). The first three lines only build the execution plan (transformations are lazy).
</details>

### Question 8: Shuffling
**Q**: Which operations cause a shuffle?

<details>
<summary>Answer</summary>

Operations causing shuffle:
- `groupBy()`, `agg()`
- `join()` (except broadcast join)
- `distinct()`
- `repartition()`
- `orderBy()`, `sort()`
- `intersection()`, `subtract()`
- `reduceByKey()`, `groupByKey()`
</details>

### Question 9: Execution Hierarchy
**Q**: How many jobs, stages, and tasks are created in this code (assume 10 partitions)?
```python
df = spark.read.csv("data.csv")
df.filter(col("age") > 25).count()
df.groupBy("name").count().show()
```

<details>
<summary>Answer</summary>

- **Jobs**: 2 (two actions: `count()` and `show()`)
- **Stages**: 
  - Job 1: 1 stage (no shuffle)
  - Job 2: 2 stages (shuffle from groupBy)
  - Total: 3 stages
- **Tasks**: Minimum 10 per stage (one per partition) = ~30 tasks total
</details>

### Question 10: DataFrames vs RDDs
**Q**: When should you use RDDs instead of DataFrames?

<details>
<summary>Answer</summary>

Use RDDs when:
- Need fine-grained control over data
- Working with unstructured data (text, binary)
- Complex transformations not expressible in DataFrame API
- Need to maintain backward compatibility

Otherwise, prefer DataFrames for:
- Better performance (Catalyst optimizer)
- Easier API
- Automatic optimization
- Schema enforcement
</details>

---

## Key Takeaways for Exam

### Must-Know Concepts

1. **Architecture**:
   - Driver creates DAG and schedules tasks
   - Executors run tasks and cache data
   - One task per partition per core

2. **Memory**:
   - 60% unified (execution + storage)
   - 40% user memory
   - 300MB reserved

3. **Partitioning**:
   - Target: 128MB-1GB per partition
   - 2-4 partitions per core
   - `repartition()` vs `coalesce()`

4. **Execution**:
   - Transformations are lazy
   - Actions trigger jobs
   - Shuffles create stage boundaries

5. **Optimization**:
   - Cache frequently used data
   - Avoid shuffles when possible
   - Use broadcast for small joins
   - Enable AQE (Adaptive Query Execution)

6. **APIs**:
   - DataFrame > RDD for structured data
   - SQL for familiar interface
   - Pandas API for migration

### Common Pitfalls

❌ Using `collect()` on large DataFrames  
❌ Too many small partitions (overhead)  
❌ Too few large partitions (memory issues)  
❌ Not caching iterative computations  
❌ Unnecessary shuffles  
❌ Ignoring data skew  

### Best Practices

✓ Use DataFrames over RDDs  
✓ Cache strategically  
✓ Monitor Spark UI  
✓ Configure partitions appropriately  
✓ Use broadcast joins for small tables  
✓ Enable Adaptive Query Execution  
✓ Use `explain()` to understand plans  

---

## Additional Resources

- **Official Documentation**: [spark.apache.org/docs](https://spark.apache.org/docs/latest/)
- **Spark UI**: Monitor jobs, stages, tasks at `http://driver:4040`
- **Practice**: Databricks Community Edition for hands-on practice
- **Books**: 
  - "Learning Spark" (O'Reilly)
  - "Spark: The Definitive Guide" (O'Reilly)

---

Good luck with your certification! 🎓
