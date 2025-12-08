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
- **Elastic Resources**: Dynamic allocation adjusts to workload

**Rich Ecosystem**
- **Spark SQL**: SQL queries on structured data
- **MLlib**: Distributed machine learning library
- **Structured Streaming**: Real-time stream processing
- **GraphX**: Graph processing and analytics

### Challenges ✗

**1. Cluster Configuration & Resource Management**
- Sizing executors (memory, cores) appropriately is complex
- Balancing driver vs executor resources
- Managing dynamic allocation settings
- Configuring correct number of partitions for optimal parallelism

**2. Data Skew**
- Uneven data distribution across partitions
- Some tasks take much longer than others (stragglers)
- Causes poor cluster utilization and wasted resources
- Requires techniques like salting, broadcast joins, or AQE to mitigate

**3. Shuffle Operations**
- Most expensive operation in Spark (disk I/O, network I/O, serialization)
- Caused by joins, groupBy, distinct, repartition
- Can bottleneck performance significantly
- Requires careful query design to minimize shuffle

**4. Memory Management**
- Out-of-memory errors (both driver and executor)
- GC overhead and long pauses freeze executors
- Balancing storage vs execution memory
- Managing broadcast variable sizes to avoid driver OOM

**5. Debugging & Monitoring Complexity**
- Distributed nature makes debugging challenging
- Logs spread across multiple nodes
- Difficult to reproduce issues in local environment
- Requires deep understanding of Spark UI metrics

**6. Serialization Issues**
- Serialization errors with UDFs and closures
- Performance overhead of serialization/deserialization
- Incompatible types between driver and executors
- Non-serializable objects causing task failures

**7. Small Files Problem**
- Too many small files = excessive task overhead and metadata
- Too few files = poor parallelism
- Requires proper partitioning and file compaction strategies

**8. Late Data & State Management (Streaming)**
- Handling out-of-order events correctly
- Configuring watermarks appropriately
- Managing growing state size that can cause OOM
- Ensuring exactly-once semantics with checkpointing

**9. Learning Curve**
- Understanding lazy evaluation and when computation happens
- Knowing when to cache/persist vs recompute
- Choosing between narrow and wide transformations
- Writing code that optimizes well with Catalyst optimizer

**10. Cost Optimization**
- Achieving efficient cluster utilization
- Avoiding over-provisioning of resources
- Balancing processing speed vs infrastructure cost
- Managing cloud resource costs effectively

**Mitigation Strategies**:
- Enable Adaptive Query Execution (AQE) for automatic optimization
- Use broadcast joins for small tables (< 10MB)
- Monitor performance with Spark UI
- Test with representative data samples
- Profile and tune iteratively
- Implement proper partitioning strategies

---

## 2. Core Components of Apache Spark Architecture

### Cluster Components

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           SPARK APPLICATION                                     │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌────────────────────────────────────────┐                                     │
│  │            DRIVER PROGRAM              │                                     │
│  │  ┌──────────────────────────────────┐  │                                     │
│  │  │         SparkSession             │  │                                     │
│  │  │    (SparkContext + SQLContext)   │  │                                     │
│  │  └──────────────────────────────────┘  │                                     │
│  │  ┌──────────────────────────────────┐  │                                     │
│  │  │         DAG Scheduler            │  │  Converts jobs into stages          │
│  │  └──────────────────────────────────┘  │                                     │
│  │  ┌──────────────────────────────────┐  │                                     │
│  │  │        Task Scheduler            │  │  Assigns tasks to executors         │
│  │  └──────────────────────────────────┘  │                                     │
│  └────────────────────────────────────────┘                                     │
│                        │                                                        │
│                        │ Communicates via                                       │
│                        ▼ Cluster Manager                                        │
│  ┌────────────────────────────────────────┐                                     │
│  │          CLUSTER MANAGER               │                                     │
│  │   (Standalone / YARN / Mesos / K8s)    │                                     │
│  └────────────────────────────────────────┘                                     │
│                        │                                                        │
│          ┌─────────────┼─────────────┐                                          │
│          │             │             │                                          │
│          ▼             ▼             ▼                                          │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐                             │
│  │ WORKER NODE 1│ │ WORKER NODE 2│ │ WORKER NODE 3│                             │
│  │              │ │              │ │              │                             │
│  │ ┌──────────┐ │ │ ┌──────────┐ │ │ ┌──────────┐ │                             │
│  │ │ EXECUTOR │ │ │ │ EXECUTOR │ │ │ │ EXECUTOR │ │                             │
│  │ │          │ │ │ │          │ │ │ │          │ │                             │
│  │ │ ┌──────┐ │ │ │ │ ┌──────┐ │ │ │ │ ┌──────┐ │ │                             │
│  │ │ │ Task │ │ │ │ │ │ Task │ │ │ │ │ │ Task │ │ │                             │
│  │ │ └──────┘ │ │ │ │ └──────┘ │ │ │ │ └──────┘ │ │                             │
│  │ │ ┌──────┐ │ │ │ │ ┌──────┐ │ │ │ │ ┌──────┐ │ │                             │
│  │ │ │ Task │ │ │ │ │ │ Task │ │ │ │ │ │ Task │ │ │                             │
│  │ │ └──────┘ │ │ │ │ └──────┘ │ │ │ │ └──────┘ │ │                             │
│  │ │ ┌──────┐ │ │ │ │ ┌──────┐ │ │ │ │ ┌──────┐ │ │                             │
│  │ │ │Cache │ │ │ │ │ │Cache │ │ │ │ │ │Cache │ │ │                             │
│  │ │ └──────┘ │ │ │ │ └──────┘ │ │ │ │ └──────┘ │ │                             │
│  │ └──────────┘ │ │ └──────────┘ │ │ └──────────┘ │                             │
│  └──────────────┘ └──────────────┘ └──────────────┘                             │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘

                    ┌─────────────────────────────────┐
                    │        DATA SOURCES             │
                    │  HDFS │ S3 │ JDBC │ Kafka │ ... │
                    └─────────────────────────────────┘
```



### 1. Cluster
- **Definition**: Collection of machines (nodes) working together
- **Purpose**: Distributed computation and storage


### 2. Driver Node
- **Definition**: It is the main controller for application execution and maintains the state of the Spark cluster.In other words, it tracks what the executors are doing, which tasks have already been completed, and which ones still need to run. In addition, the driver needs to communicate with the cluster manager to obtain physical resources (CPU, memory, etc.) and to start the executables.

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

![Alt text of the image](https://github.com/afaqueahmad7117/spark-experiments/blob/main/docs/images/memory_management.png)

Image from https://github.com/afaqueahmad7117/spark-experiments/

- **JVM on Heap Memory** (`spark.executor.memory`):
  - **Execution Memory**: For computations (shuffles, joins, sorts, aggregations)
  - **Storage Memory**: For caching and broadcast variables
  - **User Memory**: For user data structures
  - **Reserved Memory** (300MB): For Spark internal objects
    
- **Off-Heap Memory**

  Off-Heap memory means allocating memory objects (serialized to byte array) to memory outside the heap of the Java virtual machine(JVM), which is directly managed by the operating system (not the virtual machine), but stored outside the process heap in native memory (therefore, they are not processed by the garbage collector). The result of this is to keep a smaller heap to reduce the impact of garbage collection on the application. Accessing this data is slightly slower than accessing the on-heap storage, but still faster than reading/writing from a disk. The downside is that the user has to manually deal with managing the allocated memory
  
- **Overhead Memory**  
- **Driver Memory** (`spark.driver.memory`):
  - Stores metadata about partitions
  - Collects results from executors
  - Runs the main application

### 7. Cluster Manager
The Cluster Manager manages the set of machines (the cluster) where your Spark applications will run. It manages physical resources (machines, memory, CPU) and has its own driver (or master) and worker architecture — but here we are talking in terms of physical machines, not Spark processes.

**Main Functions of the Cluster Manager:**

1. **Resource Allocation**
   - Allocates CPU and memory to Spark applications
   - Decides which machines will execute tasks
   - Manages resource competition between multiple applications

2. **Cluster Health Monitoring**
   - Monitors the state of worker nodes
   - Detects failures and reallocates resources when necessary
   - Maintains information about resource availability

3. **Executor Lifecycle Management**
   - Starts executors on worker nodes as requested by the Driver
   - Terminates executors when the application finishes or in case of failure
   - Handles automatic recovery in fault scenarios

4. **Coordination Between Components**
   - Acts as intermediary between the Driver and Workers
   - Communicates resource availability to the Driver
   - Distributes information about which workers will run each executor

**Available Cluster Managers:**

| Manager | Characteristics |
|---------|-----------------|
| **Standalone** | Spark's native cluster manager, simple to configure |
| **YARN** | Hadoop's resource manager, ideal for environments with existing Hadoop infrastructure |
| **Mesos** | General-purpose cluster manager, supports multiple frameworks |
| **Kubernetes** | Orchestration of containers, modern and highly scalable |

**Cluster Manager Workflow:**
```
┌──────────────────────────────────────────────────────────────────┐
│                    CLUSTER MANAGER WORKFLOW                      │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│   1. Driver requests resources from Cluster Manager              │
│                         │                                        │
│                         ▼                                        │
│   2. Cluster Manager checks available resources in Workers       │
│                         │                                        │
│                         ▼                                        │
│   3. Cluster Manager allocates resources and starts Executors    │
│                         │                                        │
│                         ▼                                        │
│   4. Driver sends tasks directly to Executors                    │
│                         │                                        │
│                         ▼                                        │
│   5. Executors process tasks and return results to Driver        │
│                         │                                        │
│                         ▼                                        │
│   6. At the end, Cluster Manager releases allocated resources    │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

**Important Note:** 

- **The Cluster Manager does NOT execute Spark code** - it only manages resources.
- **The Driver communicates directly with the Executors** after allocation.
- **Dynamic allocation** allows for automatic adjustment of executors.
- **In Local mode**, there is no external cluster manager (everything runs in a JVM).

---

## 3. Spark Architecture Deep Dive

### DataFrame and Dataset Concepts

#### DataFrames

DataFrame is a distributed in-memory collection of data organized into named columns similar to a table in relational database.

```python
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
- **Schema**: Tabular structured with column names and types
- **Immutable**: Cannot be changed after creation


#### Datasets
Dataset is a type-safe version of DataFrame (Scala/Java only) that provides compile-time type safety.

```scala
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
- DataFrame = collection of generic objects, Dataset[Row], where a Row is a generic untyped JVM object that may hold different types of fields.
- Dataset provides type safety at compile time
- In Spark’s supported languages, Datasets make sense only in Java and Scala, because types are bound to variables and objects at compile time. In Scala, however, a DataFrame is just an alias for untyped Dataset[Row].
- In Python and R only DataFrames make sense. This is because Python types are dynamically inferred or assigned during execution, not during compile time. 

### SparkSession Lifecycle

The SparkSession, introduced in Spark 2.0, provides a unified entry point for programming Spark with the Structured APIs. You can use a SparkSession to access Spark functionality: just import the class and create an instance in your code.
To issue any SQL query, use the sql() method on the SparkSession instance, spark, such as spark.sql("SELECT * FROM myTableName"). All spark.sql queries executed in this manner return a DataFrame on which you may perform further Spark operations if you desire.

#### 1. Creation Phase

```python
from pyspark.sql import SparkSession

# Standard creation
spark = SparkSession.builder \
    .appName("MyApplication") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()
```

**Key Points:**
- `builder` pattern allows fluent configuration
- `getOrCreate()` returns existing session if one exists (singleton pattern)
- `master()` specifies cluster manager (local, yarn, mesos, k8s)
- Configuration can be set at creation time

**What happens during creation:**
1. SparkContext is initialized (if not already existing)
2. SQL Context is created
3. Hive support is enabled (if configured)
4. Catalog is initialized for metadata management
5. Session state is established

#### 2. Active Phase (Usage)

During this phase, you perform all Spark operations:

```python
# Reading data
df = spark.read.parquet("data.parquet")
df = spark.read.json("data.json")

# Creating DataFrames
df = spark.createDataFrame(data, schema)

# SQL operations
df.createOrReplaceTempView("my_table")
result = spark.sql("SELECT * FROM my_table WHERE age > 25")

# Accessing catalog
spark.catalog.listDatabases()
spark.catalog.listTables()

# Configuration changes at runtime
spark.conf.set("spark.sql.shuffle.partitions", "100")
current_value = spark.conf.get("spark.sql.shuffle.partitions")

# Creating new sessions (isolated SQL configs)
newSession = spark.newSession()
```

**Session Components Available:**

| Component | Access | Purpose |
|-----------|--------|---------|
| SparkContext | `spark.sparkContext` | Low-level RDD operations |
| SQL Context | Built-in | SQL query execution |
| Catalog | `spark.catalog` | Metadata management |
| Conf | `spark.conf` | Runtime configuration |
| UDF Registration | `spark.udf` | Register user-defined functions |

#### 3. Session vs Context Relationship

```
┌─────────────────────────────────────────────────────┐
│                  SparkSession                       │
│  ┌───────────────────────────────────────────────┐  │
│  │              SparkContext                     │  │
│  │  (One per JVM - shared across sessions)       │  │
│  └───────────────────────────────────────────────┘  │
│  ┌───────────────┐  ┌───────────────────────────┐   │
│  │  SQL Config   │  │       Catalog             │   │
│  │  (Per session)│  │   (Metadata management)   │   │
│  └───────────────┘  └───────────────────────────┘   │
│  ┌───────────────────────────────────────────────┐  │
│  │            Session State                      │  │
│  │  (Temp views, UDFs, current database)         │  │
│  └───────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
```

**Important:** 
- Only **one SparkContext** per JVM
- Multiple SparkSessions can share the same SparkContext
- Each session has isolated SQL configurations and temp views

#### 4. Multiple Sessions

```python
# Create a new isolated session
spark2 = spark.newSession()

# Each session has isolated:
# - Temporary views
# - SQL configurations
# - Registered UDFs

# Session 1
spark.conf.set("spark.sql.shuffle.partitions", "100")
df.createOrReplaceTempView("table1")

# Session 2 - isolated config and views
spark2.conf.set("spark.sql.shuffle.partitions", "200")
# spark2.sql("SELECT * FROM table1")  # ERROR - table1 not visible

# But they share:
# - SparkContext (cluster resources)
# - Cached DataFrames
# - Permanent tables in metastore
```

#### 5. Termination Phase

```python
# Stop the SparkSession (and underlying SparkContext)
spark.stop()
```

**What happens during termination:**
1. All running jobs are cancelled
2. Executors are released
3. Cached data is cleared
4. SparkContext is stopped
5. Connection to cluster manager is closed
6. All resources are freed

**Important Considerations:**
```python
# After stop(), the session cannot be reused
spark.stop()
# spark.read.csv("data.csv")  # ERROR - session already stopped

# You must create a new session
spark = SparkSession.builder.getOrCreate()  # Creates new session
```

#### 6. Lifecycle States Summary

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   CREATED    │ ──► │    ACTIVE    │ ──► │   STOPPED    │
│              │     │              │     │              │
│ - Context    │     │ - Read/Write │     │ - Resources  │
│   initialized│     │ - SQL queries│     │   released   │
│ - Resources  │     │ - Processing │     │ - Cannot     │
│   allocated  │     │ - Caching    │     │   reuse      │
└──────────────┘     └──────────────┘     └──────────────┘
                            │
                            ▼
                     ┌──────────────┐
                     │  NEW SESSION │
                     │  (Optional)  │
                     │              │
                     │ - Isolated   │
                     │   config     │
                     │ - Shared     │
                     │   context    │
                     └──────────────┘
```

#### 7. Common Patterns

**Pattern 1: Application Entry Point**
```python
def main():
    spark = SparkSession.builder \
        .appName("MyETL") \
        .getOrCreate()
    
    try:
        run_etl(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
```

**Pattern 2: Notebook/Interactive**
```python
# In Databricks/Jupyter - session often pre-created
# Access existing session
spark = SparkSession.builder.getOrCreate()

# Don't call spark.stop() in notebooks!
```

**Pattern 3: Testing**
```python
# Create isolated session for tests
@pytest.fixture
def spark_session():
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("test") \
        .getOrCreate()
    yield spark
    spark.stop()
```

#### 8. Lifecycle Best Practices

✓ Use `getOrCreate()` to avoid duplicate sessions  
✓ Single session per application (typical pattern)  
✓ Always stop session when done (releases cluster resources)  
✓ Use try/finally for graceful shutdown  
✓ Don't call `spark.stop()` in notebooks (session is managed)  
✓ Use `newSession()` for isolated SQL configs when needed

### Caching and Persistence

#### Cache
In applications that reuse the same datasets over and over, one of the most useful optimizations is
caching. Caching will place a DataFrame, table, or RDD into temporary storage (either memory
or disk) across the executors in your cluster, and make subsequent reads faster. Although caching
might sound like something we should do all the time, it’s not always a good thing to do. That’s
because caching data incurs a serialization, deserialization, and storage cost. For example, if you
are only going to process a dataset once (in a later transformation), caching it will only slow you
down.
The **cache** command in Spark always places data in memory by default, caching only part of the
dataset if the cluster’s total memory is full. For more control, there is also a **persist** method that
takes a StorageLevel object to specify where to cache the data: in memory, on disk, or both.
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

A garbage collector is a form of automatic memory management in computer science that reclaims memory no longer in use by a program. It operates by identifying and freeing memory that is no longer referenced, which prevents memory leaks and improves performance by ensuring memory is available for reuse.
During the course of running Spark jobs, the executor or driver machines may struggle to complete their tasks because of a lack of sufficient memory or “memory pressure.” This may occur when an application takes up too much memory during execution or when garbage collection runs too frequently or is slow to run as large numbers of objects are created in the JVM and subsequently garbage collected as they are no longer used. One strategy for easing this issue is to ensure that you’re using the Structured APIs as much as possible. These will not only
increase the efficiency with which your Spark jobs will execute, but it will also greatly reduce memory pressure because JVM objects are never realized and Spark SQL simply performs the
computation on its internal format.

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
- **Definition**: Groups of tasks that can be executed together to compute the same operation on multiple machines.
- **Division**: Stages are divided by shuffle boundaries. Spark starts a new stage after each
shuffle, and keeps track of what order the stages must run in to compute the final result (A shuffle represents a physical repartitioning of the data).Regardless of the number of partitions, that entire stage is computed in parallel. The final result aggregates those partitions individually, brings them all to a single partition before finally sending the final result to the driver.
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
- Operations that create new RDD/DataFrame from existing one
- All transformations are evaluated lazily. That is, their results are not computed until an action is invoked or data is “touched” (read from or written to disk).
- Their results are recorded or remembered as a lineage, allowing spark, at later time, to promote optimizations for more efficient execution.


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
┌────────────────────────────────────────────────────┐
│                  Apache Spark                      │
├────────────────────────────────────────────────────┤
│                   Spark Core                       │
│            (RDD, Transformations, Actions)         │
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

**Pandas API on Spark** (formerly known as Koalas) allows you to use the familiar Pandas syntax to process data at distributed scale using Apache Spark.

#### Main Characteristics

**1. Identical Pandas Syntax**
```python
import pyspark.pandas as ps

# Create DataFrame - same Pandas syntax
psdf = ps.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35],
    'salary': [50000, 60000, 70000]
})

# Familiar operations
psdf['bonus'] = psdf['salary'] * 0.1
psdf = psdf[psdf['age'] > 25]
psdf.groupby('age').mean()
```

**2. Distributed Execution**
- Data is distributed in partitions across the cluster
- Parallel processing on multiple executors
- Scales to terabytes of data

**3. Easy Conversion**
```python
# Pandas → Pandas on Spark
import pandas as pd
pdf = pd.DataFrame({'a': [1, 2, 3]})
psdf = ps.from_pandas(pdf)

# Pandas on Spark → Spark DataFrame
spark_df = psdf.to_spark()

# Spark DataFrame → Pandas on Spark
psdf = ps.DataFrame(spark_df)
# or
psdf = spark_df.pandas_api()

# Pandas on Spark → Pandas (caution: collects to driver!)
pdf = psdf.to_pandas()
```

**4. Index Support**
```python
# Pandas on Spark supports indexes like Pandas
psdf = ps.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
psdf = psdf.set_index('a')
psdf.loc[2]  # Access by index
```

**5. Integration with Spark Ecosystem**
- Access to Spark SQL
- Compatible with Delta Lake
- Uses Catalyst Optimizer for optimization

#### Advantages

| Advantage | Description |
|-----------|-------------|
| **Low Learning Curve** | Data scientists already familiar with Pandas can use Spark immediately |
| **Scalability** | Processes datasets much larger than single machine memory |
| **Easy Migration** | Existing Pandas code can be migrated with minimal changes |
| **Productivity** | Rich and expressive API for data analysis |
| **Performance** | Leverages Spark optimizations (Catalyst, Tungsten) |
| **Flexibility** | Can switch between Pandas API and Spark DataFrame API as needed |

#### Comparison: Pandas vs Pandas on Spark

| Aspect | Pandas | Pandas on Spark |
|--------|--------|-----------------|
| **Execution** | Single-node (1 machine) | Distributed (cluster) |
| **Data Limit** | Available RAM memory | Scales to PB |
| **Evaluation** | Eager (immediate) | Lazy (for some operations) |
| **Ordering** | Guaranteed by default | Not guaranteed (distributed) |
| **Small data performance** | Faster | Spark overhead |
| **Big Data performance** | Not supported | Highly efficient |

#### Practical Examples

**Common Operations**:
```python
import pyspark.pandas as ps

# Reading data
psdf = ps.read_csv("large_file.csv")
psdf = ps.read_parquet("data.parquet")

# Exploratory analysis
psdf.head(10)
psdf.describe()
psdf.info()
psdf.shape

# Data manipulation
psdf['new_col'] = psdf['col1'] + psdf['col2']
psdf = psdf.dropna()
psdf = psdf.fillna(0)
psdf = psdf.drop_duplicates()

# Aggregations
psdf.groupby('category').agg({
    'sales': 'sum',
    'quantity': 'mean'
})

# Sorting
psdf = psdf.sort_values('date', ascending=False)

# Merge/Join
result = ps.merge(psdf1, psdf2, on='key', how='left')
```

**Window Functions**:
```python
# Window functions work like in Pandas
psdf['rolling_avg'] = psdf.groupby('category')['sales'].transform(
    lambda x: x.rolling(7).mean()
)
```

**Plotting (Visualization)**:
```python
# Visualization support
psdf['sales'].plot.hist()
psdf.plot.scatter(x='age', y='salary')
```

#### Limitations and Considerations

**⚠️ Differences from Traditional Pandas**:

1. **Ordering not guaranteed**
```python
# Pandas on Spark is distributed - order may vary
# Use sort_values() explicitly when order matters
psdf = psdf.sort_values('column')
```

2. **Some functions not supported**
```python
# Not all Pandas functions are available
# Check documentation for compatibility
```

3. **Operations that collect data**
```python
# Be careful with operations that bring data to driver
psdf.to_pandas()  # Collects ALL data - may cause OOM
psdf.to_numpy()   # Same issue
```

4. **Default Index**
```python
# By default, uses distributed sequence index
# Can be expensive for large datasets
ps.set_option('compute.default_index_type', 'distributed')
```

#### When to Use Pandas API on Spark?

**✅ Use when:**
- Dataset larger than single machine memory
- Team already knows Pandas and wants to scale
- Quick migration of existing Pandas code
- Exploratory analysis on Big Data
- Prototyping before optimizing with Spark DataFrame API

**❌ Prefer Spark DataFrame API when:**
- Performance is critical
- Complex production pipelines
- Need fine-grained control over partitioning
- Operations not supported by Pandas API

#### Visual Summary

```
┌─────────────────────────────────────────────────────────────┐
│                  PANDAS API ON SPARK                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────────┐     ┌─────────────────────────────┐   │
│  │   Pandas API    │ ──► │   Spark Execution Engine    │   │
│  │   (Familiar)    │     │   (Distributed Processing)  │   │
│  └─────────────────┘     └─────────────────────────────┘   │
│                                                             │
│  ADVANTAGES:                                                │
│  ✓ Familiar Pandas syntax                                   │
│  ✓ Spark scalability                                        │
│  ✓ Easy migration of existing code                          │
│  ✓ Simple conversion between APIs                           │
│  ✓ Automatic optimization (Catalyst)                        │
│                                                             │
│  IDEAL FOR:                                                 │
│  • Data Scientists migrating to Big Data                    │
│  • Exploratory analysis on large datasets                   │
│  • Rapid prototyping                                        │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

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
