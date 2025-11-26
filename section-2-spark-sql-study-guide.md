# Section 2: Using Spark SQL - Study Guide

## Overview
Spark SQL is a Spark module for structured data processing. It provides a programming abstraction called DataFrames and can also act as a distributed SQL query engine.

---

## 1. Reading and Writing Data with Common Data Sources

### JDBC Data Sources

**Reading from JDBC:**
```python
# Read from JDBC database
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://hostname:port/database") \
    .option("dbtable", "schema.tablename") \
    .option("user", "username") \
    .option("password", "password") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# With query instead of table
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://hostname:port/database") \
    .option("query", "SELECT * FROM table WHERE column > 100") \
    .option("user", "username") \
    .option("password", "password") \
    .load()
```

**Writing to JDBC:**
```python
# Write DataFrame to JDBC
df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://hostname:port/database") \
    .option("dbtable", "schema.tablename") \
    .option("user", "username") \
    .option("password", "password") \
    .mode("overwrite") \
    .save()
```

### File-Based Data Sources

**CSV Files:**
```python
# Read CSV
df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("sep", ",") \
    .load("/path/to/file.csv")

# Write CSV
df.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("/path/to/output")
```

**JSON Files:**
```python
# Read JSON
df = spark.read \
    .format("json") \
    .load("/path/to/file.json")

# Write JSON
df.write \
    .format("json") \
    .mode("overwrite") \
    .save("/path/to/output")
```

**Parquet Files:**
```python
# Read Parquet
df = spark.read \
    .format("parquet") \
    .load("/path/to/file.parquet")

# Write Parquet
df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("/path/to/output")
```

**ORC Files:**
```python
# Read ORC
df = spark.read \
    .format("orc") \
    .load("/path/to/file.orc")

# Write ORC
df.write \
    .format("orc") \
    .mode("overwrite") \
    .save("/path/to/output")
```

**Text Files:**
```python
# Read Text
df = spark.read \
    .text("/path/to/file.txt")

# Write Text
df.write \
    .text("/path/to/output")
```

**Delta Files:**
```python
# Read Delta
df = spark.read \
    .format("delta") \
    .load("/path/to/delta-table")

# Write Delta
df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/path/to/delta-table")
```

### Overwriting and Partitioning by Column

**Overwrite Modes:**
```python
# Overwrite entire table
df.write \
    .mode("overwrite") \
    .parquet("/path/to/output")

# Append to existing data
df.write \
    .mode("append") \
    .parquet("/path/to/output")

# Error if data exists (default)
df.write \
    .mode("error") \
    .parquet("/path/to/output")

# Ignore if data exists
df.write \
    .mode("ignore") \
    .parquet("/path/to/output")
```

**Partitioning by Column:**
```python
# Partition by single column
df.write \
    .partitionBy("year") \
    .parquet("/path/to/output")

# Partition by multiple columns
df.write \
    .partitionBy("year", "month", "day") \
    .mode("overwrite") \
    .parquet("/path/to/output")

# Partition with bucketing
df.write \
    .bucketBy(10, "user_id") \
    .sortBy("timestamp") \
    .saveAsTable("bucketed_table")
```

---

## 2. Execute SQL Queries Directly on Files

### Query Files Without Loading to DataFrame First

**CSV Files:**
```python
# Direct SQL query on CSV
result = spark.sql("""
    SELECT * 
    FROM csv.`/path/to/file.csv`
    WHERE column > 100
""")
```

**JSON Files:**
```python
# Direct SQL query on JSON
result = spark.sql("""
    SELECT name, age 
    FROM json.`/path/to/file.json`
    WHERE age > 18
""")
```

**Parquet Files:**
```python
# Direct SQL query on Parquet
result = spark.sql("""
    SELECT * 
    FROM parquet.`/path/to/file.parquet`
    WHERE year = 2024
""")
```

**ORC Files:**
```python
# Direct SQL query on ORC
result = spark.sql("""
    SELECT column1, column2 
    FROM orc.`/path/to/file.orc`
""")
```

**Text Files:**
```python
# Direct SQL query on Text
result = spark.sql("""
    SELECT * 
    FROM text.`/path/to/file.txt`
""")
```

**Delta Files:**
```python
# Direct SQL query on Delta
result = spark.sql("""
    SELECT * 
    FROM delta.`/path/to/delta-table`
    WHERE date >= '2024-01-01'
""")
```

### Save Modes for Outputting Data

| Mode | Behavior | Description |
|------|----------|-------------|
| `append` | Append mode | Adds new data to existing data |
| `overwrite` | Overwrite mode | Overwrites existing data |
| `error` or `errorifexists` | Error mode | Throws exception if data already exists (default) |
| `ignore` | Ignore mode | Ignores the write operation if data already exists |

**Examples:**
```python
# SaveMode usage
from pyspark.sql import SaveMode

df.write.mode(SaveMode.Overwrite).parquet("/path/to/output")
df.write.mode(SaveMode.Append).parquet("/path/to/output")
df.write.mode(SaveMode.ErrorIfExists).parquet("/path/to/output")
df.write.mode(SaveMode.Ignore).parquet("/path/to/output")
```

---

## 3. Save Data to Persistent Tables with Sorting and Partitioning

### Creating Persistent Tables

**Managed Tables:**
```python
# Save as managed table
df.write \
    .mode("overwrite") \
    .saveAsTable("database_name.table_name")

# With format specified
df.write \
    .format("parquet") \
    .mode("overwrite") \
    .saveAsTable("my_table")
```

**External Tables:**
```python
# Save as external table with location
df.write \
    .option("path", "/external/location/table") \
    .mode("overwrite") \
    .saveAsTable("external_table")
```

### Partitioning Tables

**Partition by Columns:**
```python
# Create partitioned table
df.write \
    .partitionBy("year", "month") \
    .mode("overwrite") \
    .saveAsTable("partitioned_table")

# Partition with format
df.write \
    .format("parquet") \
    .partitionBy("country", "state") \
    .mode("overwrite") \
    .saveAsTable("geo_partitioned_table")
```

### Sorting Tables

**Sort Data Before Writing:**
```python
# Sort and save
df.orderBy("timestamp", "user_id") \
    .write \
    .mode("overwrite") \
    .saveAsTable("sorted_table")

# Partition and sort
df.write \
    .partitionBy("date") \
    .sortBy("timestamp") \
    .mode("overwrite") \
    .saveAsTable("partitioned_sorted_table")
```

### Bucketing for Optimization

**Create Bucketed Tables:**
```python
# Bucket by column
df.write \
    .bucketBy(100, "user_id") \
    .sortBy("user_id") \
    .mode("overwrite") \
    .saveAsTable("bucketed_table")

# Partition and bucket
df.write \
    .partitionBy("date") \
    .bucketBy(50, "customer_id") \
    .sortBy("customer_id", "transaction_time") \
    .mode("overwrite") \
    .saveAsTable("optimized_table")
```

### SQL DDL for Table Creation

```sql
-- Create partitioned table
CREATE TABLE sales_data (
    transaction_id INT,
    amount DOUBLE,
    customer_id INT
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET;

-- Create bucketed table
CREATE TABLE user_events (
    event_id BIGINT,
    user_id INT,
    event_type STRING,
    timestamp TIMESTAMP
)
CLUSTERED BY (user_id) INTO 100 BUCKETS
STORED AS PARQUET;
```

---

## 4. Register DataFrames as Temporary Views

### Creating Temporary Views

**Basic Temporary View:**
```python
# Create temporary view
df.createOrReplaceTempView("temp_view_name")

# Query the view
result = spark.sql("SELECT * FROM temp_view_name WHERE column > 100")
```

**Global Temporary View:**
```python
# Create global temporary view (shared across sessions)
df.createOrReplaceGlobalTempView("global_temp_view")

# Query global temp view (must use global_temp database)
result = spark.sql("SELECT * FROM global_temp.global_temp_view")
```

### Differences Between View Types

| View Type | Scope | Lifetime | Access |
|-----------|-------|----------|--------|
| Temporary View | Session-specific | Until session ends | `view_name` |
| Global Temporary View | Cross-session | Until application ends | `global_temp.view_name` |

### Practical Examples

**Example 1: Complex Query Using Temp View**
```python
# Create DataFrame
sales_df = spark.read.parquet("/data/sales")

# Register as temp view
sales_df.createOrReplaceTempView("sales")

# Execute complex SQL query
result = spark.sql("""
    SELECT 
        region,
        SUM(amount) as total_sales,
        AVG(amount) as avg_sales,
        COUNT(*) as num_transactions
    FROM sales
    WHERE date >= '2024-01-01'
    GROUP BY region
    ORDER BY total_sales DESC
""")

result.show()
```

**Example 2: Join Multiple Temp Views**
```python
# Register multiple DataFrames as views
customers_df.createOrReplaceTempView("customers")
orders_df.createOrReplaceTempView("orders")
products_df.createOrReplaceTempView("products")

# Join using SQL
result = spark.sql("""
    SELECT 
        c.customer_name,
        p.product_name,
        o.quantity,
        o.total_price
    FROM orders o
    INNER JOIN customers c ON o.customer_id = c.customer_id
    INNER JOIN products p ON o.product_id = p.product_id
    WHERE o.order_date >= '2024-01-01'
""")
```

**Example 3: Using Global Temp View**
```python
# Create global temp view
df.createOrReplaceGlobalTempView("my_global_view")

# Access from same session
result1 = spark.sql("SELECT * FROM global_temp.my_global_view")

# Can be accessed from different SparkSession (within same application)
spark2 = spark.newSession()
result2 = spark2.sql("SELECT * FROM global_temp.my_global_view")
```

### Checking Existing Views

```python
# List all temporary views
spark.catalog.listTables()

# Check if view exists
spark.catalog.tableExists("view_name")

# Drop temporary view
spark.catalog.dropTempView("view_name")

# Drop global temp view
spark.catalog.dropGlobalTempView("global_view_name")
```

---

## Key Concepts Summary

### 1. Data Source API
- Unified interface for reading/writing data
- Supports JDBC, CSV, JSON, Parquet, ORC, Text, Delta
- Options for customization (headers, schemas, delimiters)

### 2. Write Modes
- **Append**: Add new data to existing data
- **Overwrite**: Replace existing data completely
- **Error**: Fail if data exists (default)
- **Ignore**: Skip write if data exists

### 3. Partitioning Benefits
- Improved query performance (partition pruning)
- Parallel processing of partitions
- Efficient data skipping
- Better organized data layout

### 4. Bucketing Benefits
- Optimizes joins and aggregations
- Reduces shuffle operations
- Pre-sorts data within buckets
- Fixed number of files per partition

### 5. Temporary Views
- **Temp View**: Session-scoped, simple access
- **Global Temp View**: Application-scoped, cross-session access
- SQL queries on DataFrame data
- No physical storage (view of in-memory data)

---

## Best Practices

1. **Partitioning Strategy**
   - Choose partition columns with low-to-medium cardinality
   - Avoid over-partitioning (too many small files)
   - Partition by commonly filtered columns

2. **File Formats**
   - Use Parquet/ORC for analytical workloads (columnar)
   - Use Delta for ACID transactions and time travel
   - Avoid CSV for large-scale production (slow, no schema)

3. **Write Operations**
   - Use `overwrite` mode carefully (data loss risk)
   - Consider `append` for incremental loads
   - Partition large datasets for better performance

4. **Temporary Views**
   - Use for complex SQL operations
   - Clean up views after use (memory)
   - Prefer temp views over global for session isolation

5. **Performance Optimization**
   - Combine partitioning and bucketing for best results
   - Sort data for range queries
   - Use appropriate compression (snappy, gzip, zstd)

---

## Common Exam Topics

- Reading/writing with different data sources and formats
- Understanding save modes and their behaviors
- Partitioning strategies and syntax
- Creating and querying temporary views
- Direct SQL queries on files
- Differences between managed and external tables
- Bucketing vs partitioning
- Global temp views vs regular temp views

---

## Practice Questions

1. **What happens if you write a DataFrame with mode("error") to an existing path?**
   - Answer: An exception is thrown

2. **How do you access a global temporary view?**
   - Answer: Use `global_temp.view_name` syntax

3. **What's the benefit of partitioning by column?**
   - Answer: Partition pruning improves query performance by skipping irrelevant partitions

4. **Which file format is best for analytical queries?**
   - Answer: Parquet or ORC (columnar formats)

5. **What's the difference between bucketBy() and partitionBy()?**
   - Answer: bucketBy creates fixed number of files with hash distribution; partitionBy creates directory structure based on column values

---

## Additional Resources

- [Spark SQL Documentation](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [DataFrameReader API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.html)
- [DataFrameWriter API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.html)
