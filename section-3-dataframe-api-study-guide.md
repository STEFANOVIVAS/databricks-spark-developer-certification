# Section 3: Developing Apache Spark™ DataFrame/DataSet API Applications - Study Guide

## Overview
This section covers comprehensive DataFrame/DataSet API operations for data manipulation, transformation, and processing in Apache Spark.

---

## 1. Manipulating Columns, Rows, and Table Structures

### Adding Columns

```python
from pyspark.sql.functions import col, lit, expr

# Add new column with literal value
df = df.withColumn("new_column", lit(100))

# Add column based on existing column
df = df.withColumn("price_with_tax", col("price") * 1.1)

# Add multiple columns
df = df.withColumn("total", col("quantity") * col("price")) \
       .withColumn("discount", col("total") * 0.1)

# Add column using expression
df = df.withColumn("category", expr("CASE WHEN price > 100 THEN 'expensive' ELSE 'cheap' END"))
```

### Dropping Columns

```python
# Drop single column
df = df.drop("column_name")

# Drop multiple columns
df = df.drop("col1", "col2", "col3")

# Drop columns using list
columns_to_drop = ["col1", "col2"]
df = df.drop(*columns_to_drop)
```

### Renaming Columns

```python
# Rename single column
df = df.withColumnRenamed("old_name", "new_name")

# Rename multiple columns
df = df.withColumnRenamed("col1", "new_col1") \
       .withColumnRenamed("col2", "new_col2")

# Rename all columns at once
new_column_names = ["new_col1", "new_col2", "new_col3"]
df = df.toDF(*new_column_names)
```

### Selecting Columns

```python
# Select specific columns
df.select("col1", "col2")

# Select with expressions
df.select(col("col1"), (col("col2") * 2).alias("doubled_col2"))

# Select all columns plus new one
df.select("*", (col("price") * col("quantity")).alias("total"))
```

### Splitting Columns

```python
from pyspark.sql.functions import split

# Split string column into array
df = df.withColumn("name_parts", split(col("full_name"), " "))

# Split and extract specific parts
df = df.withColumn("first_name", split(col("full_name"), " ").getItem(0)) \
       .withColumn("last_name", split(col("full_name"), " ").getItem(1))
```

### Applying Filters

```python
# Simple filter
df.filter(col("age") > 18)
df.where(col("age") > 18)  # where() is alias for filter()

# Multiple conditions with AND
df.filter((col("age") > 18) & (col("country") == "USA"))

# Multiple conditions with OR
df.filter((col("age") < 18) | (col("age") > 65))

# Filter with NOT
df.filter(~(col("status") == "inactive"))

# Filter using SQL expression
df.filter("age > 18 AND country = 'USA'")

# Filter with IN clause
df.filter(col("country").isin(["USA", "Canada", "Mexico"]))

# Filter NULL values
df.filter(col("email").isNotNull())
df.filter(col("email").isNull())
```

### Exploding Arrays

```python
from pyspark.sql.functions import explode, explode_outer, posexplode

# Explode array column (creates new row for each array element)
df.select("id", explode("items").alias("item"))

# Explode with original rows preserved (includes nulls)
df.select("id", explode_outer("items").alias("item"))

# Explode with position
df.select("id", posexplode("items").alias("pos", "item"))

# Explode map column
from pyspark.sql.functions import explode
df.select("id", explode("map_column").alias("key", "value"))
```

---

## 2. Data Deduplication and Validation

### Deduplication

```python
# Remove exact duplicate rows
df_deduplicated = df.dropDuplicates()
df_deduplicated = df.distinct()  # Same as dropDuplicates()

# Remove duplicates based on specific columns
df_deduplicated = df.dropDuplicates(["customer_id", "date"])

# Remove duplicates keeping first occurrence
from pyspark.sql import Window
from pyspark.sql.functions import row_number

window = Window.partitionBy("customer_id").orderBy("timestamp")
df_deduplicated = df.withColumn("row_num", row_number().over(window)) \
                    .filter(col("row_num") == 1) \
                    .drop("row_num")
```

### Data Validation

```python
# Check for null values
null_counts = df.select([
    sum(col(c).isNull().cast("int")).alias(c) 
    for c in df.columns
])

# Validate data types
df.printSchema()

# Check value ranges
df.filter((col("age") < 0) | (col("age") > 120)).count()

# Validate against business rules
invalid_records = df.filter(
    (col("email").isNull()) | 
    (~col("email").contains("@")) |
    (col("age") < 18)
)

# Count distinct values
df.select("column_name").distinct().count()

# Check for duplicates
duplicate_count = df.count() - df.dropDuplicates().count()
```

---

## 3. Aggregate Operations

### Basic Aggregations

```python
from pyspark.sql.functions import count, sum, avg, min, max, mean, stddev

# Simple aggregations
df.select(
    count("*").alias("total_count"),
    sum("amount").alias("total_amount"),
    avg("amount").alias("avg_amount"),
    min("amount").alias("min_amount"),
    max("amount").alias("max_amount")
).show()

# Group by aggregations
df.groupBy("category").agg(
    count("*").alias("count"),
    sum("sales").alias("total_sales"),
    avg("price").alias("avg_price")
)

# Multiple group by columns
df.groupBy("region", "category").agg(
    sum("revenue").alias("total_revenue"),
    avg("units_sold").alias("avg_units")
)
```

### Count and Count Distinct

```python
from pyspark.sql.functions import countDistinct, approx_count_distinct

# Exact count
total_count = df.count()

# Count distinct (exact)
unique_customers = df.select(countDistinct("customer_id")).collect()[0][0]

# Count distinct with groupBy
df.groupBy("region").agg(countDistinct("customer_id").alias("unique_customers"))

# Approximate count distinct (faster for large datasets)
df.select(approx_count_distinct("customer_id")).show()

# With custom relative standard deviation
df.select(approx_count_distinct("customer_id", 0.05)).show()

# Group by with approximate count distinct
df.groupBy("category").agg(
    approx_count_distinct("product_id").alias("approx_unique_products")
)
```

### Summary Statistics

```python
# Summary statistics for all numeric columns
df.summary().show()

# Describe with percentiles
df.describe().show()

# Custom summary statistics
df.select("price").summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max").show()

# Statistical summary
from pyspark.sql.functions import mean, stddev, variance

df.select(
    mean("price").alias("mean_price"),
    stddev("price").alias("stddev_price"),
    variance("price").alias("variance_price")
).show()
```

### Advanced Aggregations

```python
from pyspark.sql.functions import collect_list, collect_set, first, last

# Collect values into list
df.groupBy("category").agg(collect_list("product_name").alias("products"))

# Collect unique values
df.groupBy("category").agg(collect_set("brand").alias("unique_brands"))

# First and last values
df.groupBy("customer_id").agg(
    first("purchase_date").alias("first_purchase"),
    last("purchase_date").alias("last_purchase")
)
```

---

## 4. Date and Time Operations

### Unix Timestamp Conversions

```python
from pyspark.sql.functions import (
    from_unixtime, unix_timestamp, to_timestamp, to_date, current_timestamp
)

# Unix timestamp to date string
df = df.withColumn("date_string", from_unixtime("unix_time", "yyyy-MM-dd HH:mm:ss"))

# String to unix timestamp
df = df.withColumn("unix_time", unix_timestamp("date_string", "yyyy-MM-dd HH:mm:ss"))

# String to timestamp
df = df.withColumn("timestamp", to_timestamp("date_string", "yyyy-MM-dd HH:mm:ss"))

# String to date
df = df.withColumn("date", to_date("date_string", "yyyy-MM-dd"))

# Current timestamp
df = df.withColumn("current_time", current_timestamp())
```

### Extracting Date Components

```python
from pyspark.sql.functions import (
    year, month, dayofmonth, dayofweek, dayofyear, 
    hour, minute, second, weekofyear, quarter
)

# Extract date components
df = df.withColumn("year", year("date_column")) \
       .withColumn("month", month("date_column")) \
       .withColumn("day", dayofmonth("date_column")) \
       .withColumn("day_of_week", dayofweek("date_column")) \
       .withColumn("day_of_year", dayofyear("date_column")) \
       .withColumn("week_of_year", weekofyear("date_column")) \
       .withColumn("quarter", quarter("date_column"))

# Extract time components
df = df.withColumn("hour", hour("timestamp_column")) \
       .withColumn("minute", minute("timestamp_column")) \
       .withColumn("second", second("timestamp_column"))
```

### Date Arithmetic

```python
from pyspark.sql.functions import date_add, date_sub, datediff, months_between, add_months

# Add days to date
df = df.withColumn("future_date", date_add("date_column", 30))

# Subtract days from date
df = df.withColumn("past_date", date_sub("date_column", 7))

# Difference between dates (in days)
df = df.withColumn("days_diff", datediff("end_date", "start_date"))

# Months between dates
df = df.withColumn("months_diff", months_between("end_date", "start_date"))

# Add months to date
df = df.withColumn("next_month", add_months("date_column", 1))
```

### Date Formatting

```python
from pyspark.sql.functions import date_format

# Format date as string
df = df.withColumn("formatted_date", date_format("date_column", "yyyy-MM-dd"))
df = df.withColumn("formatted_date", date_format("date_column", "MMM dd, yyyy"))
df = df.withColumn("formatted_time", date_format("timestamp_column", "HH:mm:ss"))
```

---

## 5. Combining DataFrames

### Inner Join

```python
# Inner join (returns only matching rows)
result = df1.join(df2, df1.id == df2.id, "inner")

# Simplified syntax when column names match
result = df1.join(df2, "id", "inner")
result = df1.join(df2, "id")  # inner is default
```

### Left Join (Left Outer Join)

```python
# Left join (returns all rows from left, matching from right)
result = df1.join(df2, df1.id == df2.id, "left")
result = df1.join(df2, "id", "left_outer")
```

### Right Join (Right Outer Join)

```python
# Right join (returns all rows from right, matching from left)
result = df1.join(df2, df1.id == df2.id, "right")
result = df1.join(df2, "id", "right_outer")
```

### Full Outer Join

```python
# Full outer join (returns all rows from both)
result = df1.join(df2, df1.id == df2.id, "outer")
result = df1.join(df2, "id", "full_outer")
```

### Left Semi Join

```python
# Left semi join (returns only rows from left that have match in right)
result = df1.join(df2, df1.id == df2.id, "left_semi")
```

### Left Anti Join

```python
# Left anti join (returns only rows from left that have NO match in right)
result = df1.join(df2, df1.id == df2.id, "left_anti")
```

### Cross Join

```python
# Cross join (Cartesian product)
result = df1.crossJoin(df2)

# With explicit join type
result = df1.join(df2, how="cross")
```

### Join on Multiple Keys

```python
# Multiple join conditions
result = df1.join(
    df2, 
    (df1.country == df2.country) & (df1.city == df2.city),
    "inner"
)

# Using list of column names (when names match)
result = df1.join(df2, ["country", "city"], "inner")
```

### Broadcast Join

```python
from pyspark.sql.functions import broadcast

# Broadcast smaller DataFrame to all nodes
result = large_df.join(broadcast(small_df), "id")

# Explicitly specify broadcast join
result = large_df.join(broadcast(small_df), large_df.id == small_df.id, "inner")
```

**When to use Broadcast Join:**
- One DataFrame is small (< 10MB default threshold)
- Reduces shuffle operations
- Improves performance for skewed joins
- Configure threshold: `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")`

### Union Operations

```python
# Union (removes duplicates)
result = df1.union(df2).distinct()

# Union all (keeps duplicates)
result = df1.union(df2)
result = df1.unionAll(df2)  # Deprecated, use union()

# Union by name (matches by column names, not position)
result = df1.unionByName(df2)

# Union by name with missing columns
result = df1.unionByName(df2, allowMissingColumns=True)
```

---

## 6. Input/Output Operations with Schemas

### Reading DataFrames with Schema

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Define schema
schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True),
    StructField("salary", DoubleType(), nullable=True),
    StructField("hire_date", TimestampType(), nullable=True)
])

# Read with schema
df = spark.read.schema(schema).csv("/path/to/file.csv")

# Read with schema inference disabled
df = spark.read.option("inferSchema", "false").schema(schema).json("/path/to/file.json")
```

### DDL String Schema

```python
# Define schema using DDL string
schema_ddl = "id INT, name STRING, age INT, salary DOUBLE, hire_date TIMESTAMP"
df = spark.read.schema(schema_ddl).csv("/path/to/file.csv")
```

### Writing DataFrames

```python
# Write with overwrite mode
df.write.mode("overwrite").parquet("/path/to/output")

# Write with append mode
df.write.mode("append").parquet("/path/to/output")

# Write with specific format
df.write.format("parquet").mode("overwrite").save("/path/to/output")

# Write with partitioning
df.write.partitionBy("year", "month").mode("overwrite").parquet("/path/to/output")

# Write to table
df.write.mode("overwrite").saveAsTable("database.table_name")
```

### Schema Operations

```python
# Print schema
df.printSchema()

# Get schema as StructType
schema = df.schema

# Get column names
columns = df.columns

# Get data types
df.dtypes
```

---

## 7. DataFrame Operations

### Sorting

```python
from pyspark.sql.functions import asc, desc, asc_nulls_first, asc_nulls_last

# Sort ascending (default)
df.orderBy("age")
df.sort("age")

# Sort descending
df.orderBy(desc("age"))
df.orderBy(col("age").desc())

# Sort by multiple columns
df.orderBy("country", "age")
df.orderBy(asc("country"), desc("age"))

# Null handling in sorting
df.orderBy(asc_nulls_first("age"))
df.orderBy(asc_nulls_last("age"))
```

### Iterating Over DataFrames

```python
# Collect and iterate (use with caution on large datasets)
rows = df.collect()
for row in rows:
    print(row.column_name)
    print(row["column_name"])
    print(row[0])  # By index

# Iterate with take
for row in df.take(10):
    print(row)

# Iterate with foreach (action on executors)
def process_row(row):
    # Process row logic
    pass

df.foreach(process_row)

# Iterate with foreachPartition (more efficient)
def process_partition(partition):
    for row in partition:
        # Process row
        pass

df.foreachPartition(process_partition)

# Convert to Pandas for iteration (small datasets only)
pandas_df = df.toPandas()
for index, row in pandas_df.iterrows():
    print(row['column_name'])
```

### Printing Schema

```python
# Print schema tree
df.printSchema()

# Get schema programmatically
schema = df.schema
print(schema)

# Get specific field
field = schema["column_name"]
print(field.dataType)
print(field.nullable)
```

### Showing Data

```python
# Show first 20 rows (default)
df.show()

# Show specific number of rows
df.show(10)

# Show without truncation
df.show(truncate=False)

# Show with custom truncation
df.show(truncate=50)

# Show vertical format
df.show(n=5, vertical=True)
```

### DataFrame to List/Sequence Conversion

```python
# Convert to list of Rows
rows_list = df.collect()

# Convert single column to list
values_list = df.select("column_name").rdd.flatMap(lambda x: x).collect()

# Convert to list of dictionaries
dict_list = [row.asDict() for row in df.collect()]

# Convert to Pandas DataFrame
pandas_df = df.toPandas()

# Convert from list to DataFrame
data = [("John", 30), ("Jane", 25)]
df = spark.createDataFrame(data, ["name", "age"])

# Convert from list of dictionaries
data = [{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]
df = spark.createDataFrame(data)
```

---

## 8. User-Defined Functions (UDFs)

### Basic UDFs

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType

# Define Python function
def upper_case(text):
    return text.upper() if text else None

# Register as UDF
upper_udf = udf(upper_case, StringType())

# Use UDF in DataFrame
df = df.withColumn("upper_name", upper_udf(col("name")))

# UDF with decorator
@udf(returnType=StringType())
def lower_case(text):
    return text.lower() if text else None

df = df.withColumn("lower_name", lower_case(col("name")))
```

### UDFs with Multiple Parameters

```python
def calculate_discount(price, discount_rate):
    return price * (1 - discount_rate)

discount_udf = udf(calculate_discount, DoubleType())

df = df.withColumn("final_price", discount_udf(col("price"), col("discount_rate")))
```

### Registering UDFs for SQL

```python
# Register UDF for SQL use
spark.udf.register("upper_case_sql", upper_case, StringType())

# Use in SQL
spark.sql("SELECT upper_case_sql(name) as upper_name FROM table")
```

### Pandas UDFs (Vectorized UDFs)

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

# Pandas UDF (Series to Series)
@pandas_udf(DoubleType())
def pandas_multiply(series: pd.Series) -> pd.Series:
    return series * 2

df = df.withColumn("doubled", pandas_multiply(col("value")))

# Pandas UDF with multiple inputs
@pandas_udf(DoubleType())
def pandas_add(s1: pd.Series, s2: pd.Series) -> pd.Series:
    return s1 + s2

df = df.withColumn("sum", pandas_add(col("col1"), col("col2")))
```

### Stateful Operations with StateStores

```python
from pyspark.sql.streaming import GroupState, GroupStateTimeout

# Define stateful function
def update_state(key, values, state):
    # Access existing state
    if state.exists:
        old_count = state.get
    else:
        old_count = 0
    
    # Update state
    new_count = old_count + len(values)
    state.update(new_count)
    
    return (key, new_count)

# Apply stateful operation (in streaming context)
result = df.groupByKey(lambda x: x.key) \
           .mapGroupsWithState(update_state, GroupStateTimeout.NoTimeout())
```

---

## 9. Broadcast Variables and Accumulators

### Broadcast Variables

```python
# Create broadcast variable
lookup_dict = {"A": "Category A", "B": "Category B", "C": "Category C"}
broadcast_dict = spark.sparkContext.broadcast(lookup_dict)

# Use in UDF
def map_category(code):
    mapping = broadcast_dict.value
    return mapping.get(code, "Unknown")

map_udf = udf(map_category, StringType())
df = df.withColumn("category_name", map_udf(col("category_code")))

# Use in transformation
def enrich_partition(partition):
    mapping = broadcast_dict.value
    for row in partition:
        # Use mapping
        yield row

df.rdd.mapPartitions(enrich_partition)
```

**Benefits of Broadcast Variables:**
- Read-only variables cached on each executor
- Efficient for small lookup tables (< 10MB)
- Reduces network overhead
- Shared across tasks on same executor

### Accumulators

```python
# Create accumulator
error_count = spark.sparkContext.accumulator(0)
record_count = spark.sparkContext.longAccumulator("RecordCount")

# Use in transformations
def process_row(row):
    global error_count, record_count
    try:
        record_count.add(1)
        # Process row
        return row
    except Exception:
        error_count.add(1)
        return None

df.rdd.map(process_row)

# Access accumulator value (only on driver)
print(f"Total records: {record_count.value}")
print(f"Errors: {error_count.value}")

# Custom accumulator
from pyspark import AccumulatorParam

class ListAccumulatorParam(AccumulatorParam):
    def zero(self, value):
        return []
    
    def addInPlace(self, acc1, acc2):
        acc1.extend(acc2)
        return acc1

list_accumulator = spark.sparkContext.accumulator([], ListAccumulatorParam())
```

**Accumulator Characteristics:**
- Write-only variables from executors
- Read from driver after action
- Used for counters and sums
- Only updated once per task (even if task retries)

---

## 10. Broadcast Joins - Purpose and Implementation

### What is a Broadcast Join?

A broadcast join sends a copy of the smaller DataFrame to all executor nodes, avoiding expensive shuffle operations.

### When to Use Broadcast Joins

1. **Small DataFrame Size**: One DataFrame is small enough to fit in memory (default < 10MB)
2. **Large DataFrame Join**: Joining large DataFrame with small lookup table
3. **Avoid Shuffle**: Eliminate shuffle for the large DataFrame
4. **Skewed Data**: Handle data skew issues

### Implementation Methods

**Method 1: Using broadcast() function**
```python
from pyspark.sql.functions import broadcast

# Explicit broadcast
result = large_df.join(broadcast(small_df), "id")
```

**Method 2: Auto-broadcast (based on threshold)**
```python
# Check current threshold
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

# Set threshold (in bytes, -1 to disable)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "104857600")  # 100MB

# Spark auto-broadcasts if size < threshold
result = large_df.join(small_df, "id")
```

**Method 3: Broadcast hint in SQL**
```sql
SELECT /*+ BROADCAST(small_table) */ *
FROM large_table
JOIN small_table ON large_table.id = small_table.id
```

### Performance Comparison

```python
# Regular join (with shuffle)
start = time.time()
result_regular = large_df.join(small_df, "id")
result_regular.count()
regular_time = time.time() - start

# Broadcast join (no shuffle)
start = time.time()
result_broadcast = large_df.join(broadcast(small_df), "id")
result_broadcast.count()
broadcast_time = time.time() - start

print(f"Regular join: {regular_time}s")
print(f"Broadcast join: {broadcast_time}s")
```

### Broadcast Join Best Practices

1. **Size Limit**: Keep broadcast DataFrame under 2GB (executor memory limit)
2. **Memory Check**: Ensure executors have enough memory
3. **Cardinality**: Works best with low-to-medium cardinality joins
4. **Multiple Joins**: Can broadcast multiple small tables
5. **Monitoring**: Check Spark UI for broadcast stages

### Example Use Cases

```python
# Use Case 1: Dimension table lookup
fact_df = spark.read.parquet("/data/sales")  # Large fact table
dim_df = spark.read.parquet("/data/products")  # Small dimension table

enriched_df = fact_df.join(broadcast(dim_df), "product_id")

# Use Case 2: Multiple dimension joins
result = fact_df \
    .join(broadcast(products_df), "product_id") \
    .join(broadcast(customers_df), "customer_id") \
    .join(broadcast(regions_df), "region_id")

# Use Case 3: Filter before broadcast
small_df = large_lookup_df.filter(col("active") == True)
result = main_df.join(broadcast(small_df), "id")
```

---

## Key Concepts Summary

### DataFrame Transformations
- **Narrow transformations**: map, filter, select (no shuffle)
- **Wide transformations**: groupBy, join, distinct (shuffle required)

### Performance Optimization
- Use broadcast joins for small tables
- Partition data appropriately
- Cache intermediate results
- Use approximate functions for large datasets
- Prefer built-in functions over UDFs

### Data Quality
- Deduplication strategies
- Null handling
- Data validation
- Schema enforcement

### Join Strategies
- Choose appropriate join type
- Consider data size and distribution
- Use broadcast for small tables
- Monitor shuffle operations

---

## Best Practices

1. **Column Operations**
   - Use built-in functions over UDFs
   - Chain operations efficiently
   - Avoid excessive withColumn calls

2. **Filtering**
   - Filter early in pipeline
   - Push down filters when possible
   - Use partition pruning

3. **Aggregations**
   - Use approx_count_distinct for large datasets
   - Consider pre-aggregation
   - Group by low-cardinality columns

4. **Joins**
   - Broadcast small tables
   - Join on indexed columns
   - Avoid cross joins
   - Use appropriate join type

5. **UDFs**
   - Prefer built-in functions
   - Use Pandas UDFs for better performance
   - Minimize state in stateful operations
   - Register once, use many times

6. **Variables**
   - Use broadcast for read-only shared data
   - Use accumulators for counters only
   - Don't use accumulators for critical logic

---

## Common Exam Topics

- Column manipulation operations
- Filter and where clause syntax
- Explode arrays and maps
- Deduplication strategies
- Aggregate functions (count, countDistinct, approx_count_distinct)
- Date/time functions and conversions
- Join types and syntax
- Broadcast join purpose and implementation
- Union vs unionAll
- Schema definition and enforcement
- UDF creation and registration
- Broadcast variables vs accumulators
- DataFrame to list/sequence conversions

---

## Practice Questions

1. **What's the difference between union() and unionAll()?**
   - unionAll() is deprecated; both keep duplicates now. Use distinct() to remove duplicates.

2. **When should you use approx_count_distinct() instead of countDistinct()?**
   - For large datasets where approximate result (2-3% error) is acceptable and performance is critical.

3. **What's the benefit of broadcast join?**
   - Eliminates shuffle for large DataFrame, improves performance by sending small table to all executors.

4. **How do accumulators differ from broadcast variables?**
   - Accumulators: write-only from executors, read from driver. Broadcast: read-only, shared across executors.

5. **What does explode() do?**
   - Creates new row for each element in array/map column.

---

## Additional Resources

- [DataFrame API Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
- [Functions API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [Performance Tuning Guide](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
