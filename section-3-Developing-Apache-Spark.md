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


#### `explode` with Arrays

```python
from pyspark.sql.functions import explode

# Sample data with array column
data = [
    (1, "Alice", ["Python", "Scala", "SQL"]),
    (2, "Bob", ["Java", "Kotlin"]),
    (3, "Charlie", ["R"])
]

df = spark.createDataFrame(data, ["id", "name", "languages"])

# Using explode - creates a new row for each element in the array
df.select("id", "name", explode("languages").alias("language")).show()

# Output:
# +---+-------+--------+
# | id|   name|language|
# +---+-------+--------+
# |  1|  Alice|  Python|
# |  1|  Alice|   Scala|
# |  1|  Alice|     SQL|
# |  2|    Bob|    Java|
# |  2|    Bob|  Kotlin|
# |  3|Charlie|       R|
# +---+-------+--------+
```

#### `explode` with Maps

```python
from pyspark.sql.functions import explode

# Sample data with map column
data = [
    (1, "Alice", {"math": 90, "english": 85}),
    (2, "Bob", {"math": 78, "science": 92, "history": 88})
]

df = spark.createDataFrame(data, ["id", "name", "scores"])

# Explode map - creates key and value columns
df.select("id", "name", explode("scores").alias("subject", "score")).show()

# Output:
# +---+-----+-------+-----+
# | id| name|subject|score|
# +---+-----+-------+-----+
# |  1|Alice|   math|   90|
# |  1|Alice|english|   85|
# |  2|  Bob|   math|   78|
# |  2|  Bob|science|   92|
# |  2|  Bob|history|   88|
# +---+-----+-------+-----+
```

#### `explode` Behavior with Null/Empty Arrays

**Important:** `explode` drops rows with null or empty arrays:

```python
data = [
    (1, "Alice", ["Python", "Scala"]),
    (2, "Bob", ["Java"]),
    (3, "Charlie", None),    # null - row will be dropped!
    (4, "Diana", [])         # empty - row will be dropped!
]

df = spark.createDataFrame(data, ["id", "name", "languages"])

df.select("id", "name", explode("languages").alias("language")).show()

# Output:
# +---+-----+--------+
# | id| name|language|
# +---+-----+--------+
# |  1|Alice|  Python|
# |  1|Alice|   Scala|
# |  2|  Bob|    Java|
# +---+-----+--------+
# Note: Charlie (id=3) and Diana (id=4) are NOT in the output!
```

#### `explode` with Nested Arrays

```python
from pyspark.sql.functions import explode, col

# Nested structure
data = [
    (1, "Order1", [{"product": "Laptop", "qty": 1}, {"product": "Mouse", "qty": 2}]),
    (2, "Order2", [{"product": "Keyboard", "qty": 1}])
]

schema = "id INT, order_name STRING, items ARRAY<STRUCT<product: STRING, qty: INT>>"
df = spark.createDataFrame(data, schema)

# Explode and access nested fields
df.select("id", "order_name", explode("items").alias("item")) \
  .select("id", "order_name", col("item.product"), col("item.qty")).show()

# Output:
# +---+----------+--------+---+
# | id|order_name| product|qty|
# +---+----------+--------+---+
# |  1|    Order1|  Laptop|  1|
# |  1|    Order1|   Mouse|  2|
# |  2|    Order2|Keyboard|  1|
# +---+----------+--------+---+
```

#### `explode_outer` Example

Unlike `explode`, `explode_outer` keeps rows with `null` or empty arrays/maps instead of dropping them:

```python
from pyspark.sql.functions import explode_outer

# Sample data with null and empty arrays
data = [
    (1, "Alice", ["Python", "Scala"]),
    (2, "Bob", ["Java"]),
    (3, "Charlie", None),          # null array
    (4, "Diana", [])               # empty array
]

df = spark.createDataFrame(data, ["id", "name", "languages"])

# Using explode_outer - preserves rows with null/empty arrays
df.select("id", "name", explode_outer("languages").alias("language")).show()

# Output:
# +---+-------+--------+
# | id|   name|language|
# +---+-------+--------+
# |  1|  Alice|  Python|
# |  1|  Alice|   Scala|
# |  2|    Bob|    Java|
# |  3|Charlie|    null|  <-- preserved with null value
# |  4|  Diana|    null|  <-- preserved with null value
# +---+-------+--------+
```

#### `posexplode` Example

`posexplode` works like `explode` but also returns the position (index) of each element:

```python
from pyspark.sql.functions import posexplode

data = [
    (1, "Alice", ["Python", "Scala", "SQL"]),
    (2, "Bob", ["Java", "Kotlin"])
]

df = spark.createDataFrame(data, ["id", "name", "languages"])

# Using posexplode - returns position and value
df.select("id", "name", posexplode("languages").alias("pos", "language")).show()

# Output:
# +---+-----+---+--------+
# | id| name|pos|language|
# +---+-----+---+--------+
# |  1|Alice|  0|  Python|
# |  1|Alice|  1|   Scala|
# |  1|Alice|  2|     SQL|
# |  2|  Bob|  0|    Java|
# |  2|  Bob|  1|  Kotlin|
# +---+-----+---+--------+
```

#### `posexplode_outer` Example

Combines both behaviors - returns position and preserves null/empty arrays:

```python
from pyspark.sql.functions import posexplode_outer

data = [
    (1, "Alice", ["Python", "Scala"]),
    (2, "Bob", None),
    (3, "Charlie", [])
]

df = spark.createDataFrame(data, ["id", "name", "languages"])

df.select("id", "name", posexplode_outer("languages").alias("pos", "language")).show()

# Output:
# +---+-------+----+--------+
# | id|   name| pos|language|
# +---+-------+----+--------+
# |  1|  Alice|   0|  Python|
# |  1|  Alice|   1|   Scala|
# |  2|    Bob|null|    null|  <-- preserved
# |  3|Charlie|null|    null|  <-- preserved
# +---+-------+----+--------+
```

#### Key Differences Summary

| Function | Returns Position | Keeps Null/Empty |
|----------|-----------------|------------------|
| `explode` | ❌ | ❌ |
| `explode_outer` | ❌ | ✅ |
| `posexplode` | ✅ | ❌ |
| `posexplode_outer` | ✅ | ✅ |

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
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, avg, min, max, round

spark = SparkSession.builder.appName("AggregationExample").getOrCreate()

# Sample e-commerce sales data
sales_data = [
    ("Electronics", "Laptop", 1200.00, 5, "North"),
    ("Electronics", "Phone", 800.00, 12, "North"),
    ("Electronics", "Tablet", 450.00, 8, "South"),
    ("Clothing", "Shirt", 45.00, 50, "North"),
    ("Clothing", "Pants", 65.00, 30, "South"),
    ("Clothing", "Jacket", 120.00, 15, "East"),
    ("Home", "Lamp", 35.00, 25, "South"),
    ("Home", "Chair", 150.00, 10, "West"),
    ("Home", "Table", 300.00, 5, "North")
]

df = spark.createDataFrame(sales_data, ["category", "product", "price", "quantity", "region"])

# Simple aggregations - overall sales metrics
df.select(
    count("*").alias("total_products"),
    round(sum(df.price * df.quantity), 2).alias("total_revenue"),
    round(avg("price"), 2).alias("avg_price"),
    min("price").alias("min_price"),
    max("price").alias("max_price")
).show()

# Output:
# +--------------+-------------+---------+---------+---------+
# |total_products|total_revenue|avg_price|min_price|max_price|
# +--------------+-------------+---------+---------+---------+
# |             9|      26875.0|   351.67|     35.0|   1200.0|
# +--------------+-------------+---------+---------+---------+

# Group by category - sales by category
df.groupBy("category").agg(
    count("*").alias("product_count"),
    round(sum(df.price * df.quantity), 2).alias("total_revenue"),
    round(avg("price"), 2).alias("avg_price")
).orderBy("total_revenue", ascending=False).show()

# Output:
# +-----------+-------------+-------------+---------+
# |   category|product_count|total_revenue|avg_price|
# +-----------+-------------+-------------+---------+
# |Electronics|            3|      19200.0|   816.67|
# |   Clothing|            3|       6075.0|    76.67|
# |       Home|            3|       1600.0|   161.67|
# +-----------+-------------+-------------+---------+

# Multiple group by - sales by region and category
df.groupBy("region", "category").agg(
    count("*").alias("products"),
    round(sum(df.price * df.quantity), 2).alias("revenue")
).orderBy("region", "category").show()

# Output:
# +------+-----------+--------+--------+
# |region|   category|products| revenue|
# +------+-----------+--------+--------+
# |  East|   Clothing|       1|  1800.0|
# | North|   Clothing|       1|  2250.0|
# | North|Electronics|       2| 15600.0|
# | North|       Home|       1|  1500.0|
# | South|   Clothing|       1|  1950.0|
# | South|Electronics|       1|  3600.0|
# | South|       Home|       1|   875.0|
# |  West|       Home|       1|  1500.0|
# +------+-----------+--------+--------+
```

### Count and Count Distinct


```python
from pyspark.sql.functions import countDistinct, approx_count_distinct, count

# Sample customer purchase data
purchase_data = [
    ("C001", "Electronics", "P001", "2024-01-15"),
    ("C001", "Electronics", "P002", "2024-01-20"),
    ("C002", "Clothing", "P003", "2024-01-18"),
    ("C003", "Electronics", "P001", "2024-01-22"),  # Same product as C001
    ("C003", "Home", "P004", "2024-01-25"),
    ("C004", "Clothing", "P003", "2024-02-01"),     # Same product as C002
    ("C004", "Clothing", "P005", "2024-02-05"),
    ("C005", "Electronics", "P002", "2024-02-10"),
    ("C005", "Electronics", "P006", "2024-02-15"),
    ("C006", "Home", "P007", "2024-02-20")
]

purchases_df = spark.createDataFrame(purchase_data, ["customer_id", "category", "product_id", "purchase_date"])

# Total count of purchases
total_purchases = purchases_df.count()
print(f"Total purchases: {total_purchases}")  # Output: 10

# Count distinct customers (exact)
unique_customers = purchases_df.select(countDistinct("customer_id")).collect()[0][0]
print(f"Unique customers: {unique_customers}")  # Output: 6

# Count distinct customers per category
purchases_df.groupBy("category").agg(
    count("*").alias("total_purchases"),
    countDistinct("customer_id").alias("unique_customers"),
    countDistinct("product_id").alias("unique_products")
).show()

# Output:
# +-----------+---------------+----------------+---------------+
# |   category|total_purchases|unique_customers|unique_products|
# +-----------+---------------+----------------+---------------+
# |Electronics|              5|               4|              3|
# |   Clothing|              3|               2|              2|
# |       Home|              2|               2|              2|
# +-----------+---------------+----------------+---------------+

# Approximate count distinct (useful for large datasets - faster but ~2-3% error)
purchases_df.select(
    countDistinct("customer_id").alias("exact_count"),
    approx_count_distinct("customer_id").alias("approx_count"),
    approx_count_distinct("customer_id", 0.01).alias("approx_count_precise")  # 1% error
).show()

# Output:
# +-----------+------------+--------------------+
# |exact_count|approx_count|approx_count_precise|
# +-----------+------------+--------------------+
# |          6|           6|                   6|
# +-----------+------------+--------------------+

# Group by with approximate count - for big data scenarios
purchases_df.groupBy("category").agg(
    approx_count_distinct("product_id").alias("approx_unique_products"),
    approx_count_distinct("customer_id").alias("approx_unique_customers")
).show()

# Output:
# +-----------+----------------------+-----------------------+
# |   category|approx_unique_products|approx_unique_customers|
# +-----------+----------------------+-----------------------+
# |Electronics|                     3|                      4|
# |   Clothing|                     2|                      2|
# |       Home|                     2|                      2|
# +-----------+----------------------+-----------------------+
```

### Summary Statistics


```python
from pyspark.sql.functions import mean, stddev, variance, round, percentile_approx

# Sample employee salary data
salary_data = [
    ("Engineering", "Alice", 85000, 5),
    ("Engineering", "Bob", 92000, 7),
    ("Engineering", "Charlie", 78000, 3),
    ("Engineering", "Diana", 120000, 12),
    ("Marketing", "Eve", 65000, 4),
    ("Marketing", "Frank", 72000, 6),
    ("Marketing", "Grace", 58000, 2),
    ("Sales", "Henry", 55000, 3),
    ("Sales", "Ivy", 68000, 5),
    ("Sales", "Jack", 95000, 10),
    ("HR", "Kate", 52000, 4),
    ("HR", "Leo", 48000, 2)
]

salary_df = spark.createDataFrame(salary_data, ["department", "name", "salary", "years_experience"])

# Summary statistics for all numeric columns
salary_df.summary().show()

# Output:
# +-------+------------------+------------------+
# |summary|            salary|  years_experience|
# +-------+------------------+------------------+
# |  count|                12|                12|
# |   mean|           74000.0| 5.25             |
# | stddev|20511.169827...   |3.10812...        |
# |    min|             48000|                 2|
# |    25%|             56500|                 3|
# |    50%|             70000|               4.5|
# |    75%|             90250|                 7|
# |    max|            120000|                12|
# +-------+------------------+------------------+

# Describe - basic statistics
salary_df.describe("salary", "years_experience").show()

# Output:
# +-------+------------------+------------------+
# |summary|            salary|  years_experience|
# +-------+------------------+------------------+
# |  count|                12|                12|
# |   mean|           74000.0|              5.25|
# | stddev|20511.169827619...| 3.108126775732...|
# |    min|             48000|                 2|
# |    max|            120000|                12|
# +-------+------------------+------------------+

# Custom summary statistics for salary
salary_df.select("salary").summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max").show()

# Output:
# +-------+------------------+
# |summary|            salary|
# +-------+------------------+
# |  count|                12|
# |   mean|           74000.0|
# | stddev|20511.169827619...|
# |    min|             48000|
# |    25%|             56500|
# |    50%|             70000|
# |    75%|             90250|
# |    max|            120000|
# +-------+------------------+

# Statistical summary with explicit functions
salary_df.select(
    round(mean("salary"), 2).alias("mean_salary"),
    round(stddev("salary"), 2).alias("stddev_salary"),
    round(variance("salary"), 2).alias("variance_salary")
).show()

# Output:
# +-----------+-------------+---------------+
# |mean_salary|stddev_salary|variance_salary|
# +-----------+-------------+---------------+
# |   74000.00|     20511.17|  420707878.79 |
# +-----------+-------------+---------------+

# Statistics by department
salary_df.groupBy("department").agg(
    count("*").alias("employee_count"),
    round(mean("salary"), 2).alias("avg_salary"),
    round(stddev("salary"), 2).alias("stddev_salary"),
    min("salary").alias("min_salary"),
    max("salary").alias("max_salary"),
    round(mean("years_experience"), 1).alias("avg_experience")
).orderBy("avg_salary", ascending=False).show()

# Output:
# +-----------+--------------+----------+-------------+----------+----------+--------------+
# | department|employee_count|avg_salary|stddev_salary|min_salary|max_salary|avg_experience|
# +-----------+--------------+----------+-------------+----------+----------+--------------+
# |Engineering|             4|  93750.00|     18500.00|     78000|    120000|           6.8|
# | Sales     |             3|  72666.67|     20502.85|     55000|     95000|           6.0|
# | Marketing |             3|  65000.00|      7000.00|     58000|     72000|           4.0|
# | HR        |             2|  50000.00|      2828.43|     48000|     52000|           3.0|
# +-----------+--------------+----------+-------------+----------+----------+--------------+

# Percentile calculation
salary_df.select(
    percentile_approx("salary", 0.25).alias("p25"),
    percentile_approx("salary", 0.50).alias("median"),
    percentile_approx("salary", 0.75).alias("p75"),
    percentile_approx("salary", 0.90).alias("p90")
).show()

# Output:
# +-----+------+-----+------+
# |  p25|median|  p75|   p90|
# +-----+------+-----+------+
# |56500| 70000|90250| 95000|
# +-----+------+-----+------+
```

### Advanced Aggregations


```python
from pyspark.sql.functions import collect_list, collect_set, first, last, size, array_distinct, sort_array

# Sample order data
order_data = [
    ("C001", "2024-01-10", "Laptop", "Electronics", 1200),
    ("C001", "2024-01-15", "Mouse", "Electronics", 25),
    ("C001", "2024-02-20", "Keyboard", "Electronics", 75),
    ("C002", "2024-01-12", "Shirt", "Clothing", 45),
    ("C002", "2024-01-12", "Pants", "Clothing", 65),
    ("C002", "2024-03-01", "Jacket", "Clothing", 120),
    ("C003", "2024-02-05", "Lamp", "Home", 35),
    ("C003", "2024-02-05", "Chair", "Home", 150),
    ("C003", "2024-02-28", "Lamp", "Home", 35),  # Same product again
    ("C004", "2024-01-20", "Phone", "Electronics", 800),
    ("C004", "2024-03-15", "Tablet", "Electronics", 450)
]

orders_df = spark.createDataFrame(order_data, ["customer_id", "order_date", "product", "category", "price"])

# Collect all products purchased by each customer (keeps duplicates)
orders_df.groupBy("customer_id").agg(
    collect_list("product").alias("all_products")
).show(truncate=False)

# Output:
# +-----------+--------------------------------+
# |customer_id|all_products                    |
# +-----------+--------------------------------+
# |C001       |[Laptop, Mouse, Keyboard]       |
# |C002       |[Shirt, Pants, Jacket]          |
# |C003       |[Lamp, Chair, Lamp]             |  <-- Lamp appears twice
# |C004       |[Phone, Tablet]                 |
# +-----------+--------------------------------+

# Collect unique products per customer (removes duplicates)
orders_df.groupBy("customer_id").agg(
    collect_set("product").alias("unique_products"),
    collect_set("category").alias("categories_shopped")
).show(truncate=False)

# Output:
# +-----------+-------------------------+------------------+
# |customer_id|unique_products          |categories_shopped|
# +-----------+-------------------------+------------------+
# |C001       |[Keyboard, Mouse, Laptop]|[Electronics]     |
# |C002       |[Shirt, Jacket, Pants]   |[Clothing]        |
# |C003       |[Chair, Lamp]            |[Home]            |  <-- Lamp only once
# |C004       |[Tablet, Phone]          |[Electronics]     |
# +-----------+-------------------------+------------------+

# First and last purchase for each customer
orders_df.groupBy("customer_id").agg(
    first("order_date").alias("first_order_date"),
    first("product").alias("first_product"),
    last("order_date").alias("last_order_date"),
    last("product").alias("last_product")
).show()

# Output:
# +-----------+----------------+-------------+---------------+------------+
# |customer_id|first_order_date|first_product|last_order_date|last_product|
# +-----------+----------------+-------------+---------------+------------+
# |       C001|      2024-01-10|       Laptop|     2024-02-20|    Keyboard|
# |       C002|      2024-01-12|        Shirt|     2024-03-01|      Jacket|
# |       C003|      2024-02-05|         Lamp|     2024-02-28|        Lamp|
# |       C004|      2024-01-20|        Phone|     2024-03-15|      Tablet|
# +-----------+----------------+-------------+---------------+------------+

# Comprehensive customer summary
orders_df.groupBy("customer_id").agg(
    count("*").alias("total_orders"),
    round(sum("price"), 2).alias("total_spent"),
    size(collect_set("product")).alias("unique_products_count"),
    sort_array(collect_set("category")).alias("categories"),
    first("order_date").alias("first_purchase"),
    last("order_date").alias("last_purchase")
).show(truncate=False)

# Output:
# +-----------+------------+-----------+---------------------+-------------+--------------+-------------+
# |customer_id|total_orders|total_spent|unique_products_count|categories   |first_purchase|last_purchase|
# +-----------+------------+-----------+---------------------+-------------+--------------+-------------+
# |C001       |3           |1300.0     |3                    |[Electronics]|2024-01-10    |2024-02-20   |
# |C002       |3           |230.0      |3                    |[Clothing]   |2024-01-12    |2024-03-01   |
# |C003       |3           |220.0      |2                    |[Home]       |2024-02-05    |2024-02-28   |
# |C004       |2           |1250.0     |2                    |[Electronics]|2024-01-20    |2024-03-15   |
# +-----------+------------+-----------+---------------------+-------------+--------------+-------------+

# Products summary by category
orders_df.groupBy("category").agg(
    sort_array(collect_set("product")).alias("products_in_category"),
    size(collect_set("product")).alias("unique_product_count"),
    size(collect_set("customer_id")).alias("customers_count"),
    round(avg("price"), 2).alias("avg_price")
).show(truncate=False)

# Output:
# +-----------+-----------------------------------------+--------------------+---------------+---------+
# |category   |products_in_category                     |unique_product_count|customers_count|avg_price|
# +-----------+-----------------------------------------+--------------------+---------------+---------+
# |Electronics|[Keyboard, Laptop, Mouse, Phone, Tablet] |  5                 |2              |510.0    |
# |Clothing   |[Jacket, Pants, Shirt]                   |  3                 |1              |76.67    |
# |Home       |[Chair, Lamp]                            |  2                 |1              |73.33    |
# +-----------+-----------------------------------------+--------------------+---------------+---------+
```

---

## 4. Date and Time Operations

### Unix Timestamp Conversions


```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_unixtime, unix_timestamp, to_timestamp, to_date, 
    current_timestamp, current_date, col
)

spark = SparkSession.builder.appName("DateTimeExample").getOrCreate()

# Sample data with different date/time formats
event_data = [
    (1, 1703001600, "2024-01-15 14:30:00", "15/01/2024"),
    (2, 1703088000, "2024-02-20 09:15:30", "20/02/2024"),
    (3, 1703174400, "2024-03-10 18:45:00", "10/03/2024"),
    (4, 1703260800, "2024-04-05 11:00:00", "05/04/2024")
]

df = spark.createDataFrame(event_data, ["event_id", "unix_time", "datetime_str", "date_str_eu"])

# Unix timestamp to date string
df_converted = df.withColumn(
    "from_unix", 
    from_unixtime("unix_time", "yyyy-MM-dd HH:mm:ss")
)

df_converted.select("event_id", "unix_time", "from_unix").show(truncate=False)

# Output:
# +--------+----------+-------------------+
# |event_id|unix_time |from_unix          |
# +--------+----------+-------------------+
# |1       |1703001600|2023-12-19 16:00:00|
# |2       |1703088000|2023-12-20 16:00:00|
# |3       |1703174400|2023-12-21 16:00:00|
# |4       |1703260800|2023-12-22 16:00:00|
# +--------+----------+-------------------+

# String to unix timestamp
df_converted = df.withColumn(
    "to_unix", 
    unix_timestamp("datetime_str", "yyyy-MM-dd HH:mm:ss")
)

df_converted.select("event_id", "datetime_str", "to_unix").show(truncate=False)

# Output:
# +--------+-------------------+----------+
# |event_id|datetime_str       |to_unix   |
# +--------+-------------------+----------+
# |1       |2024-01-15 14:30:00|1705325400|
# |2       |2024-02-20 09:15:30|1708420530|
# |3       |2024-03-10 18:45:00|1710092700|
# |4       |2024-04-05 11:00:00|1712318400|
# +--------+-------------------+----------+

# String to timestamp (proper timestamp type)
df_converted = df.withColumn(
    "timestamp_col", 
    to_timestamp("datetime_str", "yyyy-MM-dd HH:mm:ss")
)

df_converted.select("event_id", "datetime_str", "timestamp_col").show(truncate=False)

# Output:
# +--------+-------------------+-------------------+
# |event_id|datetime_str       |timestamp_col      |
# +--------+-------------------+-------------------+
# |1       |2024-01-15 14:30:00|2024-01-15 14:30:00|
# |2       |2024-02-20 09:15:30|2024-02-20 09:15:30|
# |3       |2024-03-10 18:45:00|2024-03-10 18:45:00|
# |4       |2024-04-05 11:00:00|2024-04-05 11:00:00|
# +--------+-------------------+-------------------+

# String to date (European format dd/MM/yyyy)
df_converted = df.withColumn(
    "date_col", 
    to_date("date_str_eu", "dd/MM/yyyy")
)

df_converted.select("event_id", "date_str_eu", "date_col").show(truncate=False)

# Output:
# +--------+-----------+----------+
# |event_id|date_str_eu|date_col  |
# +--------+-----------+----------+
# |1       |15/01/2024 |2024-01-15|
# |2       |20/02/2024 |2024-02-20|
# |3       |10/03/2024 |2024-03-10|
# |4       |05/04/2024 |2024-04-05|
# +--------+-----------+----------+

# Add current timestamp and date
df_with_current = df.withColumn("current_ts", current_timestamp()) \
                    .withColumn("current_dt", current_date())

df_with_current.select("event_id", "current_ts", "current_dt").show(truncate=False)

# Output:
# +--------+-----------------------+----------+
# |event_id|current_ts             |current_dt|
# +--------+-----------------------+----------+
# |1       |2024-12-19 10:30:45.123|2024-12-19|
# |2       |2024-12-19 10:30:45.123|2024-12-19|
# |3       |2024-12-19 10:30:45.123|2024-12-19|
# |4       |2024-12-19 10:30:45.123|2024-12-19|
# +--------+-----------------------+----------+
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

#### Extracting Date Components - Real Example

```python
from pyspark.sql.functions import (
    year, month, dayofmonth, dayofweek, dayofyear,
    hour, minute, second, weekofyear, quarter,
    to_timestamp, when
)

# Sample order data with timestamps
order_data = [
    ("ORD001", "2024-01-15 09:30:45"),
    ("ORD002", "2024-03-22 14:15:00"),
    ("ORD003", "2024-06-08 18:45:30"),
    ("ORD004", "2024-09-01 11:00:00"),
    ("ORD005", "2024-12-25 08:00:00")
]

orders_df = spark.createDataFrame(order_data, ["order_id", "order_datetime"])

# Convert string to timestamp first
orders_df = orders_df.withColumn(
    "order_ts", 
    to_timestamp("order_datetime", "yyyy-MM-dd HH:mm:ss")
)

# Extract all date components
orders_with_components = orders_df \
    .withColumn("year", year("order_ts")) \
    .withColumn("month", month("order_ts")) \
    .withColumn("day", dayofmonth("order_ts")) \
    .withColumn("day_of_week", dayofweek("order_ts")) \
    .withColumn("day_of_year", dayofyear("order_ts")) \
    .withColumn("week_of_year", weekofyear("order_ts")) \
    .withColumn("quarter", quarter("order_ts"))

orders_with_components.select(
    "order_id", "order_ts", "year", "month", "day", "quarter"
).show(truncate=False)

# Output:
# +--------+-------------------+----+-----+---+-------+
# |order_id|order_ts           |year|month|day|quarter|
# +--------+-------------------+----+-----+---+-------+
# |ORD001  |2024-01-15 09:30:45|2024|1    |15 |1      |
# |ORD002  |2024-03-22 14:15:00|2024|3    |22 |1      |
# |ORD003  |2024-06-08 18:45:30|2024|6    |8  |2      |
# |ORD004  |2024-09-01 11:00:00|2024|9    |1  |3      |
# |ORD005  |2024-12-25 08:00:00|2024|12   |25 |4      |
# +--------+-------------------+----+-----+---+-------+

orders_with_components.select(
    "order_id", "day_of_week", "day_of_year", "week_of_year"
).show()

# Output:
# +--------+-----------+-----------+------------+
# |order_id|day_of_week|day_of_year|week_of_year|
# +--------+-----------+-----------+------------+
# |  ORD001|          2|         15|           3|
# |  ORD002|          6|         82|          12|
# |  ORD003|          7|        160|          23|
# |  ORD004|          1|        245|          35|
# |  ORD005|          4|        360|          52|
# +--------+-----------+-----------+------------+
# Note: day_of_week: 1=Sunday, 2=Monday, ..., 7=Saturday

# Extract time components
orders_with_time = orders_df \
    .withColumn("hour", hour("order_ts")) \
    .withColumn("minute", minute("order_ts")) \
    .withColumn("second", second("order_ts"))

orders_with_time.select("order_id", "order_ts", "hour", "minute", "second").show(truncate=False)

# Output:
# +--------+-------------------+----+------+------+
# |order_id|order_ts           |hour|minute|second|
# +--------+-------------------+----+------+------+
# |ORD001  |2024-01-15 09:30:45|9   |30    |45    |
# |ORD002  |2024-03-22 14:15:00|14  |15    |0     |
# |ORD003  |2024-06-08 18:45:30|18  |45    |30    |
# |ORD004  |2024-09-01 11:00:00|11  |0     |0     |
# |ORD005  |2024-12-25 08:00:00|8   |0     |0     |
# +--------+-------------------+----+------+------+

# Practical use case: Categorize orders by time of day
orders_categorized = orders_df \
    .withColumn("hour", hour("order_ts")) \
    .withColumn("time_of_day", 
        when(col("hour") < 12, "Morning")
        .when(col("hour") < 17, "Afternoon")
        .otherwise("Evening")
    )

orders_categorized.select("order_id", "order_ts", "hour", "time_of_day").show(truncate=False)

# Output:
# +--------+-------------------+----+-----------+
# |order_id|order_ts           |hour|time_of_day|
# +--------+-------------------+----+-----------+
# |ORD001  |2024-01-15 09:30:45|9   |Morning    |
# |ORD002  |2024-03-22 14:15:00|14  |Afternoon  |
# |ORD003  |2024-06-08 18:45:30|18  |Evening    |
# |ORD004  |2024-09-01 11:00:00|11  |Morning    |
# |ORD005  |2024-12-25 08:00:00|8   |Morning    |
# +--------+-------------------+----+-----------+

# Practical use case: Identify weekend vs weekday orders
orders_weekday = orders_df \
    .withColumn("day_of_week", dayofweek("order_ts")) \
    .withColumn("is_weekend", 
        when(col("day_of_week").isin(1, 7), "Weekend")
        .otherwise("Weekday")
    )

orders_weekday.select("order_id", "order_ts", "day_of_week", "is_weekend").show(truncate=False)

# Output:
# +--------+-------------------+-----------+----------+
# |order_id|order_ts           |day_of_week|is_weekend|
# +--------+-------------------+-----------+----------+
# |ORD001  |2024-01-15 09:30:45|2          |Weekday   |
# |ORD002  |2024-03-22 14:15:00|6          |Weekday   |
# |ORD003  |2024-06-08 18:45:30|7          |Weekend   |
# |ORD004  |2024-09-01 11:00:00|1          |Weekend   |
# |ORD005  |2024-12-25 08:00:00|4          |Weekday   |
# +--------+-------------------+-----------+----------+
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

#### Date Arithmetic - Real Example

```python
from pyspark.sql.functions import (
    date_add, date_sub, datediff, months_between, add_months,
    to_date, round, current_date, col, last_day, next_day, trunc
)

# Sample subscription data
subscription_data = [
    ("SUB001", "Alice", "2024-01-15", "2024-07-15"),
    ("SUB002", "Bob", "2024-03-01", "2025-03-01"),
    ("SUB003", "Charlie", "2024-06-20", "2024-12-20"),
    ("SUB004", "Diana", "2023-11-10", "2024-11-10"),
    ("SUB005", "Eve", "2024-08-01", None)  # Active subscription, no end date
]

subs_df = spark.createDataFrame(subscription_data, ["sub_id", "customer", "start_date", "end_date"])

# Convert strings to dates
subs_df = subs_df \
    .withColumn("start_date", to_date("start_date")) \
    .withColumn("end_date", to_date("end_date"))

# Add days - Calculate trial end date (30 days after start)
subs_with_trial = subs_df.withColumn(
    "trial_end_date", 
    date_add("start_date", 30)
)

subs_with_trial.select("sub_id", "customer", "start_date", "trial_end_date").show()

# Output:
# +------+--------+----------+--------------+
# |sub_id|customer|start_date|trial_end_date|
# +------+--------+----------+--------------+
# |SUB001|   Alice|2024-01-15|    2024-02-14|
# |SUB002|     Bob|2024-03-01|    2024-03-31|
# |SUB003| Charlie|2024-06-20|    2024-07-20|
# |SUB004|   Diana|2023-11-10|    2023-12-10|
# |SUB005|     Eve|2024-08-01|    2024-08-31|
# +------+--------+----------+--------------+

# Subtract days - Calculate reminder date (7 days before end)
subs_with_reminder = subs_df.withColumn(
    "reminder_date", 
    date_sub("end_date", 7)
)

subs_with_reminder.select("sub_id", "customer", "end_date", "reminder_date").show()

# Output:
# +------+--------+----------+-------------+
# |sub_id|customer|  end_date|reminder_date|
# +------+--------+----------+-------------+
# |SUB001|   Alice|2024-07-15|   2024-07-08|
# |SUB002|     Bob|2025-03-01|   2025-02-22|
# |SUB003| Charlie|2024-12-20|   2024-12-13|
# |SUB004|   Diana|2024-11-10|   2024-11-03|
# |SUB005|     Eve|      null|         null|
# +------+--------+----------+-------------+

# Calculate subscription duration in days
subs_with_duration = subs_df.withColumn(
    "duration_days", 
    datediff("end_date", "start_date")
)

subs_with_duration.select("sub_id", "customer", "start_date", "end_date", "duration_days").show()

# Output:
# +------+--------+----------+----------+-------------+
# |sub_id|customer|start_date|  end_date|duration_days|
# +------+--------+----------+----------+-------------+
# |SUB001|   Alice|2024-01-15|2024-07-15|          182|
# |SUB002|     Bob|2024-03-01|2025-03-01|          366|
# |SUB003| Charlie|2024-06-20|2024-12-20|          183|
# |SUB004|   Diana|2023-11-10|2024-11-10|          366|
# |SUB005|     Eve|2024-08-01|      null|         null|
# +------+--------+----------+----------+-------------+

# Calculate months between dates
subs_with_months = subs_df.withColumn(
    "duration_months", 
    round(months_between("end_date", "start_date"), 1)
)

subs_with_months.select("sub_id", "customer", "start_date", "end_date", "duration_months").show()

# Output:
# +------+--------+----------+----------+---------------+
# |sub_id|customer|start_date|  end_date|duration_months|
# +------+--------+----------+----------+---------------+
# |SUB001|   Alice|2024-01-15|2024-07-15|            6.0|
# |SUB002|     Bob|2024-03-01|2025-03-01|           12.0|
# |SUB003| Charlie|2024-06-20|2024-12-20|            6.0|
# |SUB004|   Diana|2023-11-10|2024-11-10|           12.0|
# |SUB005|     Eve|2024-08-01|      null|           null|
# +------+--------+----------+----------+---------------+

# Add months - Calculate renewal date
subs_with_renewal = subs_df.withColumn(
    "renewal_date", 
    add_months("end_date", 12)
)

subs_with_renewal.select("sub_id", "customer", "end_date", "renewal_date").show()

# Output:
# +------+--------+----------+------------+
# |sub_id|customer|  end_date|renewal_date|
# +------+--------+----------+------------+
# |SUB001|   Alice|2024-07-15|  2025-07-15|
# |SUB002|     Bob|2025-03-01|  2026-03-01|
# |SUB003| Charlie|2024-12-20|  2025-12-20|
# |SUB004|   Diana|2024-11-10|  2025-11-10|
# |SUB005|     Eve|      null|        null|
# +------+--------+----------+------------+

# Comprehensive date calculations
subs_comprehensive = subs_df \
    .withColumn("today", current_date()) \
    .withColumn("days_since_start", datediff(current_date(), "start_date")) \
    .withColumn("days_until_end", datediff("end_date", current_date())) \
    .withColumn("is_expired", col("end_date") < current_date()) \
    .withColumn("last_day_of_month", last_day("start_date")) \
    .withColumn("next_monday", next_day("start_date", "Monday"))

subs_comprehensive.select(
    "sub_id", "days_since_start", "days_until_end", "is_expired"
).show()

# Output (assuming today is 2024-12-19):
# +------+----------------+--------------+----------+
# |sub_id|days_since_start|days_until_end|is_expired|
# +------+----------------+--------------+----------+
# |SUB001|             339|          -157|      true|
# |SUB002|             294|            72|     false|
# |SUB003|             182|             1|     false|
# |SUB004|             405|          -39|      true|
# |SUB005|             140|          null|      null|
# +------+----------------+--------------+----------+

# Truncate date to month/year
subs_truncated = subs_df \
    .withColumn("start_month", trunc("start_date", "month")) \
    .withColumn("start_year", trunc("start_date", "year"))

subs_truncated.select("sub_id", "start_date", "start_month", "start_year").show()

# Output:
# +------+----------+-----------+----------+
# |sub_id|start_date|start_month|start_year|
# +------+----------+-----------+----------+
# |SUB001|2024-01-15| 2024-01-01|2024-01-01|
# |SUB002|2024-03-01| 2024-03-01|2024-01-01|
# |SUB003|2024-06-20| 2024-06-01|2024-01-01|
# |SUB004|2023-11-10| 2023-11-01|2023-01-01|
# |SUB005|2024-08-01| 2024-08-01|2024-01-01|
# +------+----------+-----------+----------+
```

### Date Formatting

```python
from pyspark.sql.functions import date_format

# Format date as string
df = df.withColumn("formatted_date", date_format("date_column", "yyyy-MM-dd"))
df = df.withColumn("formatted_date", date_format("date_column", "MMM dd, yyyy"))
df = df.withColumn("formatted_time", date_format("timestamp_column", "HH:mm:ss"))
```

#### Date Formatting - Real Example

```python
from pyspark.sql.functions import date_format, to_timestamp

# Sample event data
event_data = [
    ("EVT001", "2024-01-15 09:30:45"),
    ("EVT002", "2024-03-22 14:15:00"),
    ("EVT003", "2024-06-08 18:45:30"),
    ("EVT004", "2024-09-01 11:00:00"),
    ("EVT005", "2024-12-25 08:00:00")
]

events_df = spark.createDataFrame(event_data, ["event_id", "event_datetime"])

# Convert to timestamp
events_df = events_df.withColumn(
    "event_ts", 
    to_timestamp("event_datetime", "yyyy-MM-dd HH:mm:ss")
)

# Various date formats
formatted_df = events_df \
    .withColumn("iso_date", date_format("event_ts", "yyyy-MM-dd")) \
    .withColumn("us_date", date_format("event_ts", "MM/dd/yyyy")) \
    .withColumn("eu_date", date_format("event_ts", "dd/MM/yyyy")) \
    .withColumn("full_date", date_format("event_ts", "MMMM dd, yyyy")) \
    .withColumn("short_date", date_format("event_ts", "MMM dd, yyyy")) \
    .withColumn("day_name", date_format("event_ts", "EEEE")) \
    .withColumn("short_day", date_format("event_ts", "EEE"))

formatted_df.select("event_id", "iso_date", "us_date", "eu_date").show(truncate=False)

# Output:
# +--------+----------+----------+----------+
# |event_id|iso_date  |us_date   |eu_date   |
# +--------+----------+----------+----------+
# |EVT001  |2024-01-15|01/15/2024|15/01/2024|
# |EVT002  |2024-03-22|03/22/2024|22/03/2024|
# |EVT003  |2024-06-08|06/08/2024|08/06/2024|
# |EVT004  |2024-09-01|09/01/2024|01/09/2024|
# |EVT005  |2024-12-25|12/25/2024|25/12/2024|
# +--------+----------+----------+----------+

formatted_df.select("event_id", "full_date", "short_date").show(truncate=False)

# Output:
# +--------+------------------+-------------+
# |event_id|full_date         |short_date   |
# +--------+------------------+-------------+
# |EVT001  |January 15, 2024  |Jan 15, 2024 |
# |EVT002  |March 22, 2024    |Mar 22, 2024 |
# |EVT003  |June 08, 2024     |Jun 08, 2024 |
# |EVT004  |September 01, 2024|Sep 01, 2024 |
# |EVT005  |December 25, 2024 |Dec 25, 2024 |
# +--------+------------------+-------------+

formatted_df.select("event_id", "day_name", "short_day").show(truncate=False)

# Output:
# +--------+---------+---------+
# |event_id|day_name |short_day|
# +--------+---------+---------+
# |EVT001  |Monday   |Mon      |
# |EVT002  |Friday   |Fri      |
# |EVT003  |Saturday |Sat      |
# |EVT004  |Sunday   |Sun      |
# |EVT005  |Wednesday|Wed      |
# +--------+---------+---------+

# Time formatting
time_formatted_df = events_df \
    .withColumn("time_24h", date_format("event_ts", "HH:mm:ss")) \
    .withColumn("time_12h", date_format("event_ts", "hh:mm:ss a")) \
    .withColumn("hour_minute", date_format("event_ts", "HH:mm")) \
    .withColumn("full_datetime", date_format("event_ts", "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("readable_datetime", date_format("event_ts", "EEEE, MMMM dd, yyyy 'at' hh:mm a"))

time_formatted_df.select("event_id", "time_24h", "time_12h", "hour_minute").show(truncate=False)

# Output:
# +--------+--------+-----------+-----------+
# |event_id|time_24h|time_12h   |hour_minute|
# +--------+--------+-----------+-----------+
# |EVT001  |09:30:45|09:30:45 AM|09:30      |
# |EVT002  |14:15:00|02:15:00 PM|14:15      |
# |EVT003  |18:45:30|06:45:30 PM|18:45      |
# |EVT004  |11:00:00|11:00:00 AM|11:00      |
# |EVT005  |08:00:00|08:00:00 AM|08:00      |
# +--------+--------+-----------+-----------+

time_formatted_df.select("event_id", "readable_datetime").show(truncate=False)

# Output:
# +--------+----------------------------------------+
# |event_id|readable_datetime                       |
# +--------+----------------------------------------+
# |EVT001  |Monday, January 15, 2024 at 09:30 AM    |
# |EVT002  |Friday, March 22, 2024 at 02:15 PM      |
# |EVT003  |Saturday, June 08, 2024 at 06:45 PM     |
# |EVT004  |Sunday, September 01, 2024 at 11:00 AM  |
# |EVT005  |Wednesday, December 25, 2024 at 08:00 AM|
# +--------+----------------------------------------+

# Common date format patterns reference:
# yyyy - 4-digit year (2024)
# yy   - 2-digit year (24)
# MM   - 2-digit month (01-12)
# MMM  - Short month name (Jan, Feb)
# MMMM - Full month name (January, February)
# dd   - 2-digit day (01-31)
# d    - Day without leading zero (1-31)
# EEE  - Short day name (Mon, Tue)
# EEEE - Full day name (Monday, Tuesday)
# HH   - 24-hour format (00-23)
# hh   - 12-hour format (01-12)
# mm   - Minutes (00-59)
# ss   - Seconds (00-59)
# a    - AM/PM marker
# Q    - Quarter (1-4)
# w    - Week of year (1-52)
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

**Stateful operations** in PySpark UDFs allow you to maintain and accumulate information across multiple rows/records without needing to repartition data.

#### Common Use Cases

| Use Case | Benefit |
|---|---|
| **Context Windows** | Track last N rows for calculations |
| **Sequences** | Detect patterns or behaviors in time series |
| **Deduplication** | Maintain history of already processed values |
| **Custom Aggregations** | Custom logic for data accumulation |

#### Performance Benefits
- Avoids unnecessary shuffles
- Reduces communication between nodes
- More efficient processing than window functions for certain scenarios

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

### UDF vs Pandas UDF Comparison

| Aspect | Traditional UDF | Pandas UDF |
|---|---|---|
| **Processing** | Row-at-a-time | Batch/Vectorized |
| **Serialization** | Pickle (slow) | Apache Arrow (efficient) |
| **Input Format** | Scalar values | pd.Series / pd.DataFrame |
| **Performance** | 🐢 Slower | 🚀 10x-100x faster |
| **Optimization** | None | Vectorized operations |

#### When to Use Each

| Scenario | Recommendation |
|---|---|
| Simple and quick operations | Traditional UDF may suffice |
| Large data volumes | **Pandas UDF** (much more efficient) |
| Vectorized operations (numpy/pandas) | **Pandas UDF** |
| ML library compatibility | **Pandas UDF** |
| Legacy code | Traditional UDF |

**General Recommendation:** Always prefer **Pandas UDF** for production, especially with large data volumes.

---

## 9. Types of Variables in Spark

### 1. Local Variables (Driver Variables)
Regular variables defined on the driver. They are **NOT directly accessible** by executors unless explicitly shared.

```python
# Lives only on the driver
my_value = 100
lookup_table = {"a": 1, "b": 2}
```

⚠️ **Problem:** If used in transformations, Spark serializes and sends a copy to each task, causing inefficiency.

### 2. Broadcast Variables
Read-only variables efficiently **distributed once** to all executors and cached there.

| Feature | Description |
|---|---|
| **Direction** | Driver → Executors |
| **Mutability** | Read-only |
| **Use Case** | Lookup tables, ML models, configuration |
| **Size Limit** | Recommended < 10MB (can be larger) |

```python
# Create broadcast variable
lookup = {"US": "United States", "BR": "Brazil"}
broadcast_lookup = spark.sparkContext.broadcast(lookup)

# Access on executors
def map_country(code):
    return broadcast_lookup.value.get(code, "Unknown")

# Use in UDF
map_udf = udf(map_country, StringType())
df = df.withColumn("country_name", map_udf(col("country_code")))

# Use in transformation
def enrich_partition(partition):
    mapping = broadcast_lookup.value
    for row in partition:
        # Use mapping
        yield row

df.rdd.mapPartitions(enrich_partition)
```

### 3. Accumulators
Variables that executors can **add to**, but only the driver can read. Used for counters and sums.

| Feature | Description |
|---|---|
| **Direction** | Executors → Driver |
| **Operations** | Add only (associative) |
| **Use Case** | Counters, error tracking, metrics |
| **Read Access** | Only on driver |

```python
# Built-in accumulators
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
```

#### Types of Accumulators:
- **LongAccumulator** - For counting (integers)
- **DoubleAccumulator** - For sums (floating point)
- **CollectionAccumulator** - For collecting values (lists)

```python
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

### 4. Closure Variables
Variables captured when Spark serializes functions to send to executors.

```python
multiplier = 10  # Closure variable

def multiply(x):
    return x * multiplier  # Captured in closure

rdd.map(multiply)  # multiplier is serialized with the function
```

⚠️ **Warning:** Each task gets its own copy. Changes are NOT reflected back to driver.

### Variable Types Comparison Summary

| Variable Type | Scope | Direction | Mutability | Use Case |
|---|---|---|---|---|
| **Local** | Driver only | - | Read/Write | Driver logic |
| **Broadcast** | All executors | Driver → Executors | Read-only | Lookup tables |
| **Accumulator** | All executors | Executors → Driver | Write (add) | Counters, metrics |
| **Closure** | Per task copy | Driver → Task | Local copy | Small values in functions |

### Best Practices

| ✅ Do | ❌ Don't |
|---|---|
| Use broadcast for large lookups | Send large objects via closure |
| Use accumulators for debugging/metrics | Read accumulators inside transformations |
| Unpersist broadcasts when done | Forget to call `.value` on broadcasts |
| Use built-in accumulators when possible | Rely on accumulators for critical logic |

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
