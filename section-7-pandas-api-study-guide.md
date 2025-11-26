# Section 7: Using Pandas API on Spark - Study Guide

## Overview
Pandas API on Spark (formerly known as Koalas) provides a pandas-like API on top of Apache Spark, enabling data scientists to scale pandas code with minimal changes while leveraging Spark's distributed computing capabilities.

---

## 1. Advantages of Using Pandas API on Spark

### What is Pandas API on Spark?

**Definition:**
Pandas API on Spark is a pandas-compatible API built on top of Apache Spark that allows users to work with large datasets using familiar pandas syntax while benefiting from Spark's distributed computing.

**Import:**
```python
import pyspark.pandas as ps

# Also works with
from pyspark.pandas import DataFrame, Series
```

### Key Advantages

**1. Scalability Beyond Memory Limits**

```python
# Pandas (limited to single machine memory)
import pandas as pd
df = pd.read_csv("large_file.csv")  # OutOfMemoryError if file > RAM

# Pandas API on Spark (distributed across cluster)
import pyspark.pandas as ps
df = ps.read_csv("large_file.csv")  # Handles TB+ datasets
```

**Benefits:**
- Process datasets larger than single machine memory
- Automatic data distribution across cluster
- No memory overflow errors
- Scale horizontally by adding nodes

**2. Minimal Code Changes**

```python
# Original Pandas code
import pandas as pd

df = pd.read_csv("/data/sales.csv")
df['total'] = df['quantity'] * df['price']
result = df.groupby('category')['total'].sum()
print(result)

# Pandas API on Spark (almost identical)
import pyspark.pandas as ps

df = ps.read_csv("/data/sales.csv")
df['total'] = df['quantity'] * df['price']
result = df.groupby('category')['total'].sum()
print(result)
```

**Benefits:**
- Familiar pandas syntax
- Easy migration of existing pandas code
- Reduced learning curve
- Reuse existing pandas knowledge

**3. Distributed Computing Power**

```python
# Leverage Spark's distributed processing
import pyspark.pandas as ps

# Read large dataset (distributed across cluster)
df = ps.read_parquet("/big-data/sales")

# Operations run in parallel across executors
filtered = df[df['amount'] > 1000]
aggregated = filtered.groupby(['region', 'category']).agg({
    'amount': ['sum', 'mean', 'count'],
    'quantity': 'sum'
})

# Write results (parallelized)
aggregated.to_parquet("/output/results")
```

**Benefits:**
- Parallel processing across executors
- Faster execution for large datasets
- Efficient resource utilization
- Built-in fault tolerance

**4. Seamless Integration with Spark Ecosystem**

```python
# Convert between Pandas API on Spark and Spark DataFrames
import pyspark.pandas as ps
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Pandas API on Spark DataFrame
ps_df = ps.read_csv("/data/sales.csv")

# Convert to Spark DataFrame
spark_df = ps_df.to_spark()

# Use Spark SQL
spark_df.createOrReplaceTempView("sales")
result = spark.sql("SELECT category, SUM(amount) FROM sales GROUP BY category")

# Convert back to Pandas API on Spark
ps_result = result.to_pandas_on_spark()

# Or convert to regular pandas (collects to driver)
pandas_result = result.toPandas()
```

**Benefits:**
- Use Spark SQL when needed
- Access full Spark API
- Mix pandas-style and Spark operations
- Leverage existing Spark libraries (MLlib, Streaming)

**5. Better Performance Than Pure Pandas for Large Data**

```python
import time
import pandas as pd
import pyspark.pandas as ps

# Pandas (single-threaded, memory-bound)
start = time.time()
df_pandas = pd.read_csv("10GB_file.csv")  # May crash
result_pandas = df_pandas.groupby('key').agg({'value': 'sum'})
print(f"Pandas time: {time.time() - start}s")

# Pandas API on Spark (distributed, parallel)
start = time.time()
df_ps = ps.read_csv("10GB_file.csv")  # No problem
result_ps = df_ps.groupby('key').agg({'value': 'sum'})
print(f"Pandas on Spark time: {time.time() - start}s")
```

**Benefits:**
- Parallel execution across cores
- Distributed data processing
- No single-machine bottleneck
- Optimized execution plans (Catalyst)

**6. Built-in Support for Missing Pandas Features at Scale**

```python
import pyspark.pandas as ps

# Operations that are slow/impossible in pandas
df = ps.read_parquet("/massive-dataset")

# Window functions at scale
df['rank'] = df.groupby('category')['sales'].rank()

# Rolling operations on large data
df['moving_avg'] = df.groupby('store_id')['sales'].rolling(window=7).mean()

# Efficient joins on large datasets
sales = ps.read_parquet("/sales")
products = ps.read_parquet("/products")
merged = sales.merge(products, on='product_id', how='inner')
```

**Benefits:**
- Complex operations scale automatically
- Window functions without memory issues
- Large joins handled efficiently
- Time series operations at scale

**7. Lazy Evaluation and Query Optimization**

```python
import pyspark.pandas as ps

# Operations are lazy (not executed immediately)
df = ps.read_parquet("/data")
df_filtered = df[df['amount'] > 100]
df_grouped = df_filtered.groupby('category').sum()

# Execution happens when result is needed
print(df_grouped)  # Triggers execution

# Spark optimizes the entire query plan
# - Predicate pushdown
# - Column pruning
# - Join reordering
```

**Benefits:**
- Optimized query execution
- Reduced data movement
- Better resource utilization
- Automatic optimization by Catalyst

**8. Interactive Data Science Workflows**

```python
# Jupyter notebook with Pandas API on Spark
import pyspark.pandas as ps
import matplotlib.pyplot as plt

# Explore large dataset interactively
df = ps.read_parquet("/big-data/customer-events")

# Quick stats
print(df.describe())
print(df.head(10))

# Sample for visualization (brings small sample to driver)
sample = df.sample(n=10000)
sample_pd = sample.to_pandas()
sample_pd['amount'].hist()
plt.show()

# Full aggregation on entire dataset
daily_stats = df.groupby(df['date'].dt.date)['amount'].agg(['sum', 'mean', 'count'])
daily_stats.plot()
```

**Benefits:**
- Familiar pandas workflow
- Interactive exploration of big data
- Easy visualization with sampling
- Quick iterations

**9. Automatic Handling of Data Types**

```python
import pyspark.pandas as ps

# Pandas API on Spark handles type conversions
df = ps.read_csv("/data/mixed_types.csv")

# Automatic type inference
print(df.dtypes)

# Type conversions work like pandas
df['amount'] = df['amount'].astype('float')
df['date'] = ps.to_datetime(df['date'])

# String operations
df['upper_name'] = df['name'].str.upper()
```

**Benefits:**
- Familiar type system
- Automatic type inference
- Easy type conversions
- String/datetime operations

**10. Production-Ready Features**

```python
import pyspark.pandas as ps

# Fault tolerance built-in
df = ps.read_parquet("/data")

# Checkpointing for long pipelines
df = df.spark.checkpoint()

# Cache for reuse
df = df.spark.cache()

# Efficient partitioning
df.to_parquet("/output", partition_cols=['year', 'month'])

# Monitoring via Spark UI
# All operations visible in Spark UI for debugging
```

**Benefits:**
- Automatic fault recovery
- Caching for performance
- Partitioning support
- Production monitoring tools

### Pandas vs Pandas API on Spark Comparison

| Feature | Pandas | Pandas API on Spark |
|---------|--------|-------------------|
| **Data Size** | Limited to memory | Unlimited (distributed) |
| **Execution** | Single-threaded | Multi-threaded, distributed |
| **Memory** | Single machine RAM | Cluster memory |
| **Performance** | Fast for small data | Fast for large data |
| **API** | Full pandas API | Most pandas API (~95%) |
| **Learning Curve** | None (if you know pandas) | Minimal |
| **Scalability** | Vertical (bigger machine) | Horizontal (more nodes) |
| **Fault Tolerance** | None | Built-in (Spark) |
| **Integration** | Python ecosystem | Spark ecosystem |
| **Best For** | < 10GB datasets | > 10GB datasets |

### When to Use Pandas API on Spark

**Use Pandas API on Spark When:**
```python
# ✓ Dataset doesn't fit in memory
# ✓ Need distributed processing
# ✓ Want to scale pandas code
# ✓ Working with TB+ data
# ✓ Need fault tolerance
# ✓ Have existing pandas code to migrate
# ✓ Want familiar pandas syntax with Spark power

import pyspark.pandas as ps
df = ps.read_parquet("/big-data")  # TB scale
```

**Use Regular Pandas When:**
```python
# ✓ Dataset fits comfortably in memory (< 10GB)
# ✓ Need cutting-edge pandas features
# ✓ Single-machine processing is sufficient
# ✓ No need for distribution
# ✓ Working with small to medium data

import pandas as pd
df = pd.read_csv("small_data.csv")  # MB to GB scale
```

### Common Use Cases

**1. ETL Pipelines:**
```python
import pyspark.pandas as ps

# Read from multiple sources
sales = ps.read_parquet("/data/sales")
customers = ps.read_csv("/data/customers.csv")

# Transform
sales['date'] = ps.to_datetime(sales['date'])
sales['revenue'] = sales['quantity'] * sales['price']

# Join
enriched = sales.merge(customers, on='customer_id', how='left')

# Aggregate
daily_summary = enriched.groupby('date').agg({
    'revenue': 'sum',
    'customer_id': 'nunique',
    'order_id': 'count'
})

# Write
daily_summary.to_parquet("/output/daily_summary")
```

**2. Data Analysis:**
```python
import pyspark.pandas as ps

# Load large dataset
df = ps.read_parquet("/logs/clickstream")

# Exploratory analysis
print(df.describe())
print(df['event_type'].value_counts())

# Complex aggregations
user_behavior = df.groupby('user_id').agg({
    'timestamp': ['min', 'max'],
    'page_views': 'sum',
    'session_duration': 'mean'
})

# Statistical analysis
correlation = df[['page_views', 'session_duration', 'conversions']].corr()
```

**3. Feature Engineering for ML:**
```python
import pyspark.pandas as ps

df = ps.read_parquet("/data/features")

# Create features
df['hour'] = df['timestamp'].dt.hour
df['day_of_week'] = df['timestamp'].dt.dayofweek
df['is_weekend'] = df['day_of_week'].isin([5, 6])

# Scaling
df['amount_scaled'] = (df['amount'] - df['amount'].mean()) / df['amount'].std()

# One-hot encoding
df = ps.get_dummies(df, columns=['category'])

# Write for ML
df.to_parquet("/ml/features")
```

---

## 2. Pandas UDF (User-Defined Functions)

### What are Pandas UDFs?

**Definition:**
Pandas UDFs (also called Vectorized UDFs) are user-defined functions that execute vectorized operations on Pandas Series/DataFrames, offering significantly better performance than row-by-row Python UDFs.

**Advantages over Regular UDFs:**
- **Vectorized execution** (operates on batches, not individual rows)
- **Better performance** (10-100x faster than row-at-a-time UDFs)
- **Arrow optimization** (efficient data transfer)
- **Leverage pandas operations** (use pandas functions inside UDF)

### Types of Pandas UDFs

**1. Series to Series**
**2. Iterator of Series to Iterator of Series**
**3. Iterator of Multiple Series to Iterator of Series**
**4. Series to Scalar (Aggregation)**
**5. GroupBy Aggregation**

### 1. Series to Series Pandas UDF

**Basic Example:**
```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType
import pandas as pd

# Define Pandas UDF
@pandas_udf(DoubleType())
def multiply_by_two(series: pd.Series) -> pd.Series:
    return series * 2

# Use in DataFrame
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame([(1,), (2,), (3,)], ["value"])
df = df.withColumn("doubled", multiply_by_two("value"))
df.show()

# Output:
# +-----+-------+
# |value|doubled|
# +-----+-------+
# |    1|    2.0|
# |    2|    4.0|
# |    3|    6.0|
# +-----+-------+
```

**Complex Transformation:**
```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType
import pandas as pd

@pandas_udf(StringType())
def process_text(text: pd.Series) -> pd.Series:
    # Vectorized string operations
    return text.str.lower().str.strip().str.replace('[^a-z0-9]', '', regex=True)

df = spark.createDataFrame([
    ("  HELLO WORLD!  ",),
    ("  Test-123  ",),
    ("SPARK  ",)
], ["text"])

df = df.withColumn("cleaned", process_text("text"))
df.show(truncate=False)

# Output:
# +------------------+-----------+
# |text              |cleaned    |
# +------------------+-----------+
# |  HELLO WORLD!    |helloworld |
# |  Test-123        |test123    |
# |SPARK             |spark      |
# +------------------+-----------+
```

### 2. Iterator of Series to Iterator of Series

**Use Case:** Processing data in batches for efficiency (e.g., loading models once per batch)

```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType
import pandas as pd
from typing import Iterator

@pandas_udf(DoubleType())
def predict_batch(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    # Load model once for entire partition
    model = load_model()  # Expensive operation done once
    
    # Process batches
    for batch in iterator:
        predictions = model.predict(batch)
        yield pd.Series(predictions)

df = spark.read.parquet("/data/features")
df = df.withColumn("prediction", predict_batch("features"))
```

**Real Example with ML Model:**
```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType
import pandas as pd
from typing import Iterator
import pickle

@pandas_udf(DoubleType())
def score_model(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    # Load model once per executor partition
    with open("/models/model.pkl", "rb") as f:
        model = pickle.load(f)
    
    # Process each batch
    for features_batch in iterator:
        # Reshape for sklearn
        X = features_batch.values.reshape(-1, 1)
        predictions = model.predict(X)
        yield pd.Series(predictions)

df = spark.read.parquet("/data/input")
df_scored = df.withColumn("score", score_model("feature"))
df_scored.write.parquet("/data/scored")
```

### 3. Iterator of Multiple Series to Iterator of Series

**Use Case:** Multiple input columns, batch processing

```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType
import pandas as pd
from typing import Iterator, Tuple

@pandas_udf(DoubleType())
def complex_calculation(iterator: Iterator[Tuple[pd.Series, pd.Series, pd.Series]]) -> Iterator[pd.Series]:
    for col1, col2, col3 in iterator:
        # Complex calculation on multiple columns
        result = (col1 * col2 + col3) / (col1 + col2 + 1)
        yield result

df = spark.createDataFrame(
    [(1.0, 2.0, 3.0), (4.0, 5.0, 6.0)],
    ["a", "b", "c"]
)

df = df.withColumn("result", complex_calculation("a", "b", "c"))
df.show()
```

### 4. Series to Scalar (Aggregate Pandas UDF)

**Use Case:** Custom aggregation functions

```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType
import pandas as pd

# Weighted average aggregation
@pandas_udf(DoubleType())
def weighted_average(values: pd.Series, weights: pd.Series) -> float:
    return (values * weights).sum() / weights.sum()

df = spark.createDataFrame([
    (1, 100.0, 1.0),
    (1, 200.0, 2.0),
    (2, 150.0, 1.5),
    (2, 250.0, 2.5)
], ["group", "value", "weight"])

# Use in aggregation
result = df.groupBy("group").agg(
    weighted_average("value", "weight").alias("weighted_avg")
)
result.show()

# Output:
# +-----+------------------+
# |group|      weighted_avg|
# +-----+------------------+
# |    1|166.66666666666666|
# |    2|208.33333333333334|
# +-----+------------------+
```

**Percentile Calculation:**
```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType
import pandas as pd

@pandas_udf(DoubleType())
def percentile_95(values: pd.Series) -> float:
    return values.quantile(0.95)

df = spark.range(0, 1000).withColumn("value", "id")
result = df.agg(percentile_95("value").alias("p95"))
result.show()
```

### 5. GroupBy Aggregation with Pandas UDF

**Use Case:** Apply pandas operations to grouped data

```python
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import pandas as pd

# Define output schema
schema = StructType([
    StructField("category", StringType()),
    StructField("mean", DoubleType()),
    StructField("std", DoubleType()),
    StructField("count", DoubleType())
])

@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def calculate_stats(pdf: pd.DataFrame) -> pd.DataFrame:
    # Process entire group as pandas DataFrame
    return pd.DataFrame({
        'category': [pdf['category'].iloc[0]],
        'mean': [pdf['value'].mean()],
        'std': [pdf['value'].std()],
        'count': [len(pdf)]
    })

df = spark.createDataFrame([
    ("A", 10.0), ("A", 20.0), ("A", 30.0),
    ("B", 15.0), ("B", 25.0), ("B", 35.0)
], ["category", "value"])

result = df.groupBy("category").apply(calculate_stats)
result.show()

# Output:
# +--------+----+------------------+-----+
# |category|mean|               std|count|
# +--------+----+------------------+-----+
# |       A|20.0|10.0              |  3.0|
# |       B|25.0|10.0              |  3.0|
# +--------+----+------------------+-----+
```

**Time Series Interpolation:**
```python
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructType, StructField, TimestampType, DoubleType
import pandas as pd

schema = StructType([
    StructField("timestamp", TimestampType()),
    StructField("sensor_id", StringType()),
    StructField("value", DoubleType()),
    StructField("interpolated", DoubleType())
])

@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def interpolate_missing(pdf: pd.DataFrame) -> pd.DataFrame:
    # Sort by timestamp
    pdf = pdf.sort_values('timestamp')
    
    # Interpolate missing values
    pdf['interpolated'] = pdf['value'].interpolate(method='linear')
    
    return pdf

df = spark.read.parquet("/sensors/data")
result = df.groupBy("sensor_id").apply(interpolate_missing)
result.write.parquet("/sensors/interpolated")
```

### Pandas UDF Best Practices

**1. Choose Appropriate UDF Type:**
```python
# ✓ Good: Use iterator for large data
@pandas_udf(DoubleType())
def process(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    model = load_model()  # Load once
    for batch in iterator:
        yield model.transform(batch)

# ✗ Bad: Load model for every batch
@pandas_udf(DoubleType())
def process(series: pd.Series) -> pd.Series:
    model = load_model()  # Loads many times!
    return model.transform(series)
```

**2. Vectorize Operations:**
```python
# ✓ Good: Vectorized pandas operations
@pandas_udf(DoubleType())
def calculate(values: pd.Series) -> pd.Series:
    return values.apply(lambda x: x ** 2)  # Vectorized

# ✗ Bad: Python loops
@pandas_udf(DoubleType())
def calculate(values: pd.Series) -> pd.Series:
    result = []
    for v in values:
        result.append(v ** 2)
    return pd.Series(result)
```

**3. Specify Return Types:**
```python
# ✓ Good: Explicit type
@pandas_udf(DoubleType())
def multiply(x: pd.Series) -> pd.Series:
    return x * 2

# ✗ Bad: No type (may cause errors)
@pandas_udf()
def multiply(x):
    return x * 2
```

**4. Handle Null Values:**
```python
@pandas_udf(DoubleType())
def safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    # Handle division by zero and nulls
    result = numerator / denominator
    result = result.replace([float('inf'), float('-inf')], None)
    return result
```

**5. Use Arrow for Performance:**
```python
# Enable Arrow optimization
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

# Pandas UDF automatically uses Arrow for data transfer
@pandas_udf(DoubleType())
def process(x: pd.Series) -> pd.Series:
    return x * 2
```

### Performance Comparison

```python
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import DoubleType
import time

# Regular UDF (row-by-row)
@udf(returnType=DoubleType())
def regular_udf(x):
    return float(x) * 2

# Pandas UDF (vectorized)
@pandas_udf(DoubleType())
def pandas_udf_func(x: pd.Series) -> pd.Series:
    return x * 2

df = spark.range(0, 10000000)

# Regular UDF
start = time.time()
df.withColumn("result", regular_udf("id")).count()
regular_time = time.time() - start

# Pandas UDF
start = time.time()
df.withColumn("result", pandas_udf_func("id")).count()
pandas_time = time.time() - start

print(f"Regular UDF: {regular_time}s")
print(f"Pandas UDF: {pandas_time}s")
print(f"Speedup: {regular_time / pandas_time}x")

# Typical output:
# Regular UDF: 45.2s
# Pandas UDF: 2.3s
# Speedup: 19.7x
```

### Complete Example: Feature Engineering Pipeline

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import DoubleType, StringType
import pandas as pd
from typing import Iterator

spark = SparkSession.builder.appName("PandasUDF Example").getOrCreate()

# Enable Arrow
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Define Pandas UDFs
@pandas_udf(DoubleType())
def normalize(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    # Calculate mean and std for entire column
    all_data = pd.concat(list(iterator))
    mean = all_data.mean()
    std = all_data.std()
    
    # Normalize
    for batch in [all_data]:
        yield (batch - mean) / std

@pandas_udf(StringType())
def categorize_amount(amounts: pd.Series) -> pd.Series:
    return pd.cut(
        amounts,
        bins=[0, 100, 500, float('inf')],
        labels=['low', 'medium', 'high']
    )

@pandas_udf(DoubleType())
def calculate_features(price: pd.Series, quantity: pd.Series, discount: pd.Series) -> pd.Series:
    # Complex feature calculation
    total = price * quantity
    discounted = total * (1 - discount)
    return discounted * 1.1  # Add tax

# Load data
df = spark.read.parquet("/data/transactions")

# Apply UDFs
result = df \
    .withColumn("normalized_amount", normalize("amount")) \
    .withColumn("amount_category", categorize_amount("amount")) \
    .withColumn("final_price", calculate_features("price", "quantity", "discount"))

# Write results
result.write.parquet("/data/features")

spark.stop()
```

---

## Key Concepts Summary

### Pandas API on Spark
- **Scalability**: Process data larger than memory
- **Familiarity**: Use pandas syntax on Spark
- **Performance**: Distributed computing for big data
- **Integration**: Seamless with Spark ecosystem

### Pandas UDFs
- **Vectorized**: Operate on batches, not rows
- **Performance**: 10-100x faster than regular UDFs
- **Types**: Series-to-Series, Iterator, Aggregation, GroupBy
- **Arrow**: Efficient data transfer between JVM and Python

---

## Best Practices

### Pandas API on Spark
1. Use for datasets > 10GB
2. Minimize conversions to/from pandas
3. Leverage lazy evaluation
4. Cache when reusing data
5. Use appropriate file formats (Parquet)

### Pandas UDFs
1. Use iterator style for large data
2. Vectorize operations
3. Load models once per partition
4. Handle nulls explicitly
5. Enable Arrow optimization
6. Prefer Pandas UDFs over regular UDFs

---

## Common Exam Topics

- Advantages of Pandas API on Spark
- When to use Pandas API on Spark vs regular pandas
- Types of Pandas UDFs
- Pandas UDF performance benefits
- Iterator-based Pandas UDFs
- GroupBy aggregation with Pandas UDFs
- Arrow optimization
- Converting between Spark and Pandas API on Spark

---

## Practice Questions

1. **What is the main advantage of Pandas API on Spark?**
   - Scale pandas code to handle large datasets using distributed computing

2. **How much faster are Pandas UDFs compared to regular UDFs?**
   - Typically 10-100x faster due to vectorized execution

3. **When should you use iterator-style Pandas UDFs?**
   - When you need to load resources once per partition (e.g., ML models)

4. **What is Arrow and why is it important for Pandas UDFs?**
   - Arrow is a columnar memory format that enables efficient data transfer between JVM and Python

5. **Can you use all pandas operations in Pandas API on Spark?**
   - Most (~95%), but some operations are not available or have slight differences

6. **What's the difference between Series to Series and GroupBy Pandas UDFs?**
   - Series to Series operates on individual columns; GroupBy operates on grouped DataFrames

7. **When should you use Pandas API on Spark instead of regular Spark DataFrames?**
   - When you have existing pandas code to migrate or prefer pandas-style syntax

---

## Additional Resources

- [Pandas API on Spark Documentation](https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/index.html)
- [Pandas UDFs Documentation](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html)
- [Apache Arrow](https://arrow.apache.org/)
