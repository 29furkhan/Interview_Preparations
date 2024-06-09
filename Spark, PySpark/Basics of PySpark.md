****
##### Window Functions ^c18388

> `1. Window functions are used to calculate results, such as rank, row_number, over a range of input values.`
> `2. Window functions operates on group of rows (partition) and returns a single value for each input row.`
> `3. Kinds of window functions.`
> >`1. Ranking functions` 
> >`2. Analytical functions`
> >`3. aggregate functions`
> 
> `1. Ranking functions: row_number(), rank(), and dense_number() are ranking functions. These assigns the sequential numbers to DFs based on criteria specified in partition. `
>*  [[Functions#^04cbdc|row_number()]]
>* [[Functions#^23ec05|rank()]]
>* [[Functions#^78d32c|dense_rank()]]
>
> `Both rank and dense_rank assigns the same rank if duplicates are found even in criteria. However, row_number randomly assigns unique value to each input.`
> 
> More on window functions can be read [Here](https://sparkbyexamples.com/pyspark/pyspark-window-functions/#ranking-functions)


##### Schema  Comparison ^496137

> `1. It helps us unify the schema in case we deal with multiple dataframes.`
>> `1. Find missing columns`
>> `2. Collect all the columns`
>> `3. Compare schema`
>
>```python
># Compare schema of 2 DFs.
>if df1.schema == df2.schema:
>	print("Schema is same")
>else:
>	print("Schema is not same")
>```
>```python
># Find missing columns
>set(df1.schema) - set(df2.schema)
># or
>set(df1.columns) - set(df2.columns)
>```
>```python
># Collect all columns
>all_cols = set(df1.columns + df2.columns)
>```
>```python
># Add the missing columns with None value
>for i in all_cols:
>	df2 = df2.withColumn(i, lit(None))
>```
****
##### StructType and StructField ^8a58d8

> Description
> >`1. Classes used to programmatically specify the schema of the DF. We can use it to create complex structures like struct, array, and map columns.,`
> `2. StructType: Class to create structure of DF.`
> >`3. StructField: Specify column name, data type, flag for null, nested arrays, etc.`
>
>Imports
>>```python
>>from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType
>>```
>
>Data
>```python
># Schema preparation
>schema = StructType([
StructField("Name", StringType(), True),
StructField("Age", IntegerType()),
StructField("Commission", FloatType()),
StructField("Address", StructType([
>	StructField("City", StringType()),
>	StructField("State", StringType()),
>	StructField("Pincode", IntegerType()),
>	])),
StructField("Technologies", ArrayType(StringType()))
])
># Prepare data as per schema
data = [("Furkhan", 29, 6500.234,("Pune", "Maharashtra", 412308), ["Python", "SQL", "PySpark"])]
df = spark.createDataFrame(data=data, schema=schema)
>```
>Resultant Schema
>```
>|-- Name: string (nullable = true)
 |-- Age: integer (nullable = true)
 |-- Commission: float (nullable = true)
 |-- Address: struct (nullable = true)
 |    |-- City: string (nullable = true)
 |    |-- State: string (nullable = true)
 |    |-- Pincode: integer (nullable = true)
 |-- Technologies: array (nullable = true)
 |    |-- element: string (containsNull = true)
>```
##### MapType and ArrayType ^08821d

**MapType**
>Description
>>`* Represents python dictionary that stores key-value pair.`
>>`* MapType object consists of key type, value type, Nullable.`
>
>Imports
>```python
>from pyspark.sql.types import MapType
>```
>Data
>```python
># Schema for a map type
>schema = StructType([
>	StructField("Name", StringType(), False),
>	StructField("Properties", MapType(StringType(), StringType(), False))
])
>```
>Schema
>```python
>root
 |-- Name: string (nullable = false)
 |-- Properties: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = false)
>```
>Queries
>```python
>data = [("Furkhan", {
"Age": "24",
"Gender": "Male",
"Salary": "100$"
})]
df = spark.createDataFrame(data=data, schema=schema)
>```
>Result
>```
>+-------+-------------------------------------------+
|Name   |Properties                                 |
+-------+-------------------------------------------+
|Furkhan|{Salary -> 100$, Gender -> Male, Age -> 24}|
+-------+-------------------------------------------+
>```

**ArrayType**
>Description
>
>> `* Array of multiple elements of same type. `
> >` * ArrayType(<DataType> of elements)`
> 
> Imports
>```python
>from pyspark.sql.types import ArrayType
>```
> Data and Schema
>```python
># Schema for a map type
schema = StructType([
>	StructField("Company", StringType(), False),
>	StructField("Locations", ArrayType(StringType(), True))
])
> # Data as per the schema, some NULLs and ""
>data = [("TCS", ("Pune", None, "Chennai")), ("Infosys", ("Mumbai", "", "Raichur"))]
df = spark.createDataFrame(data=data, schema=schema)
>```
> Result
>```
>+-------+---------------------+--------+
|Company|Locations            |Location|
+-------+---------------------+--------+
|TCS    |[Pune, NULL, Chennai]|Pune    |
|TCS    |[Pune, NULL, Chennai]|Chennai |
|Infosys|[Mumbai, , Raichur]  |Mumbai  |
|Infosys|[Mumbai, , Raichur]  |Raichur |
+-------+---------------------+--------+
>```

##### RDD 

**`Definition: `**
- `An RDD is abstraction in spark.`
- `Represents an immutable, partitioned collection of elements that can be operated in parallel.`
- `Once created, RDDs cannot be changed, you can transform existing RDDs to create new one.`
- `Transformations are not performed immedietly, they are done only when action is triggered.`
- `RDDs tracks the transforamtions info to recompute lost data making it fault tolerant.`

**`RDD operations.`**
- `Transformations: Creating new RDD from existing one.`
- `Actions: Triggers computation and returns result.`

**`Transformations:`**
- `Transformations always returns a new RDD.`
- `Not executed immedietly.` 
- `Ex. map(), filter(), flatMap(), reduceByKey(), etc.`

**`Actions:`**
- `Triggers computation and returns some result or write data.`
- `All series of transformations are executed when action is triggered.`
- `Ex. collect(), count(), first(), take(n), reduce(func), etc.`

**`How to identify difference between these 2?`**
1. `Does it return a new RDD? Yes -> transformation.`
2. `Does it return a value or write data? Yes -> action.`
3. `Is it triggering computation? Yes -> action.`

```python
rdd = sc.parallelize([1, 2, 3, 4, 5]) 
result = rdd.collect() 
# Output: [1, 2, 3, 4, 5]
```
##### DataFrame
**`Definition: `**
- `Data structure offered by PySpark. Data organized into named columns.`
- `Higher level of abstraction compared to RDDs. SQL like operations supported. (Spark SQL).`
- `It comes with ease of usage (APIs are more user friendly), performance, and functionality.`
- `Comes with rich amount of built-in functions.`
- `You can query DFs using SQL, create temp tables.`
- `Support read/ write to/ from multiple sources like JSONs, CSV, etc easily.`
- `DFs allows schema that enforce data types and structure ensuring data consistency.`
- ``

**`DataFrame operations.`**
- `Transformations: Creating new DFs from existing one.`
- `Actions: Triggers computation and returns result.`

**`Transformations:`**
- `select: Project subset of columns.`
- `filter: Filters rows based on a condition.`
- `withColumn: Adds a new or update existing column.`
- `groupBy.`
- `join.`
- `distinct.`
- `orderBy.`

`None of the above functions triggers actions or returns a computation. They all returns a new DF.`

**`Actions:`**
- `show: display first n rows.`
- `collect: Returns a list of row objects.`
- `count.`
- `first.`
- `take.`
- `write.`
- `describe()`
- `printSchema()`

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrameExample").getOrCreate()

# Sample DataFrame
data = [("Alice", 34, "Female"), ("Bob", 45, "Male"), ("Cathy", 29, "Female"), ("David", 31, "Male")]
df = spark.createDataFrame(data, ["Name", "Age", "Gender"])

# Transformation Examples

# Select columns
df_select = df.select("Name", "Age")
df_select.show()
# Output:
# +-----+---+
# | Name|Age|
# +-----+---+
# |Alice| 34|
# |  Bob| 45|
# |Cathy| 29|
# |David| 31|
# +-----+---+

# Filter rows
df_filter = df.filter(df["Age"] > 30)
df_filter.show()
# Output:
# +-----+---+------+
# | Name|Age|Gender|
# +-----+---+------+
# |Alice| 34|Female|
# |  Bob| 45|  Male|
# |David| 31|  Male|
# +-----+---+------+

# Add a new column
df_with_column = df.withColumn("Age_Double", df["Age"] * 2)
df_with_column.show()
# Output:
# +-----+---+------+----------+
# | Name|Age|Gender|Age_Double|
# +-----+---+------+----------+
# |Alice| 34|Female|        68|
# |  Bob| 45|  Male|        90|
# |Cathy| 29|Female|        58|
# |David| 31|  Male|        62|
# +-----+---+------+----------+

# Group by and aggregate
df_grouped = df.groupBy("Gender").agg({"Age": "avg"})
df_grouped.show()
# Output:
# +------+--------+
# |Gender|avg(Age)|
# +------+--------+
# |Female|    31.5|
# |  Male|    38.0|
# +------+--------+

# Join two DataFrames
data2 = [("Alice", "HR"), ("Bob", "IT"), ("Cathy", "Finance"), ("David", "Marketing")]
df2 = spark.createDataFrame(data2, ["Name", "Department"])

df_join = df.join(df2, on="Name")
df_join.show()
# Output:
# +-----+---+------+----------+
# | Name|Age|Gender|Department|
# +-----+---+------+----------+
# |Alice| 34|Female|        HR|
# |  Bob| 45|  Male|        IT|
# |Cathy| 29|Female|   Finance|
# |David| 31|  Male| Marketing|
# +-----+---+------+----------+

# Action Examples

# Show first few rows
df.show()
# Output:
# +-----+---+------+
# | Name|Age|Gender|
# +-----+---+------+
# |Alice| 34|Female|
# |  Bob| 45|  Male|
# |Cathy| 29|Female|
# |David| 31|  Male|
# +-----+---+------+

# Collect rows as a list
rows = df.collect()
print(rows)
# Output: [Row(Name='Alice', Age=34, Gender='Female'), Row(Name='Bob', Age=45, Gender='Male'), Row(Name='Cathy', Age=29, Gender='Female'), Row(Name='David', Age=31, Gender='Male')]

# Count number of rows
count = df.count()
print(count)
# Output: 4

# Get the first row
first_row = df.first()
print(first_row)
# Output: Row(Name='Alice', Age=34, Gender='Female')

# Take the first 2 rows
first_two_rows = df.take(2)
print(first_two_rows)
# Output: [Row(Name='Alice', Age=34, Gender='Female'), Row(Name='Bob', Age=45, Gender='Male')]

# Write DataFrame to a file
df.write.csv("output.csv")
```
**`How to identify difference between these 2?`**
1. `Does it return a new DF? Yes -> transformation.`
2. `Does it return a value or write data? Yes -> action.`
3. `Is it triggering computation? Yes -> action.`

