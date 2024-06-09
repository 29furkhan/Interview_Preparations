
##### **explode():** ^852f8d

> `1. Transforms array column into a new dataframe with one element per row. Returns a new column object with desired result.`
>
> `2. If a column with an array of 2 values is exploded, we get 2 rows 1 array value each in a new column.`
> 
> `3. NULLs?`
> - If an array has a NULL value, will be exploded.
> - If an array is NULL, then row would be skipped.
> 
> `Ex. Data Before Explode`
>
|  **Name** |  **Numbers** |
> | --- | --- |                                                         
> |  Alice |   [1, 2, 3] |
> |    Bob|      [4, 5]|
|Charlie|[6, 7, 8, 9]|
> Queries
>```
>df = df.withColumn("new_column", explode(df["numbers"]))
> df = df.drop(df["numbers"]) #Deleting the array column
> ```
> Result 
> ```
>df.show()
+-------+----------+
|   name|new_column|
+-------+----------+
|  Alice|         1|
|  Alice|         2|
|  Alice|         3|
|    Bob|         4|
|    Bob|         5|
|Charlie|         6|
|Charlie|         7|
|Charlie|         8|
|Charlie|         9|
+-------+----------+
>```
****
##### split(): ^b2be27

> `1. Available in pyspark.sql.functions.`
> `2. Inputs:`
> * string: Input string to be splitted.
> * pattern: pattern defining separator.
> 
> `3. Returns a column with Array of values. Individual array element can be accessed by getItem() method.`
> 
> `Ex. Data Queries`
>```
>data = [("Alice", "19-01-1999"), ("Fuki", "29-07-2001"), ("Duke", None)]
> df = spark.createDataFrame(data, schema=["Name", "DOB"])
>```
>`Data Before Split`
>
>```
+-----+----------+
| Name|       DOB|
+-----+----------+
|Alice|19-01-1999|
| Fuki|29-07-2001|
| Duke|      NULL|
+-----+----------+
>```
> Queries
>```
df = df.withColumn("Date", split(df.DOB, "-").getItem(0))
df = df.withColumn("Month", split(df.DOB, "-").getItem(1))
df = df.withColumn("Year", split(df.DOB, "-").getItem(2))
>```
>Result
>```
>+-----+----------+----+----+-----+
| Name|       DOB|Year|Date|Month|
+-----+----------+----+----+-----+
|Alice|19-01-1999|1999|  19|   01|
| Fuki|29-07-2001|2001|  29|   07|
| Duke|      NULL|NULL|NULL| NULL|
+-----+----------+----+----+-----+
>```
>More Queries
>```
>#1
>split_col = split(df.DOB, "-")
>df.select(split_col.getItem(2).alias("Year")).show()
>```
>```
+----+
|Year|
+----+
|1999|
|2001|
|NULL|
+----+
>```
****
##### coalesce(): ^e1d316

> `1. Available in pyspark.sql.functions.`
> `2. Inputs:`
> * n number of input column objects.
> 
> `3. Returns a first non-null value from a given input columns.`
> 
> `Ex. Data Queries`
>```
>df = spark.createDataFrame([(None, None), (1, None), (None, 2)], ("a", "b"))
>```
>`Data Before Coalesce`
>
>```
+----+----+
|   a|   b|
+----+----+
|NULL|NULL|
|   1|NULL|
|NULL|   2|
+----+----+
>```
> Queries
>```
>column = coalesce(df["a"], df["b"], lit("Default")).alias("Result")
>df.select("*", column).show()
>```
>Result
>```
>+----+----+-------+
|   a|   b| Result|
+----+----+-------+
|NULL|NULL|Default|
|   1|NULL|      1|
|NULL|   2|      2|
+----+----+-------+
>```
>****
##### when(): ^edf666

> `1. Available in pyspark.sql.functions.`
> `2. Inputs:`
> * Condition
> * Value if true
> 
> `3. Returns value if condition is true, else returns the value from otherwise as column object.`
> 
> `Ex. Data Queries`
>```
>data = [("Alice", 17), ("Bob", 20), ("Charlie", 15)]
>df = spark.createDataFrame(data, ["name", "age"])
>```
>`Data Before Conditions`
>
>```
+-------+---+
|   name|age|
+-------+---+
|  Alice| 17|
|    Bob| 20|
|Charlie| 15|
+-------+---+
>```
> Queries
>```
># 1. Simple example
>from pyspark.sql.functions import coalesce, lit, col
>df = df.withColumn("classification",
>	when(col("age") < 18, "Minor").otherwise("Adult")
)
df.show()
>```
>Result
>```
+-------+---+--------------+
|   name|age|classification|
+-------+---+--------------+
|  Alice| 17|         Minor|
|    Bob| 20|         Adult|
|Charlie| 15|         Minor|
+-------+---+--------------+
>```
>```
># 3. Multiple conditions (if-else-ladder)
>data = [("Product A", 500),("Product B", 1200),("Product C", 300),("Product D", 800),("Product E", 1500)]
df = spark.createDataFrame(data, ["product", "sales_amount"])
># Logic
df = df.withColumn(
"category",
when(col("sales_amount") < 500, "Low Sales")
.when((col("sales_amount") >= 500) & (col("sales_amount") <= 1000), "Medium Sales")
.when(col("sales_amount") > 1000, "High Sales")
.otherwise("Unknown"))
>```
>Result
>```
+---------+------------+------------+
|  product|sales_amount|    category|
+---------+------------+------------+
|Product A|         500|Medium Sales|
|Product B|        1200|  High Sales|
|Product C|         300|   Low Sales|
|Product D|         800|Medium Sales|
|Product E|        1500|  High Sales|
+---------+------------+------------+
>```
>```
># 2. Nested conditions
>data = [("Product A", 500),("Product B", 1200),("Product C", 300),("Product D", 800),("Product E", 1500)]
df = spark.createDataFrame(data, ["product", "sales_amount"])
># Logic
>df = df.withColumn(
  "result",
when(col("sales_amount") > 0,  # Outer condition
   when(col("sales_amount") % 2 == 0, "Positive Even") 
   .otherwise("Positive Odd")
 ).otherwise("Non-positive")
)
>```
>Result
>```
+---------+------------+-------------+
|  product|sales_amount|       result|
+---------+------------+-------------+
|Product A|         500|Positive Even|
|Product B|        1200|Positive Even|
|Product C|         300|Positive Even|
|Product D|         800|Positive Even|
|Product E|        1500|Positive Even|
+---------+------------+-------------+
>```
>****

##### agg(): ^2d373f

> Description
>
> `1. Can be used with pyspark.groupBy. groupBy() returns the aggregate GroupedData class object that has agg() method to perform many agg functions.`
> `2. We have sum(), avg(), min(), max().`
> `3. Used to perform data aggregation and summarization. `
> `4. Returns the aggregated data.`
> 
Data Queries
>```python
># Create Sample DataFrame
simpleData = [("James","Sales","CA",10000),
("Michael","Sales","NY",20000),
("Robert","Sales","CA",23000),
("Maria","Finance","CA",23000),
("Raman","Finance","CA",24000),
("Scott","Finance","NY",19000),
("Jen","Finance","NY",15000)
]
schema = ["employee_name","department","state","salary"]
df = spark.createDataFrame(data=simpleData, schema = schema)
>```
Data Before Conditions
>
>```
+-------------+----------+-----+------+
|employee_name|department|state|salary|
+-------------+----------+-----+------+
|        James|     Sales|   CA| 10000|
|      Michael|     Sales|   NY| 20000|
|       Robert|     Sales|   CA| 23000|
|        Maria|   Finance|   CA| 23000|
|        Raman|   Finance|   CA| 24000|
|        Scott|   Finance|   NY| 19000|
|          Jen|   Finance|   NY| 15000|
+-------------+----------+-----+------+
>```
> Queries
>```python
># Example 1 - Count department wise.
df.groupBy("department") \
.agg(count("*").alias("count")) \
.show(truncate=False)
>```
>```python
># Example 2 - Multiple agg operations.
df.groupBy("department") \
.agg(
sum("salary").alias("sum_salary"), \
avg("salary").alias("avg_salary"), \
min("salary").alias("minimum_salary"), \
max("salary").alias("max_salary") \
).show(truncate=False)
>```
>```python
># Example 3 - Where in agg
df.groupBy("department") \
.agg(
sum("salary").alias("sum_salary"), \
avg("salary").alias("avg_salary"), \
min("salary").alias("minimum_salary"), \
max("salary").alias("max_salary") \
) \
.where(col("avg_salary") > 1500) \
.show(truncate=False)
>```
Result
> ```
> # 1
> +----------+-----+
|department|count|
+----------+-----+
|Sales     |3    |
|Finance   |4    |
+----------+-----+
> # 2 and # 3
> +----------+----------+------------------+--------------+----------+
|department|sum_salary|avg_salary        |minimum_salary|max_salary|
+----------+----------+------------------+--------------+----------+
|Sales     |53000     |17666.666666666668|10000         |23000     |
|Finance   |81000     |20250.0           |15000         |24000     |
+----------+----------+------------------+--------------+----------+
> ```
****
##### row_number(): ^04cbdc

> Description
>
> `1. window function gives the sequential row number starting from 1 to the result of each window partition.`
> `2. Does not account for ties; each row receives a unique row number, even if there are ties in the ordering columns.`
> ```python
> from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank
> ```
> 
Data Queries
>```python
>data = [("T1", "20231101", "2000"), ("T1", "20231201", "1200"), ("T2", "20230101", "1000"), ("T2", "20230101", "1000")]
df = spark.createDataFrame(data, schema=["transaction_id", "date_of_payment", "amount"])
>```
Data Before Conditions
>
>```
># Observe T2, has a tie of date.
>+--------------+---------------+------+
|transaction_id|date_of_payment|amount|
+--------------+---------------+------+
|            T1|       20231101|  2000|
|            T1|       20231201|  1200|
|            T2|       20230101|  1000|
|            T2|       20230101|  1000|
+--------------+---------------+------+
>```
> Queries
>```python
>window_fn = Window.partitionBy("transaction_id").orderBy(df["date_of_payment"].desc())
df.select("*",
row_number().over(window_fn).alias("row_number") \
).show(truncate=False)
>```
Result
> ```
> # Observe T2 got a value randomly for a tie.
> +--------------+---------------+------+----------+
|transaction_id|date_of_payment|amount|row_number|
+--------------+---------------+------+----------+
|T1            |20231201       |1200  |1         |
|T1            |20231101       |2000  |2         |
|T2            |20230101       |1000  |1         |
|T2            |20230101       |1000  |2         |
+--------------+---------------+------+----------+
> ```
****

##### rank(): ^23ec05

> Description
>
> `1. window function provides a rank to an input within given partition.`
> `2. Leaves gap in ties. Meaning if 3 values are duplicate, and ordering column same for 2, then rank would be 1, 1, 3.`
> ```python
> from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank
> ```
> 
Data Queries
>```python
>data = [("T1", "20231101", "2000"), ("T1", "20231201", "1200"), ("T2", "20230101", "1000"), ("T2", "20230101", "1000"), ("T2", "20230102", "1000")]
df = spark.createDataFrame(data, schema=["transaction_id", "date_of_payment", "amount"])
>```
Data Before Conditions
>
>```
># Observe T2, has a tie of date.
>+--------------+---------------+------+
|transaction_id|date_of_payment|amount|
+--------------+---------------+------+
|            T1|       20231101|  2000|
|            T1|       20231201|  1200|
|            T2|       20230101|  1000|
|            T2|       20230101|  1000|
|            T2|       20230102|  1000|
+--------------+---------------+------+
>```
> Queries
>```python
>window = Window.partitionBy("transaction_id").orderBy(df.date_of_payment.asc())
df.select("*",
rank().over(window).alias("rank") \
).show()
>```
Result
> ```
> # Observe the T2 rank#3, it skipped 2 (gap)
> +--------------+---------------+------+----+
|transaction_id|date_of_payment|amount|rank|
+--------------+---------------+------+----+
|            T1|       20231101|  2000|   1|
|            T1|       20231201|  1200|   2|
|            T2|       20230101|  1000|   1|
|            T2|       20230101|  1000|   1|
|            T2|       20230102|  1000|   3|
+--------------+---------------+------+----+
> ```
****
##### dense_rank(): ^78d32c

> Description
>
> `1. window function provides a rank to an input within given partition.`
> `2. Leaves no gap in ties. Meaning if 3 values are duplicate, and ordering column same for 2, then rank would be 1, 1, 2.`
> ```python
> from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank
> ```
> 
Data Queries
>```python
>data = [("T1", "20231101", "2000"), ("T1", "20231201", "1200"), ("T2", "20230101", "1000"), ("T2", "20230101", "1000"), ("T2", "20230102", "1000")]
df = spark.createDataFrame(data, schema=["transaction_id", "date_of_payment", "amount"])
>```
Data Before Conditions
>
>```
># Observe T2, has a tie of date.
>+--------------+---------------+------+
|transaction_id|date_of_payment|amount|
+--------------+---------------+------+
|            T1|       20231101|  2000|
|            T1|       20231201|  1200|
|            T2|       20230101|  1000|
|            T2|       20230101|  1000|
|            T2|       20230102|  1000|
+--------------+---------------+------+
>```
> Queries
>```python
>window = Window.partitionBy("transaction_id").orderBy(df.date_of_payment.asc())
df.select("*",
dense_rank().over(window).alias("rank") \
).show()
>```
Result
> ```
> # Observe the T2 rank#2, nothing skipped
> +--------------+---------------+------+----+
|transaction_id|date_of_payment|amount|rank|
+--------------+---------------+------+----+
|            T1|       20231101|  2000|   1|
|            T1|       20231201|  1200|   2|
|            T2|       20230101|  1000|   1|
|            T2|       20230101|  1000|   1|
|            T2|       20230102|  1000|   2|
+--------------+---------------+------+----+
> ```
****

##### repartition(): ^dc8e36

> Description
>
> `- Specify the no. of partitions, column you want, it will evenly distribute the data across partitions.`
> 
>Query
>```python
># no.of part, part_col
>df.repartition(10, "city")
>df.select(spark_partition_id().alias("part_id")).groupBy("part_id").count().show()
>```
>Result
> ```
> +-------+-----+
|part_id|count|
+-------+-----+
|      0|    2|
|      1|    2|
|      2|    2|
|      3|    4|
|      4|    2|
|      5|    2|
|      6|    2|
|      7|    4|
+-------+-----+
> ```
****

##### createOrReplaceTempView():
#Task
##### unionByName():
`Usage`
- `Used to merge or union 2 DFs based on the column names, rather than positions.`
- `Function is realy handy;`
	- `When you have DFs with same columns but in different orders.`
	- `When missing columns.`
- `It allows the union even if the DFs have different number of columns.`
- `allowMissingColumns - Optional param. Allows you merge the dataframes with different number or missing columns.`

`Example`
```Python
# Preapre Data
data1 = [("James", 34, "M"), ("Michael", 56, "M"), ("Robert", 30, "M"), ("Maria", 24, "F")]
data2 = [(34, "James", "Engineer"), (45, "Maria", "Doctor"), (45, "Jen", "Lawyer"), (34, "Jeff", "Teacher")]

# Prepare DFs
df1 = spark.createDataFrame(data=data1, schema=["name", "id", "gender"])
df2 = spark.createDataFrame(data=data2, schema=["id", "name", "profession"])

df3 = df1.unionByName(df2, allowMissingColumns=True)
df3.show()
```

```SQL
/* Output of union */
+-------+---+------+----------+
|   name| id|gender|profession|
+-------+---+------+----------+
|  James| 34|     M|      NULL|
|Michael| 56|     M|      NULL|
| Robert| 30|     M|      NULL|
|  Maria| 24|     F|      NULL|
|  James| 34|  NULL|  Engineer|
|  Maria| 45|  NULL|    Doctor|
|    Jen| 45|  NULL|    Lawyer|
|   Jeff| 34|  NULL|   Teacher|
+-------+---+------+----------+
```
##### union():
`Usage`
- `Used to merge or vertically append the data of both the DFs.`
- `Function is realy handy;`
	- `When you have DFs with same number of columns or similar schema.`
- `It checks for positions rather than names of columns.`
- `It is possible if both DFs have same number of columns (different names) it will still allow append.`

`Example`
```Python
# Data and Schema
employee = [('E101', "Furkhan", 2000), ('E102', "Rehan", 3000)]
employee_cols = ["EID", "EName", "Salary"]
department = [('D101', "HR", 100000), ('D102', "Finance", 500000)]
dept_cols = ["DeptId", "DeptName", "Assets(.cr)"]

#DataFrames.
e_df = spark.createDataFrame(data=employee, schema=employee_cols)
d_df = spark.createDataFrame(data=department, schema=dept_cols)

#Union
e_df.union(d_df).show()
```

```SQL
/* Output of union */
+----+-------+------+
| EID|  EName|Salary|
+----+-------+------+
|E101|Furkhan|  2000|
|E102|  Rehan|  3000|
|D101|     HR|100000|
|D102|Finance|500000|
+----+-------+------+
# Now the union does not make sense, it's proved that union checks orders and not columns.
```
### `Date and time functions` ^9ac54d
`1. Default format is yyyy-MM-dd.`
`2. Default timestamp format is yyyy-MM-dd HH:mm:ss.SSSS`
****
##### current_date(): ^f0e9b8

Description
>
> `1. Gets you current system date in YYYY-dd-mm.`
> `2. Note: You cannot use to_date with current_date, to_date expects input as string, however current_date gives col as output.`
> ```python
from pyspark.sql.functions import date_format, current_date
> ```

Data Queries
>```python
>df = spark.createDataFrame([("Furkhan", "Empty")], schema=["Name", "current_ts"])
>```

Query
>```python
>df.select("Name", date_format(current_date(), "dd-MMM-yy").alias("created_at")).show()
>```

Result
> ```
>+-------+----------+
|   Name|created_at|
+-------+----------+
|Furkhan| 23-Apr-24|
+-------+----------+
> ```
****

##### to_date() and date_format(): ^30caea

Description
>**to_date**():
>`1. Converts the input to date yyyy-MM-dd by defalt.`
>`2. Inputs: date_column, input_date_format of date column.`
>`Here the date column should match the 2nd param format else you get NULL in result.`
>`3. Converts the string column to DateType.`
>```python
>to_date(col, date_format_of_col)
>```
>**format_date():**
> `1. Coverts date/ timestamp/ string to a string value specified in given format. Need the date col string to be in yyyy-MM-dd format.`
> `2. Converts the input datetype/ string column into string of specified format.`
> `3. Inputs: date_column, output_date_format`
> ```python
>format_date(col, date_format_needed)
>```

Imports
> ```python
from pyspark.sql.functions import date_format, to_date
> ```

Data Queries
>```python
> df = spark.createDataFrame([("Furkhan", "29-07-1999")], schema=["Name", "DOB"])
>```

Query
>```python
># to_date and date_format used in one
>df.select("*",
to_date("DOB", "dd-MM-yyyy").alias("to_date_DOB"),
date_format("to_date_DOB", "dd-MMM-yyyy").alias("format_date_DOB") \
).show(truncate=False)
>```

Result
> ```
>+-------+----------+-----------+---------------+
|Name   |DOB       |to_date_DOB|format_date_DOB|
+-------+----------+-----------+---------------+
|Furkhan|29-07-1999|1999-07-29 |29-Jul-1999    |
+-------+----------+-----------+---------------+
> ```
> [More Date Functions ](https://sparkbyexamples.com/pyspark/pyspark-sql-date-and-timestamp-functions/)
****