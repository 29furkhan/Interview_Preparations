##### 1. Read the question and give a PySpark code.
**Question**

Explode the input table in such a way that you will achive the certain output.

![[2.png]]
**Solution**

Steps
>`1.  Hobbies is a string column values separated by ',' hence we cannot directly explode the Hobbies column.`
`2. Hence we need to split() and create array, then explode() to get the needed result.`

Functions
>1. [[Functions#^b2be27|split()]]
>2. [[Functions#^852f8d|explode()]]


Imports
> ```python
>from pyspark.sql.functions import split, explode
>```

Data
>```python
>data = [("Alice", "Singing,Dancing"), ("Fuki", "Cricket,Movies,Dance"), ("Duke", None)]
> df = spark.createDataFrame(data, schema=["Name", "Hobbies"])
>```

Code
> ```python
> df = df.withColumn("array_of_hobbies", split(df["Hobbies"], ","))
>df = df.withColumn("Hobbies", explode(df["array_of_hobbies"]))
>```

****
##### 2. Read the question and give a PySpark code.
**Question**

Resulting column should take the first non-null value of a tuple.

![[1.png]]

**Solution**

Steps
>`1. We have to find first non-null of 3 columns, so we can use 'coalesce()' method giving these columns as input.`
>`2. We should also handle empty strings.`

Functions
>[[Functions#^e1d316|coalesce()]]
>[[Functions#^edf666|when()]]

Imports
> ```python
>from pyspark.sql.functions import coalesce, when
>```

Data
>```python
> data = [("GOA", "", "ap"), ("", "AP", None), (None, "", "Bglr")]
> df = spark.createDataFrame(data, schema=["city1", "city2", "city3"])
>```

Code
> ```python
> # 1. Only handles NULL, not ""
>column = coalesce(df.city1, df.city2, df.city3).alias("Result")
>df.select(column).show()
>```
>```python
> # 2. Handles both NULL and ""
>column = coalesce(
>		when(df.city1 == "", None).otherwise(df.city1),
>		when(df.city2 == "", None).otherwise(df.city2),
>		when(df.city3 == "", None).otherwise(df.city3)
>).alias("Result") #Result is stored as column
>df.select(column).show()
>```
>

##### 3. Read the question and give a PySpark code.
**Question**

Generate the result shown in the image.

![[3.png]]
**Solution**

Steps
>`1. We have data in 2 dataframes connected with Student id.`
>`2. First dataframe has unique student details, second has subject scores and name details.`
>`3. Join the dataframes based on primary keys.`
>`4. Calculate average of marks and then categorize it using when, otherwise.` 
>

Functions
>[[Functions#^edf666|when()]]
>[[Functions#^2d373f|agg()]]

Imports
> ```python
>from pyspark.sql.functions import sum, count, when, col
>```

Data
>```python
> student_data = [(1,"Steve"), (2,"David"), (3, "Aryan")]
student_df = spark.createDataFrame(student_data, schema=["Student_ID", "Student_Name"])
subject_data = [(1,"PySpark", 90), (1, "SQL", 100), (2, "SQL", 70), (2, "PySpark", 60), (3, "SQL", 30), (3, "PySpark", 20)]
subject_df = spark.createDataFrame(subject_data, schema=["Student_ID", "Subject_Name", "Marks"])
>```

Table
> ```
student_df.show()
+----------+------------+
|Student_ID|Student_Name|
+----------+------------+
|         1|       Steve|
|         2|       David|
|         3|       Aryan|
+----------+------------+
subject_df.show()
+----------+------------+-----+
|Subject_ID|Subject_Name|Marks|
+----------+------------+-----+
|         1|     PySpark|   90|
|         1|         SQL|  100|
|         2|         SQL|   70|
|         2|     PySpark|   60|
|         3|         SQL|   30|
|         3|     PySpark|   20|
+----------+------------+-----+
>```

Code
> ```python
#Join the DFs and keep aside
joined_df = student_df.join(subject_df, on="Student_ID", how="inner").drop(subject_df.Student_ID)
#Find average marks
tmp_df = joined_df.groupBy("student_id","student_name").agg((sum("Marks")/count("Marks")).alias("Average"))
#Find category based on Average
final_df = tmp_df.select("*",
when(col("Average") > 70, "Distinction")
.when((col("Average") >=60) & (col("Average") < 70), "First Class")
.when((col("Average") >=50) & (col("Average") < 60), "Second Class")
.when((col("Average") >=40) & (col("Average") < 50), "Third Class")
.otherwise("Fail").alias("Result")
)
>```

Result
> ```
> +----------+------------+-------+-----------+
|student_id|student_name|Average|     Result|
+----------+------------+-------+-----------+
|         1|       Steve|   95.0|Distinction|
|         2|       David|   65.0|First Class|
|         3|       Aryan|   25.0|       Fail|
+----------+------------+-------+-----------+
> ```


##### 4. Read the question and give a PySpark code.
**Question**

Find department wise nth highest salary.

![[4.png]]

**Solution**

Steps
>`1. To find out nth highest/ lowest, we should assign a rank number to the salaries department wise.`
>`2. For 1st highest salary filter rank no to 1.`

Articles
> Read about Window functions here [[Functions#^2d373f|pyspark.sql.window]]

Imports
> ```python
>from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
>```

Data
>```python
> data = [
(101, "John Doe", 60000, "IT"),
(102, "Jane Smith", 55000, "Sales"),
(103, "Michael Johnson", 65000, "HR"),
(104, "Emily Brown", 58000, "IT"),
(105, "David Lee", 62000, "Sales"),
(106, "Sarah Garcia", 70000, "IT"),
(107, "Jennifer Martinez", 56000, "HR"),
(108, "Robert Wilson", 59000, "Sales"),
(109, "Olivia Taylor", 63000, "IT")
]
df = spark.createDataFrame(data, schema=["EmpID", "EmpName", "Salary", "Department"])
>```

Table
> ```
+-----+-----------------+------+----------+
|EmpID|          EmpName|Salary|Department|
+-----+-----------------+------+----------+
|  101|         John Doe| 60000|        IT|
|  102|       Jane Smith| 55000|     Sales|
|  103|  Michael Johnson| 65000|        HR|
|  104|      Emily Brown| 58000|        IT|
|  105|        David Lee| 62000|     Sales|
|  106|     Sarah Garcia| 70000|        IT|
|  107|Jennifer Martinez| 56000|        HR|
|  108|    Robert Wilson| 59000|     Sales|
|  109|    Olivia Taylor| 63000|        IT|
+-----+-----------------+------+----------+
>```

Code
> ```python
#Window function that creates partitions and order them
window_spec = Window.partitionBy("Department").orderBy(df["Salary"].desc())
#2nd highest - row_number assigns rank_number based on salary
df.select("*",
row_number().over(window_spec).alias("rank") \
).filter(col("rank") == 2).show()
>```

Result
> ```
> +-----+-----------------+------+----------+----+
|EmpID|          EmpName|Salary|Department|rank|
+-----+-----------------+------+----------+----+
|  107|Jennifer Martinez| 56000|        HR|   2|
|  109|    Olivia Taylor| 63000|        IT|   2|
|  108|    Robert Wilson| 59000|     Sales|   2|
+-----+-----------------+------+----------+----+
> ```

***
##### 5. Read the question and give a PySpark code.
**Question**

It has 2 problem statements.
1. Find the DeptName, MgrName, EmpName, Sal-Year, Sal-Month,Total_Monthly_Salary.
2. Find the 7th Highest salaries and show employee name.

Note: Do not use spark.sql("")

![[5.png]]

**Solution**

Steps
>`1. Add a new column 'new_sal_date' to YYYY-MM-DD format.`
>`2. Join employee and salary data on DeptId`
>`3. Since the Employee name and manager name in the same table, we will need to do self join.`
>`4. Once self join is done, group by and print results.`

Articles and functions
> [[Functions#^9ac54d|PySpark date and timestamp functions]]
> [[Fundamentals#^37a06b|Self Join in PySpark]]
> Read more about self join [](https://learnsql.com/blog/illustrated-guide-sql-self-join/)

Imports
> ```python
>from pyspark.sql.functions import to_date
>```

Data
>```python
># Cols
employee_cols = ['EmpId', 'EmpName', 'MgrId', 'DeptId', 'Sal-Date', 'Salary']
dept_cols = ['DeptId', 'DeptName']
> # Data
employee_data = [
("E001", "Alice", "E006", "D001", "01-04-23", 60000), # Alice's manager is Frank (E006)
("E002", "Bob", "E010", "D001", "01-04-23", 58000), # Bob's manager is Judy (E010)
("E003", "Charlie", "E004", "D002", "01-04-23", 62000), # Charlie's manager is David (E004)
("E004", "David", None, "D002", "01-04-23", 59000), # David is a manager, so his MgrId is None or empty
("E005", "Eve", None, "D003", "01-04-23", 61000), # Eve is a manager, so her MgrId is None or empty
("E006", "Frank", None, "D003", "01-04-23", 63000), # Frank is a manager, so his MgrId is None or empty
("E007", "Grace", "E010", "D001", "01-04-23", 57000), # Grace's manager is Judy (E010)
("E008", "Heidi", "E005", "D002", "01-04-23", 65000), # Heidi's manager is Eve (E005)
("E009", "Ivan", "E006", "D003", "01-04-23", 64000), # Ivan's manager is Frank (E006)
("E010", "Judy", None, "D001", "01-04-23", 56000), # Judy is a manager, so her MgrId is None or empty
]
department_data = [
("D001", "HR"), # Human Resources department
("D002", "Sales"), # Sales department
("D003", "IT") # IT department
]
#DFs 
e_df = spark.createDataFrame(employee_data, schema=employee_cols)
d_df = spark.createDataFrame(department_data, schema=dept_cols)
>```

Tables
> ```
+-----+-------+-----+------+--------+------+
|EmpId|EmpName|MgrId|DeptId|Sal-Date|Salary|
+-----+-------+-----+------+--------+------+
| E001|  Alice| E006|  D001|01-04-23| 60000|
| E002|    Bob| E010|  D001|01-04-23| 58000|
| E003|Charlie| E004|  D002|01-04-23| 62000|
| E004|  David| NULL|  D002|01-04-23| 59000|
| E005|    Eve| NULL|  D003|01-04-23| 61000|
| E006|  Frank| NULL|  D003|01-04-23| 63000|
| E007|  Grace| E010|  D001|01-04-23| 57000|
| E008|  Heidi| E005|  D002|01-04-23| 65000|
| E009|   Ivan| E006|  D003|01-04-23| 64000|
| E010|   Judy| NULL|  D001|01-04-23| 56000|
+-----+-------+-----+------+--------+------+
>
>+------+--------+
|DeptId|DeptName|
+------+--------+
|  D001|      HR|
|  D002|   Sales|
|  D003|      IT|
+------+--------+
>```

Code
> ```python
> # Step-1: Formatting date column
e_df = e_df.withColumn("new_sal_date", to_date("sal-date", "yy-MM-dd"))
#Step-2: Employee and Department tables joined.
joined_df = e_df.join(d_df, on="DeptId", how="inner")
#Step-3: Self join to obtain manager details
joined_df = joined_df.alias("one").join(joined_df.alias("two"),
col("one.MgrId") == col("two.EmpId"),
how="left") \
.select(
"one.DeptId",
"one.EmpId",
"one.DeptName",
"one.EmpName",
"one.new_sal_date",
col("two.EmpName").alias("ManagerName"),
"one.Salary")
#Step-4: Group and print the results
joined_df.groupBy("DeptName",
"EmpName",
"ManagerName",
"EmpId",
year("new_sal_date").alias("Year")).sum("salary").alias("Total_Salary").show()
>```
Result
> ```
> +-----+-----------------+------+----------+----+
|EmpID|          EmpName|Salary|Department|rank|
+-----+-----------------+------+----------+----+
|  107|Jennifer Martinez| 56000|        HR|   2|
|  109|    Olivia Taylor| 63000|        IT|   2|
|  108|    Robert Wilson| 59000|     Sales|   2|
+-----+-----------------+------+----------+----+
> ```

***


##### 6. Read the question and give a PySpark code.
**Question**

How to check data skew issue and how to solve it?

**Explanation**
>`1.Data skew is a situation where distribution of daa across partitions is highly uneven.`
>`2. Data skew can cause the performance issues. Node with the skewed partition can take more time, other node with non-skewed partition can take less and for remaining time it waits for other nodes to finish processing.`
>`3. When data is not balanced between workers we call it skewed.`
>`4. When you create df, automatically it can have more than 1 partition(s).`
>
>![[data skewness.png]]

How to find no.of partitions in a df?
 ```python
 df.rdd.getNumParitions()
 ```
How to specify custom no. of partitions?
 ```python
 df.repartition(10, "city")
 ```
 No. of rows in each partitions, how to check?
 ```python
 # Gives partition_id of each one of the row in data
df = df.withColumn("part_id",spark_partition_id().alias("Partition_Number"))
# Shows no.of of rows in each partition
df.groupBy("part_id").count().show()
 ```
Result
```
+-------+-----+
|part_id|count|
+-------+-----+
|      0|    2|
|      1|    2|
|      2|    2|
|      3|    4|
|      4|    4|
+-------+-----+
```

Articles and functions
> [[Functions#^dc8e36|repartition()]]

Imports
> ```python
>from pyspark.sql.functions import spark_partition_id
>```
***
##### 7. Read the question and give a PySpark code.
**Question**

How to process the data of files those are received before and after specified time?

**Solution**
>`1. modifiedBefore: Read files modified before given TS.`
>`2. modifiedAfter: Read files modified after given TS.`
>
>```python
df = spark.read.load("path/to/dir",
modifiedBefore="2024-07-01T08:30:00")
>
df = spark.read.load("path/to/dir",                   modifiedBefore="2024-07-01T08:30:00")
>```
****
##### 8. Read the question and give a PySpark code.

**Methods**
1. [[Functions#union()|union()]]
2. [[Functions#unionByName()|unionByName()]]
3. unionAll()

**Question**
`Given two dataframes, merge them into one.`
```Python
#Sample data
sales_data = [(1, 101, 1001, 5, 100.50), (2, 102, 1002, 3, 75.25)]

customer_data = [(1001, "Alice", "alice@example.com", "1234567890", "123 Main St"), (1002, "Bob", "bob@example.com", "9876543210", "456 Elm St")]

# DataFrames
df_sales = spark.createDataFrame(sales_data, ["transaction_id", "product_id", "customer_id", "quantity_sold", "sale_amount"])

df_customer = spark.createDataFrame(customer_data, ["customer_id", "customer_name", "customer_email", "customer_phone", "customer_address"])
```

`Input tables:`

![[Pasted image 20240609222006.png]]

**Solutions**

`Method 1: UnionByName`

```python
# Merge 2 dataframes into one.
df_res = df_sales.unionByName(df_customer, allowMissingColumns=True)
```

`Method 2: union (Observe the difference in result)`

```python
# Fill the customer columns with null in sales_data
for col in df_customer.columns:
	if col not in df_sales.columns:
		df_sales = df_sales.withColumn(col, lit(None))

# Fill the sales columns with null in customer_data
for col in df_sales.columns:
	if col not in df_customer.columns:
		df_customer = df_customer.withColumn(col, lit(None))

set(df_sales.columns) - set(df_customer.columns)
# returns empty set()

# Merge 2 dataframes into one.
df_sales.union(df_customer).show(truncate=False)
```
****