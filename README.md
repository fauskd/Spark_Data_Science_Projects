# PySpark A to Z

A practical, notebook-based introduction to **Apache Spark with PySpark**.  
This project walks through PySpark fundamentals and progressively moves into DataFrames, SQL operations, window functions, joins, UDFs, and data transformations.

The main learning material is contained in [`PySpark_A_to_Z.ipynb`](./PySpark_A_to_Z.ipynb).

## 📚 Topics Covered

The notebook covers the following PySpark concepts:

### 1. PySpark & RDD Basics
- Installing and checking the PySpark version
- Creating RDDs with `SparkContext`
- Creating RDDs from:
  - Python collections
  - Text files
  - Ranges
  - Existing RDDs
  - Tuples
- RDD transformations and actions
- `map()`
- `filter()`
- `flatMap()`
- `collect()`
- `count()`
- `reduce()`

### 2. PySpark DataFrames
- Creating DataFrames from Python data
- Defining an explicit DataFrame schema
- Using `StructType` and `StructField`
- Converting RDDs to DataFrames
- Inspecting DataFrame schemas with `printSchema()`

### 3. Reading CSV Data
- Reading CSV files with `spark.read.csv()`
- Using headers
- Inferring schemas
- Reading CSV files with custom delimiters such as `;`

### 4. DataFrame & SQL Operations
Examples of common DataFrame operations, including:

- Column aliases with `alias()`
- Sorting with `asc()` and `desc()`
- Data type conversion with `cast()`
- Range filtering with `between()`
- String matching with `contains()`
- `startswith()` and `endswith()`
- Null checking with `isNotNull()`
- Extracting substrings with `substr()`

### 5. PySpark SQL Functions
The notebook demonstrates:

- Conditional logic with `when()` and `otherwise()`
- String functions:
  - `concat_ws()`
  - `upper()`
  - `lower()`
  - `length()`
  - `substring()`
- Numeric functions:
  - `round()`
  - Salary calculations
- Aggregate functions:
  - `count()`
  - `avg()`
  - `max()`
  - `sum()`

### 6. Filtering Data
Examples include:

- Numeric comparisons
- String comparisons
- `AND` conditions with `&`
- `OR` conditions with `|`
- `NOT` conditions with `~`
- Combining `filter()`, `select()`, and `sort()`

### 7. Window Functions
The notebook demonstrates window-based analytics using:

- `Window.partitionBy()`
- `Window.orderBy()`
- `rank()`
- `dense_rank()`
- `row_number()`
- `avg()` over a window
- `lag()`

Example use cases include:

- Finding top earners by country
- Comparing employee salaries with country averages
- Comparing an employee's salary with another salary in the ordered window

### 8. Joining DataFrames
Examples include:

- Inner joins
- Left outer joins
- Broadcast joins

A small country metadata DataFrame is used to demonstrate joining employee data with currency and tax-rate information.

### 9. Advanced Column Transformations & UDFs
The notebook demonstrates:

- Complex conditional column logic
- Creating full names from multiple columns
- Creating seniority categories
- Python User Defined Functions (UDFs)
- Creating salary bands with a custom Python function

The notebook also demonstrates native PySpark column expressions as the preferred approach when they can accomplish the required transformation.

### 10. PySpark SQL Expressions
The final section shows how to:

- Create a temporary SQL view with `createOrReplaceTempView()`
- Run SQL queries through `spark.sql()`
- Use `WHERE`
- Use `GROUP BY`
- Use `HAVING`
- Use `COUNT()`
- Use `AVG()`
- Use `ROUND()`
- Use `ORDER BY`

---

## 🗂️ Project Structure

```text
.
├── PySpark_A_to_Z.ipynb
└── README.md
```

The notebook may also reference local CSV/text datasets used for practice, such as employee data and SQL examples.

## ⚙️ Requirements

- Python 3.x
- Apache Spark / PySpark
- Jupyter Notebook or JupyterLab

### Install PySpark

```bash
pip install pyspark
```

Verify the installation:

```python
import pyspark
print(pyspark.__version__)
```

## 🚀 Getting Started

### 1. Clone the repository

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
cd <YOUR_PROJECT_DIRECTORY>
```

### 2. Install PySpark

```bash
pip install pyspark
```

### 3. Start Jupyter Notebook

```bash
jupyter notebook
```

### 4. Open the notebook

Open:

```text
PySpark_A_to_Z.ipynb
```

Run the cells sequentially to follow the examples from basic RDD operations through PySpark SQL and advanced transformations.

Before running the file-based examples, replace them with paths that exist on your system. For example:

```python
df = spark.read.csv(
    "data/employees.csv",
    header=True,
    inferSchema=True
)
```

A recommended repository layout is:

```text
.
├── data/
│   ├── sample.txt
│   ├── employees.csv
│   ├── employees_delimiter.csv
│   └── data_for_sql.csv
├── PySpark_A_to_Z.ipynb
└── README.md
```

If these datasets are not included in the repository, the corresponding file-reading cells will need to be updated or skipped.

## 🧪 Example

Create a simple DataFrame:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PySparkExample") \
    .getOrCreate()

data = [
    ("Alice", 28, "Engineering"),
    ("Bob", 35, "Marketing"),
    ("Charlie", 22, "HR")
]

columns = ["Name", "Age", "Department"]

df = spark.createDataFrame(data, columns)

df.show()
```

Example output:

```text
+-------+---+-----------+
|   Name|Age| Department|
+-------+---+-----------+
|  Alice| 28|Engineering|
|    Bob| 35|  Marketing|
|Charlie| 22|         HR|
+-------+---+-----------+
```

## 🔎 Example: Data Filtering

```python
from pyspark.sql.functions import col

df.filter(col("Age") > 25).show()
```

Multiple conditions can be combined using:

```python
df.filter(
    (col("Age") >= 25) &
    (col("Department") == "Engineering")
).show()
```

## 🪟 Example: Window Function

The notebook uses window functions to rank employees within each country:

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

window_spec = Window \
    .partitionBy("country") \
    .orderBy(col("salary").desc())

df.withColumn(
    "rank",
    rank().over(window_spec)
).show()
```

## 🔗 Example: DataFrame Join

```python
df.join(
    country_df,
    on="country",
    how="inner"
).show()
```

The notebook also demonstrates broadcast joins for small lookup DataFrames.

## 🧠 Learning Goals

By working through this notebook, you can practice:

- Understanding the difference between RDDs and DataFrames
- Creating and inspecting Spark DataFrames
- Reading structured data with Spark
- Transforming and filtering large datasets
- Using built-in PySpark SQL functions
- Performing aggregations
- Applying window functions
- Joining DataFrames
- Working with temporary SQL views
- Writing SQL queries with Spark
- Understanding when custom UDFs can be used

## 🛠️ Technologies

- **Python**
- **Apache Spark**
- **PySpark**
- **Spark SQL**
- **Jupyter Notebook**

## 📌 Notes

This repository is primarily intended as a **learning and reference project** for practicing PySpark concepts. The examples use employee-style datasets and progressively demonstrate common Spark transformations and analytical operations.

For the best learning experience, run the notebook from top to bottom and experiment with the examples by changing the input data, filters, transformations, and SQL queries.


---

⭐ If you find this notebook useful for learning PySpark, consider starring the repository.
