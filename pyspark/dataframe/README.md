# Practice with PySpark DataFrame

## Introduction
This repository contains code snippets for practicing PySpark DataFrame operations using the `pyspark.sql` module.

## Getting Started
Make sure you have a working PySpark environment set up. You can install PySpark using pip:

```bash
pip install pyspark
```

## Code Description
The code in this repository demonstrates various DataFrame operations in PySpark.

### Reading Data
The code reads data from a CSV file named `sample_data.csv` into a PySpark DataFrame.

```
from pyspark.sql import SparkSession

sparkSession = SparkSession.builder.appName('DataFrame').getOrCreate()
df = sparkSession.read.csv('sample_data.csv', header=True, inferSchema=True)
```

### Displaying DataFrame
The DataFrame structure and content are displayed using the `show()` and `printSchema()` methods.

```
df.show()
df.printSchema()
```

### Selecting Columns
Columns are selected using the `select()` method.

```
df.select(['Name', 'age']).show()
```

### Describing Data
Data description is obtained using the `describe()` method.

```
print(df.describe())
df.describe().show()
```

### Adding, Dropping, and Renaming Columns
Columns are added, dropped, and renamed using appropriate methods.

```
# Adding Column
df = df.withColumn('Experience after 2 years', df['Experience'] + 2)

# Dropping Column
df = df.drop('Experience after 2 years')

# Renaming Column
df = df.withColumnRenamed('age', 'new age')
```

### 1. Dataframe - RDD Example

Created a SparkSession
created a list of numbers
created a RDD using parallelize method

Created a DataFrame using above created RDD

### 2. dataframe - JSON Example

read JSON data into a PySpark DataFrame and perform basic operations like filtering.


# PySpark DataFrame Functions

PySpark provides a variety of DataFrame functions to perform operations on DataFrames efficiently.

## count()

Counts the number of rows in the DataFrame.

## select()

Selects one or more columns from the DataFrame.

## filter() & where()

Filters rows from the DataFrame based on a condition.

## like()

Filters rows based on a SQL-like pattern.

## describe()

Computes statistics for numeric and string columns.


## columns()

Returns a list of column names in the DataFrame.

## when() & otherwise()

Allows conditional processing when used in conjunction with `withColumn()`.

## alias()

Aliases a column in the DataFrame.

## orderBy() & sort()

Sorts the DataFrame by one or more columns.

## groupBy() & groupBy agg()

Groups the DataFrame using specified columns and allows aggregation.

