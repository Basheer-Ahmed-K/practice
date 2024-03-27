# Practice with PySpark DataFrame

## Introduction
This repository contains code snippets for practicing PySpark DataFrame operations using the `pyspark.sql` module.

## Getting Started
Make sure you have a working PySpark environment set up. You can install PySpark using pip:

```bash
pip install pyspark
```

### Reading Data
The code reads data from a CSV file named `sample_data.csv` into a PySpark DataFrame.


### Displaying DataFrame
The DataFrame structure and content are displayed using the `show()` and `printSchema()` methods.


### Selecting Columns
Columns are selected using the `select()` method.


### Describing Data
Data description is obtained using the `describe()` method.


### Adding, Dropping, and Renaming Columns
Columns are added, dropped, and renamed using appropriate methods.


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


