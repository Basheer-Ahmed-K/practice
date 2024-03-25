# RDD Operations

## Introduction
This README provides an overview of how to work with Resilient Distributed Datasets (RDDs) in Apache Spark. RDDs are the fundamental data structure in Spark, providing an immutable distributed collection of objects.

## Starting Spark Session
To create an RDD, we first need to start a Spark session, which serves as the entry point for Spark functionality. We can achieve this by importing the `SparkSession` module and initializing a Spark session with a specified name using the `appName` method.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDD-Test").getOrCreate()
```

## Creating RDDs
RDDs can be created from existing RDDs or by arallelizing a collection of data. We can parallelize a list of numbers or a list of tuples to create RDDs.

## RDD Actions
Actions in Spark are operations that trigger computation and return results to the driver program. Here are some common RDD actions:

- `collect()`: Returns all the elements of the RDD to the driver program.
- `count()`: Returns the count of total elements in the RDD.
- `first()`: Returns the first element in the RDD.
- `take(n)`: Returns the first n elements from the RDD.
- `foreach(func)`: Applies a function to each element of the RDD. 
- `count()`: Returns the number of elements in the RDD.
- `min()`: Returns the minimum element in the RDD.
- `max()`: Returns the maximum element in the RDD.
- `mean()`: Returns the mean (average) of the elements in the RDD.
## RDD Transformations
Transformations in Spark are operations that transform one RDD into another RDD. Here are some common RDD transformations:

- `map(func)`: Applies a function to each element of the RDD.
- `filter(func)`: Filters elements of the RDD based on a predicate function.
- `reduceByKey(func)`: Combines values with the same key.
- `sortBy(func)`: Sorts the RDD by the given function.
- `intersection()`: Computes the intersection of two RDDs.
- `distinct()`: Returns a new RDD containing distinct elements.
- `groupByKey()`: Groups the values for each key in the RDD.
- `flatMap()`: Similar to map, but each input item can be mapped to 0 or more output items. (so func should return a seq rather than a single item)
- `mapPartitions()`: Similar to map, but runs separately on each partition of the RDD.
- `mapPartitionsWithIndex()`: Similar to mapPartitions(), but provides the index of the partition.
- `glom()`: Returns an RDD created by coalescing all elements within each partition into a list.
- `union()`: Returns the union of two RDDs.

