from pyspark.sql import SparkSession

# Creating SparkSession
spark = SparkSession.builder.appName("RDD-Actions").getOrCreate()

# Sample data
numbers = [1, 2, 3, 4, 5]

# Creating RDD
rdd = spark.sparkContext.parallelize(numbers)

# RDD Actions

# count(): Returns the number of elements in the RDD
print("Count of elements:", rdd.count())

# min(): Returns the minimum element in the RDD
print("Minimum element:", rdd.min())

# max(): Returns the maximum element in the RDD
print("Maximum element:", rdd.max())

# mean(): Returns the mean (average) of the elements in the RDD
print("Mean of elements:", rdd.mean())

# Difference between map() and mapPartitions()

# map(): Applies a function to each element of the RDD
mapped_rdd = rdd.map(lambda x: x * 2)

# mapPartitions(): Applies a function to each partition of the RDD, providing an iterator over the elements in the
# partition
mapped_partitions_rdd = rdd.mapPartitions(lambda partition: [x * 2 for x in partition])

print("Result of map():", mapped_rdd.collect())
print("Result of mapPartitions():", mapped_partitions_rdd.collect())
