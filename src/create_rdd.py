# to create a RDD we need to first start spark session
#  serves as an entry point for spark functionality

# to create a spark session we need to import the module

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDD-Test").getOrCreate()

# we can create RDD from existing rdd, or we can create from parallelize method

numbers = [1, 2, 3, 4, 5]

# RDD CREATION

rdd = spark.sparkContext.parallelize(numbers)

print(rdd.collect())

sampleData = [('basheer', 50), ('google', 100), ('redmi', 23), ('google', 53)]

rdd = spark.sparkContext.parallelize(sampleData)

print(rdd.collect())