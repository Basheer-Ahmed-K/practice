from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("action").getOrCreate()

numbers = [1, 2, 3, 4, 5]

# RDD ACTIONS

rdd = spark.sparkContext.parallelize(numbers)

print(rdd.collect())

sampleData = [('basheer', 50), ('google', 100), ('redmi', 23), ('google', 53)]

rdd = spark.sparkContext.parallelize(sampleData)

print(rdd.collect())

# return the count of total element in rdd
print("count of values in sample data: ", rdd.count())

# return the first element in rdd
print("first element from sample data: ", rdd.first())

# performing take() action and pass a number as param it will return the no of element
print("print 2 elements from rdd: ", rdd.take(2))

# performing foreach() action in rdd
print("Printing each element in rdd:")
rdd.foreach(lambda x: print(x))

top_two = rdd.map(lambda x: x[1])
print(top_two)
