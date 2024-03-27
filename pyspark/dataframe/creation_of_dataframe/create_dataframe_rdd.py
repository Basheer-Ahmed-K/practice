from pyspark.sql import SparkSession

# creating session
sparkSession = SparkSession.builder.appName('Dataframe-RDD').getOrCreate()

# list of numbers for creating RDD
numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9]

# creating RDD with the numbers list
rdd = sparkSession.sparkContext.parallelize(numbers).map(lambda x: (x, x * x * x))

print(rdd.collect())

# creating DataFrame from the above created RDD
dataframe1 = sparkSession.createDataFrame(rdd).toDF("key", "cube")
dataframe1.show()
