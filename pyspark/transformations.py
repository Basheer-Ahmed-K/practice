from pyspark.sql import SparkSession

# RDD TRANSFORMATION
# here I used "collect" which is an action

sparkSession = SparkSession.builder.appName('Transformation').getOrCreate()

sampleData = [('basheer', 20), ('ahmed', 34), ('ahmed', 54)]

rdd = sparkSession.sparkContext.parallelize(sampleData)

mapped_rdd = rdd.map(lambda x: (x[0].upper(), x[1]))
print("RDD with uppercase name: ", mapped_rdd.collect())

filter_rdd = rdd.filter(lambda x: x[1] > 30)
print("filtered rdd`s are: ", filter_rdd.collect())

reduced_rdd = rdd.reduceByKey(lambda x, y: x+y)
print("reduced rdd: ", reduced_rdd.collect())

# printing the top two elements in rdd
sorted_rdd = rdd.sortBy(lambda x: x[1], ascending=False)
print("sorted the value by key: ", sorted_rdd.take(2))
