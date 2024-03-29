from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, round, avg, collect_list, collect_set, countDistinct, count, first, last

spark_session = SparkSession.builder.appName('aggregate_functions').getOrCreate()

path = r'C:\Users\Basheer AhmedK\Desktop\Diggibyte\Pyspark\pyspark practice\pyspark\datasets\sample_data.csv'

df = spark_session.read.option("header", True).option("inferSchema", True).format('csv').load(path)

# df.printSchema()
df.show()

# Aggregate Functions

# mean
df.select(round(mean(df.age), 2).alias("mean")).show()

# Average
df.select(avg(df.Experience).alias("Average")).show()

# collect_list -> returns a list of objects with duplicates.
df.select(collect_list('age')).show()

# collect_set -> returns a set of objects with duplicate elements eliminated.
df.select(collect_set('age')).show()

# countDistinct -> Returns a new ~pyspark.sql.Column for distinct count of col or cols.
df.select(countDistinct('age')).show()

# count -> returns the number of items in a group.
df.select(count(df.age)).show()

# first -> returns first value in groups
df.select(first('Name')).show()

# last -> returns last value in group
df.select(last('Name')).show()
