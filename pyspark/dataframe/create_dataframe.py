from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

sparkSession = SparkSession.builder.appName('create-dataframe').getOrCreate()

numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

df = sparkSession.createDataFrame(numbers, IntegerType())
df.show()