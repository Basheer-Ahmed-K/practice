from pyspark.sql import SparkSession
from pyspark.sql.functions import *

sparkSession = SparkSession.builder.appName('casting').getOrCreate()

sample_data = [('01/01/2023', "Burger", 5, "FALSE"),
               ('01/02/2023', "Hot Dog", 3, "TRUE"),
               ('01/03/2023', "Chicken Wing", 10, "FALSE"),
               ('01/04/2023', "Fries", 1, "TRUE")]
column = ["Date", "Item", "Amount", "IsDiscount"]
df = sparkSession.createDataFrame(sample_data, column)
df.printSchema()


# returns a new dataframe by adding or replacing the existing column that has same name
updated_df = df.withColumn('Date', to_date(col('Date'), 'M/d/yyy').alias('Date').cast('date'))
updated_df.printSchema()
