from pyspark.sql import SparkSession

sparkSession = SparkSession.builder.appName('Json-Example').getOrCreate()

df_json = sparkSession.read.json('sample.json')

df_json.show()
df_json.printSchema()

# using filter in DataFrame
df_json.filter(df_json["age"] < 25).show()