from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import MapType, StringType

spark_session = SparkSession.builder.appName('from_json').getOrCreate()

jsonString = """{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""
df = spark_session.createDataFrame([(1, jsonString)], ["id", "value"])
df.show(truncate=False)

json_schema = MapType(StringType(), StringType())
# df2 = df.select(from_json(df.value, "MAP<STRING, STRING>").alias("JSON"))
df2 = df.select(from_json(df.value, json_schema).alias("JSON"))
df2.printSchema()
df2.show(truncate=False)
