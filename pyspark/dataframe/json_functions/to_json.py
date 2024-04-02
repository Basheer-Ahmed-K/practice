from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json
from pyspark.sql.types import StructType, StructField, MapType, StringType, ArrayType, IntegerType

spark_session = SparkSession.builder.appName('from_json').getOrCreate()

# data = [("Basheer", {"Zipcode": "704", "ZipCodeType": "STANDARD", "City": "London", "State": "UK"})]
# df = spark_session.createDataFrame(data, ['name', 'address'])
# df.printSchema()
# df.show(truncate=False)
#
# # converting the json to string (MapType)
# string_df = df.withColumn("string_json", to_json(df.address))
# string_df.printSchema()
# string_df.show(truncate=False)


data1 = [("basheer", ('black', 'brown'))]
schema = StructType([
    StructField('name', StringType()),
    StructField("properties", StructType([StructField('hair', StringType()), StructField("eye", StringType())]))])

df1 = spark_session.createDataFrame(data1, schema)
df1.printSchema()
df1.show()

# converting json to string (StructType)
string_json = df1.withColumn("string", to_json(df1.properties))
string_json.printSchema()
string_json.show(truncate=False)
