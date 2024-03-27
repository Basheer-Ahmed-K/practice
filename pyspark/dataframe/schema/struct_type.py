from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

session = SparkSession.builder.appName('struct-type').getOrCreate()

schema_struct = StructType([
    StructField('name', StringType(), True),
    StructField('age', IntegerType(), True),
    StructField('experience', IntegerType(), True)
])
data = [('Basheer', 23, 2), ('Amit', 25, 3), ('kuldeep', 34, 2), ('Robot', 33, 3), ('Krishna', 30, 5), ('Tarun', 32, 5)]
df = session.createDataFrame(data, schema_struct)
df.show()
