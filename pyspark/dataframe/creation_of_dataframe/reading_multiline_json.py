from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, LongType, DoubleType

spark_session = SparkSession.builder.appName('reading_multiline_json').getOrCreate()

path = r'C:\Users\Basheer AhmedK\Desktop\Diggibyte\Pyspark\pyspark practice\pyspark\datasets\multiline.json'
# schema = ['name', 'phoneNumber', 'email', 'address', 'userAgent', 'hexcolor']
schema = StructType([
    StructField("name", StringType(), True),
    StructField("phoneNumber", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StringType(), True),
    StructField("userAgent", StringType(), True),
    StructField("hexcolor", StringType(), True),

])
df = spark_session.read.json(path, schema=schema, multiLine=True)
df.printSchema()
df.show(truncate=False)
