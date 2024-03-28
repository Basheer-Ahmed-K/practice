from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

ss = SparkSession.builder.appName('Handling Null Values').getOrCreate()

file_path = r'C:\Users\Basheer AhmedK\Desktop\Diggibyte\Pyspark\pyspark practice\pyspark\datasets\data_with_null.csv'

schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('product_name', StringType(), True),
    StructField('customer_name', StringType(), True),
    StructField('quantity', IntegerType(), True),
    StructField('discounted_price', FloatType(), True),
    StructField('sales', FloatType(), True),
    StructField('profit', IntegerType(), True),
    StructField('region', StringType(), True),
    StructField('category', StringType(), True),
    StructField('shipping_cost', FloatType(), True)
])

df = ss.read.csv(file_path, schema=schema, inferSchema=True)
df.show()

# compare the both mentioned column and returns the non-null value
coalesce_df = df.select('*', coalesce(col('product_name'), col('customer_name')))
coalesce_df.show()
