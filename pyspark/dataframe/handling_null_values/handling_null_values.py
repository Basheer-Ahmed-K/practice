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
df.show(5, truncate=False)

# checking for specific column using filter
df.filter(df.product_name.isNull()).show()
# other ways
df.filter(col('a').isNull()).show()
df.filter("product_name IS NULL").show()

# checking for a specific column using select and filtering Null values
df.select(col('product_name')).filter(col('product_name').isNull()).show()
df.na.fill(value="0").show()
df.show()

# replacing Null for both int and string values
df.fillna(value=0).show()
df.fillna(value="nothing here").show()


# removes all rows with null values and returns the clean DataFrame
# drop parameters are "all", "any"

df.na.drop().show()
df.na.drop(how="any").show(truncate=False)
df.dropna().show()

# drop selected columns
df.na.drop(subset=["product_name", "profit"]).show(truncate=False)
