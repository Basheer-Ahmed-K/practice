from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, FloatType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Read CSV with Schema") \
    .getOrCreate()

# Define the schema
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

# Path to the CSV file
csv_path = r"C:\Users\Basheer AhmedK\Desktop\Diggibyte\Pyspark\pyspark practice\pyspark\resources\data_with_null.csv"

# Read the CSV file with the specified schema
df = spark.read.csv(csv_path, schema=schema)

# Show the DataFrame
df.show()

