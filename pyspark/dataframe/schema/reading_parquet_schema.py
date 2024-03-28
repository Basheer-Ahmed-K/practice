from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Read Parquet with Schema") \
    .getOrCreate()

# Define the schema
schema = StructType([
    StructField("name", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True),
    StructField("Experience", StringType(), nullable=True)
])

# Path to the Parquet file
parquet_path = "sample_parquet.parquet"

# Read the Parquet file with the specified schema
df = spark.read.parquet(parquet_path, schema=schema)

# Show the DataFrame
df.show()


