from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Read Parquet with Schema") \
    .getOrCreate()

# Define the schema
schema = StructType([
    StructField("Name", StringType(), nullable=True),
    StructField("Age", IntegerType(), nullable=True),
    StructField("City", StringType(), nullable=True),
    StructField("HasChildren", BooleanType(), nullable=True)
])

# Path to the Parquet file
parquet_path = "path/to/your/file.parquet"

# Read the Parquet file with the specified schema
df = spark.read.parquet(parquet_path, schema=schema)

# Show the DataFrame
df.show()


