from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Read CSV with Schema") \
    .getOrCreate()

# Define the schema
schema = StructType([
    StructField("Name", StringType(), nullable=True),
    StructField("Age", IntegerType(), nullable=True),
    StructField("Experience", StringType(), nullable=True)
])

# Path to the CSV file
csv_path = r"sample_data.csv"

# Read the CSV file with the specified schema
df = spark.read.csv(csv_path, schema=schema)

# Show the DataFrame
df.show()

