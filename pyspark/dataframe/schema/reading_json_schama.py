from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Read JSON with Schema") \
    .getOrCreate()

# Define the schema
schema = StructType([
    StructField("Id", IntegerType(), nullable=False),
    StructField("Name", StringType(), nullable=False),
    StructField("Age", IntegerType(), nullable=False)
])

# Path to the JSON file
json_path = r'''sample.json'''

# Read JSON file with schema
df = spark.read.schema(schema).json(json_path)

# Show the DataFrame
df.show()
