from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Write Parquet File") \
    .getOrCreate()

# Sample data
data = [("Basheer", 30, "New York"),
        ("Krishna", 35, "Los Angeles"),
        ("Kuldeep", 25, "Chicago")]

# Define the schema
schema = StructType([
    StructField("Name", StringType(), nullable=False),
    StructField("Age", IntegerType(), nullable=False),
    StructField("City", StringType(), nullable=False)
])

# Create a DataFrame
df = spark.createDataFrame(data, schema)

# Path to save the Parquet file
parquet_path = "pyspark/dataframe/schema/prequet_file.parquet"

# Write the DataFrame to Parquet file
# df.write.mode("overwrite").parquet("sample_parquet")
df.write.format('parquet').mode('append').save('sample_parquet')

