from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp

spark = SparkSession.builder.appName('DateTime').config("spark.sql.legacy.timeParserPolicy", "LEGACY").getOrCreate()

data = [("2022-03-15", "2022-03-16 12:34:56.789"),
        ("2022-03-01", "2022-03-16 01:23:45.678")]
df = spark.createDataFrame(data, ["date_col", "timestamp_col"])

df.show()

# displaying current date
df.select(current_date()).show()

# displaying current date with time
df.select(current_timestamp()).show(truncate=False)
