from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp, date_add, date_sub, datediff, year, dayofyear, month, \
        hour, minute, second

spark = SparkSession.builder.appName('DateTime').config("spark.sql.legacy.timeParserPolicy", "LEGACY").getOrCreate()

data = [("2022-03-15", "2022-03-16 12:34:56.789"),
        ("2022-03-01", "2022-03-16 01:23:45.678")]
df = spark.createDataFrame(data, ["date_col", "timestamp_col"])

df.show()

# displaying current date
df.select(current_date()).show()

# displaying current date with time
df.select(current_timestamp()).show(truncate=False)

# returns a new date by adding a specified number of days to a given start date.
df.select(date_add("date_col", 3).alias("added 3 days")).show()

# returns a new date by subtracting a specified number of days from a given start date.
df.select(date_sub("date_col", 5).alias("subtracted 5 days")).show()

# returns the number of days between two dates.
df.select(datediff("timestamp_col", "date_col").alias("date_diff")).show()

# Extract the year of a given date/timestamp as integer.
df.select(year("timestamp_col").alias("year")).show()

# Returns the day of the year of the input date
df.select(dayofyear('date_col').alias("day of year")).show()

# returns the month component of a date.
df.select(month("date_col").alias("month")).show()

# hour, minute, second - returns corresponding component
df.select(hour("timestamp_col"), minute("timestamp_col"), second("timestamp_col")).show()
