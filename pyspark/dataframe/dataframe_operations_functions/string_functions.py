from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a SparkSession
spark = SparkSession.builder \
    .appName("PySpark String Functions") \
    .getOrCreate()

# Sample DataFrame
data = [("John Doe",),
        ("  Jane  ",),
        ("Alice",)]

df = spark.createDataFrame(data, ["name"])
df.show()

# upper(): Converts a string column to upper case
df_upper = df.select(upper(col("name")).alias("name_upper"))
df_upper.show()

# trim(): Trim leading and trailing whitespace from a string column
df_trim = df.select(trim(col("name")).alias("name_trim"))
df_trim.show()

# ltrim(): Trim leading whitespace from a string column
df_ltrim = df.select(ltrim(col("name")).alias("name_ltrim"))
df_ltrim.show()

# rtrim(): Trim trailing whitespace from a string column
df_rtrim = df.select(rtrim(col("name")).alias("name_rtrim"))
df_rtrim.show()

# translate(): Replace all characters in a string column with the given translation map
df_translate = df.select(translate(col("name"), "aeiou", "12345").alias("name_translate"))
df_translate.show()

# substring_index(): Returns the substring from string before the first count occurrences of the delimiter
df_substring_index = df.select(substring_index(col("name"), " ", 1).alias("first_name"))
df_substring_index.show()

# substring(): Substring of a string column
df_substring = df.select(substring(col("name"), 2, 3).alias("name_substring"))
df_substring.show()

# split(): Splits a string column into an array of substrings
df_split = df.select(split(col("name"), " ").alias("name_split"))
df_split.show()

# repeat(): Repeats a string column n times, and returns it as a new string column
df_repeat = df.select(repeat(col("name"), 2).alias("name_repeat"))
df_repeat.show()

# rpad(): Right-pad the string column to width width with pad
df_rpad = df.select(rpad(col("name"), 8, "#").alias("name_rpad"))
df_rpad.show()

# lpad(): Left-pad the string column to width width with pad
df_lpad = df.select(lpad(col("name"), 8, "#").alias("name_lpad"))
df_lpad.show()

# regexp_replace(): Replace all substrings of the specified string value that match regexp with rep
df_regex_replace = df.select(regexp_replace(col("name"), "Jane", "Jack").alias("name_regex_replaced"))
df_regex_replace.show()

# lower(): Converts a string column to lower case
df_lower = df.select(lower(col("name")).alias("name_lower"))
df_lower.show()

# regex_extract(): Extracts a group by a Java regex, which is equivalent to SQL REGEXP_EXTRACT
df_regex_extract = df.select(regexp_extract(col("name"), "(\w+)", 1).alias("name_regex_extracted"))
df_regex_extract.show()

# length(): Computes the character length of a string column
df_length = df.select(length(col("name")).alias("name_length"))
df_length.show()
