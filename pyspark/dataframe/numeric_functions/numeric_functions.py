from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, min, max, round, abs

spark_session = SparkSession.builder.appName('numeric_functions').getOrCreate()

df = spark_session.read.csv(
    r'C:\Users\Basheer AhmedK\Desktop\Diggibyte\Pyspark\pyspark practice\pyspark\datasets\sample_data.csv', header=True,
    inferSchema=True)
df.show()

# sum() returns the total sum of the column
print('Sum of Experience ')
df.select(sum('Experience')).show()

# avg() returns the total sum of the column
print('Average of Experience ')
df.select(avg('Experience')).show()

# min() return the minimum age from the Employee
print("Minimum age of Employee")
df.select(min('age')).show()

# max() returns the maximum age of Employee
print("Maximum age of Employee")
df.select(max('age')).show()

# round() Rounds the values in a column to the specified number of decimal places
updated_df = df.withColumn('height', df.age + 89.55)
updated_df.show()
print("Rounded value")
updated_df.select(round('height', 1)).show()

# abs() Returns the absolute value of numeric column values.
updated_df = df.withColumn('height', df.age - 89.55)
print("Absolute value")
updated_df.select(abs('height')).show()
