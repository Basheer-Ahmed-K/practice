from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, col, udf

spark = SparkSession.builder.appName("Important_function").getOrCreate()

csv_path = r'C:\Users\Basheer AhmedK\Desktop\Diggibyte\Pyspark\pyspark practice\pyspark\resources\person.csv'

df = spark.read.csv(csv_path, header=True, inferSchema=True)

df.show(10)


# defining a function to rename column
def rename_col(rename_df):
    for column in rename_df.columns:
        new_col = "UDF_" + column
        rename_df = rename_df.withColumnRenamed(column, new_col)
    return rename_df


rename_column = rename_col(df)
rename_column.show(10)


# function to convert name to uppercase
def to_upper(upper_df):
    name_upper = upper_df.withColumn("UDF_name", upper(col("UDF_name")))
    return name_upper


upper_name = to_upper(rename_column)
upper_name.show()


# another upper case function
def upperCase(some):
    return some.upper()


upperCase_udf = udf(lambda z: upperCase(z))

df.select("*", upperCase_udf(col("name")).alias("Upper using UDF")).show()
