from pyspark.sql import SparkSession
from pyspark.sql.functions import stack, expr, col, upper

spark = SparkSession.builder.appName("Important_function").getOrCreate()

csv_path = r'C:\Users\Basheer AhmedK\Desktop\Diggibyte\Pyspark\pyspark practice\pyspark\resources\sample_data.csv'

df = spark.read.csv(csv_path, header=True, inferSchema=True)

df.show()


def toUpper(dataframe):
    return dataframe.withColumn("Name", upper(dataframe.Name))


def doubleExperience(dataframe):
    return dataframe.withColumn("Experience", dataframe.Experience*2)


upper_df = df.transform(toUpper)
upper_df.show()

exp_df = upper_df.transform(doubleExperience)
exp_df.show()
