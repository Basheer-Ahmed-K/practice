from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Important_function").getOrCreate()

csv_path = r'C:\Users\Basheer AhmedK\Desktop\Diggibyte\Pyspark\pyspark practice\pyspark\resources\employee.csv'

df = spark.read.csv(csv_path, header=True, inferSchema=True)

df.show()


# pivot

df.groupby("department").pivot("gender").count().show()

df.groupby("department").pivot("gender").sum("salary").show()
