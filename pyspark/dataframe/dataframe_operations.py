from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

sparkSession = SparkSession.builder.master("local[*]").getOrCreate()

data = [("Basheer", 24, "Data Engineering"),
        ("Krishna", 23, "Full Stack"),
        ("Kuldeep", 25, "Data Engineering"),
        ("Pratibha", 26, "Devops"),
        ("Raja Mahalakshmi", 22, "Data Science")]

column = ["Name", "Age", "Department"]

df = sparkSession.createDataFrame(data, column)

# To print Total No. of rows in dataframe
print("Total No. of Rows: ", df.count())

# select()
df.select("Name", "Age").show()

