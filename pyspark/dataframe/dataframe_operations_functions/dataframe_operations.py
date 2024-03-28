from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, desc, count

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

# filter() & where()
df_filter = df.filter(df.Age > 24)
df_filter.show()

df_where = df.where(df.Department == "Data Engineering")
df_where.show()

# like()
df_like = df.filter(df.Name.like("K%"))
df_like.show()

# describe()
df.describe().show()

# columns()
print("Columns in DataFrame:", df.columns)

# when() & otherwise()
df_with_column = df.withColumn("Status", when(col("Age") >= 24, "Senior").otherwise("Junior"))
df_with_column.show()

# alias()
df_alias = df.select(col("Name").alias("employees"), "Age", "Department")
df_alias.show()

# orderBy() & sort()
df_orderBy = df.orderBy(desc("Age"))
df_orderBy.show()

# groupBy() & groupBy agg()
df_groupBy = df.groupBy("Department").agg(count("*").alias("Total"))
df_groupBy.show()
