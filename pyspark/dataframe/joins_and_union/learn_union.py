from pyspark.sql import SparkSession

spark_session = SparkSession.builder.appName("learn_union").getOrCreate()

data1 = [("Basheer", 23, 2), ("Krishna", 24, 3), ("Kuldeep", 25, 4)]
data2 = [("pratibha", 23, 1), ("Manvi", 24, 2), ("kalai", 23, 2), ("Basheer", 23, 2)]

schema = ("Name", "Age", "Experience")

df1 = spark_session.createDataFrame(data1, schema)
df2 = spark_session.createDataFrame(data2, schema)

df1.show()
df2.show()

# union() -> Return a new DataFrame containing the union of rows in this and another DataFrame.
print("Applying union() -> Return a new DataFrame containing the union of rows in this and another DataFrame. with "
      "unique values")
df1.union(df2).distinct().show()
print(df1.union(df2).distinct().count())

# unionAll() ->
print("Applying unionAll() -> Return a new DataFrame containing the union of rows in this and another DataFrame. with "
      "duplicate value")
df1.unionAll(df2).show()
print(df1.unionAll(df2).count())
