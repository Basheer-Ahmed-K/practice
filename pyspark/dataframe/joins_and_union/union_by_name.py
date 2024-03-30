from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('union_by_name').getOrCreate()

# Create DataFrame df1 with columns name, and id
data = [("basheer", 34), ("krishna", 56), ("kuldeep", 30), ("pratibha", 24)]

df1 = spark.createDataFrame(data=data, schema=["name", "id"])
df1.printSchema()
df1.show()

# Create DataFrame df2 with columns name and id
data2 = [(34, "Basheer"), (45, "Manvi"), (45, "Kalai"), (34, "Ahmed")]

df2 = spark.createDataFrame(data=data2, schema=["id", "name"])
df2.printSchema()
df2.show()

print("Using union()")
df1.union(df2).show()

print("Using unionByName()")
df1.unionByName(df2).show()
