from pyspark.sql import SparkSession

sparkSession = SparkSession.builder.appName('DataFrame').getOrCreate()

# inferSchema assign proper datatype
# header will be assigned as first col in DataFrame
df = sparkSession.read.csv('sample_data.csv', header=True, inferSchema=True)
df.show()
df.printSchema()
print(type(df))

# selecting Columns
type(df.select('Name'))
df.select(['Name', 'age']).show()
print(df.schema)

# describe
print(df.describe())
df.describe().show()

# Adding Column in dataframe
# as we use like this it will not reflect in df we have to assign it to same variable
df.withColumn('Experience after 2 years', df['Experience'] + 2).show()
# assigning to the same variable
df = df.withColumn('Experience after 2 years', df['Experience'] + 2)
# df.show()

# Drop Column
df = df.drop('Experience after 2 years')
df.show()

# rename the Column
df = df.withColumnRenamed('age', 'new age')
df.show()
