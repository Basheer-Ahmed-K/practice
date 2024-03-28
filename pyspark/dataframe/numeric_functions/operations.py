from pyspark.sql import SparkSession

spark_session = SparkSession.builder.appName('numeric_functions').getOrCreate()

df = spark_session.read.csv(
    r'C:\Users\Basheer AhmedK\Desktop\Diggibyte\Pyspark\pyspark practice\pyspark\datasets\sample_data.csv', header=True,
    inferSchema=True)
df.show()

# OR operator
print("OR Operator")
df.filter((df['age'] == 23) | (df['Experience'] <= 3)).show()

# AND operator
print("AND Operator")
df.filter((df['age']== 23) & (df['Name'] == "Basheer")).show()

# NOT Operator
print("NOT Operator (~)")
df.filter(~(df['age']== 23) | (df['Experience'] == 2)).show()