from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

spark_session = SparkSession.builder.appName('window_function').getOrCreate()

emp_data = [(1, 'basheer', 50000, 'IT', 'm'),
            (2, 'krishna', 60000, 'sales', 'm'),
            (3, 'kuldeep', 70000, 'marketing', 'm'),
            (4, 'pratibha', 80000, 'IT', 'm'),
            (5, 'satiya', 90000, 'sales', 'f'),
            (6, 'maha', 45000, 'marketing', 'f'),
            (7, 'kalai', 55000, 'marketing', 'f'),
            (8, 'amit', 100000, 'IT', 'f'),
            (9, 'lokesh', 65000, 'IT', 'm'),
            (10, 'rahul', 50000, 'marketing', 'm'),
            (11, 'rakhi', 50000, 'IT', 'f'),
            (12, 'akhilesh', 90000, 'sales', 'm')]

schema = ('id', "name", "salary", "department", "gender")

df = spark_session.createDataFrame(emp_data, schema)
df.show()

window = Window.orderBy("salary")

print("row_number")
df.withColumn("row_number", row_number().over(window)).show()
