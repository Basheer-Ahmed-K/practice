from pyspark.sql import SparkSession
from pyspark.sql.functions import stack, expr, col

spark = SparkSession.builder.appName("Important_function").getOrCreate()

csv_path = r'C:\Users\Basheer AhmedK\Desktop\Diggibyte\Pyspark\pyspark practice\pyspark\resources\salary_unpivot.csv'

df = spark.read.csv(csv_path, header=True, inferSchema=True)

df.show()

# unpivot
df.select(col("id"), col("name"),
          expr("stack(3, 'HR', HR_salary, 'IT', IT_salary, 'Finance', Finance_salary) as (department, salary)"))\
    .where("salary != 0").show()
