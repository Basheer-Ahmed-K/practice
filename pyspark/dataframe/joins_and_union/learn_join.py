from pyspark.sql import SparkSession

spark_session = SparkSession.builder.appName('learn-joins_and_union').getOrCreate()

customer_path = r'C:\Users\Basheer AhmedK\Desktop\Diggibyte\Pyspark\pyspark practice\pyspark\datasets\customer.csv'
sales_path = r'C:\Users\Basheer AhmedK\Desktop\Diggibyte\Pyspark\pyspark practice\pyspark\datasets\sales.csv'
product_path = r'C:\Users\Basheer AhmedK\Desktop\Diggibyte\Pyspark\pyspark practice\pyspark\datasets\product.csv'
customer_df = spark_session.read.csv(customer_path, header=True, inferSchema=True)
sales_df = spark_session.read.csv(sales_path, header=True, inferSchema=True)
product_df = spark_session.read.csv(product_path, header=True, inferSchema=True)
customer_df.printSchema()
sales_df.printSchema()
product_df.printSchema()
customer_df.show()
sales_df.show()
product_df.show()

# Inner Join
print("Inner Join of customer_df and sales_df")
customer_df.join(sales_df, sales_df["customer_id"] == customer_df["customer_id"], "inner").show()

# Left Join (Left Outer Join)
print("Left Join/ Left Outer Join of customer_df and sales_df")
customer_df.join(sales_df, sales_df["customer_id"] == customer_df["customer_id"], "left").show()

# Right Join (Right Outer Join)
print("Right Join/ Right Outer Join of the sales_df and product_df")
sales_df.join(product_df, sales_df["product_id"] == product_df["id"], "right").show()

# Full Outer Join (Outer Join)
print("Full Outer Join/ Outer Join of the sales_df and product_df")
customer_df.join(sales_df, customer_df["customer_id"] == sales_df["customer_id"], "outer").show()

# Left_semi Join
print("Left_semi Join of the sales_df and product_df")
customer_df.join(sales_df, customer_df["customer_id"] == sales_df["customer_id"], "left_semi").show()

# Left_anti Join
print("Left_anti Join of the customer_df and sales_df")
customer_df.join(sales_df, customer_df["customer_id"] == sales_df["customer_id"], "left_anti").show()

# cross join
print("cross Join of customer_df and sales_df")
customer_df.crossJoin(sales_df).show()
print("Total count of the cross_join table: ", customer_df.crossJoin(sales_df).count())
