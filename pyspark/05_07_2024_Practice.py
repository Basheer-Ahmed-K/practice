# Databricks notebook source
# DBTITLE 1,How to see the spark version?
print(spark.version)

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, row_number, initcap, length
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.createDataFrame([("basheer", 1),("ahmed", 2),("databricks", 3),], ["Name", "Value"])
df.show()

# COMMAND ----------

# DBTITLE 1,How to add index in Data Frame
window = Window.orderBy(monotonically_increasing_id())
index_df = df.withColumn('index',row_number().over(window)-1)
index_df.show()

# COMMAND ----------

display(df.coalesce(1).withColumn('Idx', monotonically_increasing_id()))

# COMMAND ----------

list1 = ["a", "b", "c", "d"]
list2 = [1, 2, 3, 4]
rdd1 = sc.parallelize(list(zip(list1,list2)))
final_df = rdd1.toDF(['rdd1', 'rdd2'])
display(final_df)

# COMMAND ----------

# DBTITLE 1,How to Drop rows with NA values specific
null_df = spark.createDataFrame([
("A", 1, None),
("B", None, "123" ),
("B", 3, "456"),
("D", None, None),
], ["Name", "Value", "id"])

null_df.show()

# COMMAND ----------

# DBTITLE 1,For Whole Data Frame
droped_null_df = null_df.dropna()
droped_null_df.show()

# COMMAND ----------

# DBTITLE 1,For Particular Row
row_drop_na_df = null_df.dropna(subset=['Value'])
row_drop_na_df.display()

# COMMAND ----------

# DBTITLE 1,Convert the first character of each element in a series to uppercase
data = [("john",), ("alice",), ("bob",)]
df = spark.createDataFrame(data, ["name"])
df.show()

# COMMAND ----------

cap_df = df.withColumn('name', initcap(df['name']))
display(cap_df)

# COMMAND ----------

# DBTITLE 1,Calculate the number of characters in each word in a column?
len_df = cap_df.withColumn('len', length(cap_df['name']))
display(len_df)

# COMMAND ----------

# DBTITLE 1,Get the day of month, week number, day of year and day of week from a date strings?
data = [("2023-05-18","01 Jan 2010",), ("2023-12-31", "01 Jan 2010",)]
date_df = spark.createDataFrame(data, ["date_str_1", "date_str_2"])
display(date_df)


# COMMAND ----------

from pyspark.sql.functions import to_date, dayofmonth, weekofyear, dayofyear, dayofweek


first_date_df = date_df.withColumn("date_1", to_date(date_df.date_str_1, 'yyyy-MM-dd'))
sec_date_df = first_date_df.withColumn("date_2", to_date(date_df.date_str_2, 'dd MMM yyyy'))

date_df = sec_date_df.withColumn("day_of_month", dayofmonth(sec_date_df.date_1))\
.withColumn("week_number", weekofyear(sec_date_df.date_1))\
.withColumn("day_of_year", dayofyear(sec_date_df.date_1))\
.withColumn("day_of_week", dayofweek(sec_date_df.date_1))
display(date_df)
