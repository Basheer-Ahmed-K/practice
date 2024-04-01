from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, array_contains, array_position, array_remove, array_sort, array_union

spark = SparkSession.builder \
    .appName("array_functions") \
    .getOrCreate()

initial_data = [
    ("x", 4, 1), ("x", 6, 2), ("z", 7, 3), ("a", 3, 4), ("z", 5, 2),
    ("x", 7, 3), ("x", 9, 7), ("z", 1, 8), ("z", 4, 9), ("z", 7, 4),
    ("a", 8, 5), ("a", 5, 2), ("a", 3, 8), ("x", 2, 7), ("z", 1, 9)
]

initial_df = spark.createDataFrame(initial_data, ["col1", "col2", "col3"])

# Group by "col1" and collect lists for "col2" and "col3"
full_df = initial_df.groupBy("col1").agg(collect_list("col2").alias("array_col1"),
                                         collect_list("col3").alias("array_col2"))
df = full_df
# Drop the "array_col1" column
# df = full_df.drop("array_col1")

# Show the DataFrame
# df.show(truncate=False)

# array_contains -> returns null if the array is null, true if the array contains the given value, and false otherwise.
arr_contains_df = df.withColumn("array_contains_3", array_contains("array_col2", 3))
arr_contains_df.show()

# array_position -> This function returns the position of first occurrence of a specified element.
# if the element is not present it returns 0
arr_position_df = df.withColumn("array_position_4", array_position("array_col2", 4))
arr_position_df.show()

# array_remove -> This function removes all the occurrences of an element from an array.
arr_remove_df = df.withColumn("array_remove_8", array_remove("array_col2", 8))
arr_remove_df.show()

# array_sort -> sorts the input array in ascending order if null it will be placed at the end the array
df.withColumn("array_sort", array_sort("array_col2")).show()

# array_union -> returns an array of the elements in the union of col1 and col2, without duplicates.
df.withColumn("array_union", array_union(df.array_col1, df.array_col2)).show(truncate=False)

