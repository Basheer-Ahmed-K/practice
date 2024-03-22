from pyspark import SparkContext

# Create a SparkContext
sc = SparkContext("local", "RDD_Transformations")

# Sample RDD data
rdd1 = sc.parallelize([1, 2, 3, 4, 5])
rdd2 = sc.parallelize([4, 5, 6, 7, 8])
rdd3 = sc.parallelize([(1, 'apple'), (2, 'banana'), (3, 'apple'), (4, 'banana'), (5, 'orange')])

# Intersection
intersection_rdd = rdd1.intersection(rdd2)
print("Intersection:", intersection_rdd.collect())

# Distinct
distinct_rdd = rdd3.distinct()
print("Distinct:", distinct_rdd.collect())

# Group By Key
grouped_rdd = rdd3.groupByKey()
print("Group By Key:")
for key, values in grouped_rdd.collect():
    print(key, list(values))

# FlatMap
flatmap_rdd = rdd3.flatMap(lambda x: (x[1], x[1].upper()))
print("FlatMap:", flatmap_rdd.collect())


# Map Partitions
def add_partition(iterable):
    yield sum(iterable)


mappartitions_rdd = rdd1.mapPartitions(add_partition)
print("Map Partitions:", mappartitions_rdd.collect())


# Map Partitions With Index
def add_partition_index(index, iterable):
    yield index, sum(iterable)


mappartitionsindex_rdd = rdd1.mapPartitionsWithIndex(add_partition_index)
print("Map Partitions With Index:", mappartitionsindex_rdd.collect())

# Glom
glom_rdd = rdd3.glom()
print("Glom:", glom_rdd.collect())

# Union
union_rdd = rdd1.union(rdd2)
print("Union:", union_rdd.collect())

sc.stop()
