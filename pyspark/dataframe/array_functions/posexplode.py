from pyspark.sql import SparkSession
from pyspark.sql.functions import posexplode
from pyspark.sql.types import StructType, StructField, MapType, StringType

spark_session = SparkSession.builder.appName('explore_functions').getOrCreate()

# array
data = [(1, "basheer", ['git', None]), (2, "krishna", ['python', 'sql']),
        (3, "kuldeep", ['sql', 'cloud']), (4, "amit", ['chat GPT', 'python']),
        (5, "pratibha", ['java', 'c++']), (6, "manvi", ['node.js', 'c']),
        (7, "maha", None), (8, "sathiya", ['python', 'sql'])]
schema = ['id', 'name', 'skill']

df = spark_session.createDataFrame(data, schema)
# df.printSchema()
# df.show()

df.select("*", posexplode('skill')).show(truncate=False)

# map
map_data = [
    ('basheer', {'hair': 'brown', 'eye': 'blue'}),
    ('amit', {'hair': 'black', 'eye': 'black'}),
    ('kuldeep', {'hair': 'grey', 'eye': None}),
    ('krishna', {'hair': 'red', 'eye': 'black'}),
    ('manvi', {'hair': 'black', 'eye': 'grey'}),
    ('maha', None)
]

# defining schema
map_schema = StructType([
    StructField('name', StringType(), True),
    StructField('properties', MapType(StringType(), StringType()), True),
])

map_df = spark_session.createDataFrame(map_data, map_schema)
# map_df.printSchema()
# map_df.show(truncate=False)

map_df.select("*", posexplode("properties")).show(truncate=False)
