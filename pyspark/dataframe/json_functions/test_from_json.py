from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, udf, explode_outer, col
from pyspark.sql.types import StructType, StructField, MapType, StringType, ArrayType, IntegerType

spark_session = SparkSession.builder.appName('from_json').getOrCreate()

json_path = r'C:\Users\Basheer AhmedK\Desktop\Diggibyte\Pyspark\pyspark practice\pyspark\resources\users_nested.json'

df = spark_session.read.option("multiline", "true").json(json_path)
df.show(truncate=False)


def flatten(df):
    # Compute complex fields (Arrays and Structs) in schema
    complex_fields = dict([(field.name, field.dataType)
                           for field in df.schema.fields
                           if type(field.dataType) == ArrayType or type(field.dataType) == StructType])

    # Continue flattening until all complex fields are processed
    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        print("Processing :" + col_name + " Type : " + str(type(complex_fields[col_name])))

        # Flatten structs: convert all sub-elements to columns
        if type(complex_fields[col_name]) == StructType:
            expanded = [col(col_name + '.' + k).alias(col_name + '_' + k) for k in
                        [n.name for n in complex_fields[col_name]]]
            df = df.select("*", *expanded).drop(col_name)

        # Explode arrays: add the array elements as rows using the explode function
        elif type(complex_fields[col_name]) == ArrayType:
            df = df.withColumn(col_name, explode_outer(col_name))

        # Recompute remaining complex fields in schema
        complex_fields = dict([(field.name, field.dataType)
                               for field in df.schema.fields
                               if type(field.dataType) == ArrayType or type(field.dataType) == StructType])

    return df


df_flatten = flatten(df)

df_flatten.show(truncate=False)

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json
# from pyspark.sql.types import StructType, StructField, MapType, StringType, ArrayType, IntegerType
#
# spark_session = SparkSession.builder.appName('from_json').getOrCreate()
#
# jsonString = """{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""
# df = spark_session.createDataFrame([(1, jsonString)], ["id", "value"])
# df.show(truncate=False)
#
# df2 = df.withColumn("value", from_json(df.value, MapType(StringType(), StringType())))
# df2.printSchema()
# df2.show(truncate=False)
