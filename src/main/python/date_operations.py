import pyspark.sql.functions as py_f
from pyspark.sql import SparkSession

# Create spark session

spark = SparkSession \
    .builder \
    .appName("date_operations") \
    .master("local") \
    .getOrCreate()

df = spark.createDataFrame([("10-Jan-2021", )], ["st_date"])
df = df.withColumn("start_date", py_f.to_date("st_date", "dd-MMM-yyyy"))

df.select(py_f.col("st_date"), py_f.col("start_date"),
          py_f.date_format(py_f.current_date(), "EEE, dd-MMM-yyyy").alias("current_date"),
          py_f.date_add(py_f.col("start_date"), 1).alias("next_day"),
          py_f.date_sub(py_f.col("start_date"), 1).alias("prev_day"))\
    .show()
