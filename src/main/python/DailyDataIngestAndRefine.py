import configparser
import findspark

findspark.init()

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from src.main.python.helper import read_schema

# Create spark session
spark = SparkSession \
    .builder \
    .appName("DailyDataIngestAndRefine") \
    .master("local") \
    .enableHiveSupport() \
    .getOrCreate()

# read configs
cfgParser = configparser.ConfigParser()
cfgParser.read("../project_configs/configs.ini")
inputLocation = cfgParser.get("Paths", "landingFilePath")
schema_string = cfgParser.get("Schema", "landingFileSchema")

# read schema
landingFileSchema = read_schema(schema_string)

# prev day
prev_day_suffix = ""
current_day_suffix = "_04062020"

# read landing file
landingFileDF = spark.read.schema(landingFileSchema) \
    .format("csv") \
    .option("delimiter", "|") \
    .option("header", False) \
    .load(inputLocation + "Sales_Landing/SalesDump%s/SalesDump.dat" % current_day_suffix)


invalidDF = landingFileDF.where(F.col("Vendor_ID").isNull() | F.col("Quantity_Sold").isNull())
invalidDF \
    .write \
    .format("parquet") \
    .mode("overwrite") \
    .saveAsTable("invalid_data")
hive_invalid_df = spark.read.table("invalid_data")
hive_invalid_df.show()
