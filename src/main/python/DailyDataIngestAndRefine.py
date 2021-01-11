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
prev_day_suffix = "_04062020"
current_day_suffix = "_05062020"

# read landing file
landingFileDF = spark.read.schema(landingFileSchema) \
    .format("csv") \
    .option("delimiter", "|") \
    .option("header", False) \
    .load(inputLocation + "Sales_Landing/SalesDump%s/SalesDump.dat" % current_day_suffix)

# spark.sql("SHOW PARTITIONS invalid_data").show(truncate=False)
# spark.sql("DESCRIBE FORMATTED invalid_data").show(truncate=False)
if spark.catalog._jcatalog.tableExists("invalid_data"):
    invalidDF = spark.read.table("invalid_data")
else:
    invalidDF = spark.read.schema(landingFileSchema) \
        .format("csv") \
        .option("delimiter", "|") \
        .option("header", False) \
        .load(inputLocation + "Sales_Landing/SalesDump%s/SalesDump.dat" % prev_day_suffix)

    invalidDF = landingFileDF.where(F.col("Vendor_ID").isNull() | F.col("Quantity_Sold").isNull())
    # invalidDF \
    #     .write \
    #     .partitionBy("Sale_Date") \
    #     .format("parquet") \
    #     .mode("overwrite") \
    #     .saveAsTable("invalid_data")

validLandingData = landingFileDF.filter(F.col("Vendor_ID").isNotNull() | F.col("Quantity_Sold").isNotNull())
refreshLandingData = spark.sql("SELECT A.Sale_ID, A.Product_ID, "
                               "CASE WHEN (A.Quantity_Sold IS NOT NULL) THEN A.Quantity_Sold "
                               "ELSE B.Quantity_Sold END AS Quantity_Sold, "
                               "CASE WHEN (A.Vendor_ID IS NOT NULL) THEN A.Vendor_ID "
                               "ELSE B.Vendor_ID END AS Vendor_ID, "
                               "A.Sale_Date, A.Sale_Amount, A.Sale_Currency "
                               "FROM valid_data A LEFT OUTER JOIN invalid_data B "
                               "ON A.Sale_ID = B.Sale_ID")

# Vendor_ID,StringType()|Sale_Date,DateType()|Sale_Amount,FloatType()|Sale_Currency

# refreshLandingData = validLandingData.join(invalidDF,
#                                            validLandingData.Sale_ID == invalidDF.Sale_ID,
#                                            how="inner")

# refreshLandingData.show(2, truncate=False)
# invalidDF.show(2, truncate=False)
refreshLandingData.subtract(validLandingData).show()
# validLandingData \
#     .write \
#     .partitionBy("Sale_Date") \
#     .format("parquet") \
#     .mode("overwrite") \
#     .saveAsTable("valid_data")

