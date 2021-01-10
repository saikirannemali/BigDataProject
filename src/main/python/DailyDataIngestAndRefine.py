import findspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType,\
    IntegerType, TimestampType, FloatType

findspark.init()
spark = SparkSession\
    .builder\
    .appName("DailyDataIngestAndRefine") \
    .master("local") \
    .getOrCreate()

sc = spark.sparkContext

landingFileSchema = StructType() \
    .add(StructField("Sale_ID", StringType(), True)) \
    .add(StructField("Product_ID", StringType(), True)) \
    .add(StructField("Quantity_Sold", IntegerType(), True)) \
    .add(StructField("Vendor_ID", StringType(), True)) \
    .add(StructField("Sale_Date", TimestampType(), True)) \
    .add(StructField("Sale_Amount", FloatType(), True)) \
    .add(StructField("Sale_Currency", StringType(), True)) \

landingFileDF = spark.read.schema(landingFileSchema)\
    .format("csv") \
    .option("delimiter", "|") \
    .option("header", False) \
    .load(r"D:\BigDataProject\Data\Inputs\Sales_Landing\SalesDump_04062020\SalesDump.dat")

landingFileDF.show()
