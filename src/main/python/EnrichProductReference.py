import configparser
import findspark
from os import path as os_path

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from src.main.python.helper import read_schema


spark = SparkSession \
            .builder \
            .appName("Enrich Product Reference") \
            .master("local") \
            .enableHiveSupport() \
            .getOrCreate()

cfgParser = configparser.ConfigParser()
cfgParser.read("../project_configs/configs.ini")
products_schema_str = cfgParser.get("Schema", "productSchema")
inputLocation = cfgParser.get("Paths", "landingFilePath")

current_date = '2020-04-01'
products_schema = read_schema(products_schema_str)

productsDF = spark.read\
    .format("csv") \
    .option("delimiter", "|") \
    .schema(products_schema) \
    .load(os_path.join(inputLocation, "Products", "GKProductList.dat"))

productsDF.createOrReplaceTempView("products")
# df = spark.read.table("valid_data")
# df = df.selectExpr("CAST(Sale_date AS DATE)")
# df.show()
# df.printSchema()


tmp_schema = productsDF.schema
# Sale_ID,StringType()|Product_ID,StringType()|Quantity_Sold,IntegerType()|Vendor_ID,StringType()|Sale_Date,DateType()|Sale_Amount,FloatType()|Sale_Currency,StringType()
productsDF = spark.sql("select CAST(a.SALE_ID AS DATE), a.Product_ID, a.Quantity_Sold, "
                       "a.Vendor_ID, a.Sale_Date, "
                       "a.Quantity_Sold * b.Product_Price AS Sale_Amount, "
                       "b.Product_Price_Currency AS Sale_Currency "
                       "FROM valid_data a INNER JOIN products b ON "
                       "a.Product_ID = b.Product_ID "
                       "WHERE DATEDIFF(a.Sale_Date, to_date('%s', 'yyyy-MM-dd')) = 0"
                       % current_date)
new_schema = productsDF.schema
productsDF.show()
