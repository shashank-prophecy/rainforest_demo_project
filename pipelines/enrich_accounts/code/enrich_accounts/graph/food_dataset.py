from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from enrich_accounts.config.ConfigStore import *
from enrich_accounts.udfs.UDFs import *

def food_dataset(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("market_id", FloatType(), True), StructField("created_at", TimestampType(), True), StructField("actual_delivery_time", TimestampType(), True), StructField("store_id", StringType(), True), StructField("store_primary_category", StringType(), True), StructField("order_protocol", FloatType(), True), StructField("total_items", LongType(), True), StructField("subtotal", LongType(), True), StructField("num_distinct_items", IntegerType(), True), StructField("min_item_price", LongType(), True), StructField("max_item_price", LongType(), True), StructField("total_onshift_partners", FloatType(), True), StructField("total_busy_partners", FloatType(), True), StructField("total_outstanding_orders", FloatType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/tables/raw_data/food_data.csv")
