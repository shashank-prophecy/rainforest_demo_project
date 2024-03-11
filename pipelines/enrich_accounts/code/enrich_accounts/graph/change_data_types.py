from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from enrich_accounts.config.ConfigStore import *
from enrich_accounts.udfs.UDFs import *

def change_data_types(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("market_id"), 
        col("store_id"), 
        col("total_items"), 
        col("subtotal"), 
        col("created_at").cast(DateType()).alias("created_date")
    )
