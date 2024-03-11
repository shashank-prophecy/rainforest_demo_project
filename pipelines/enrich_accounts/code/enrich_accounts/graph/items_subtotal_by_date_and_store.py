from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from enrich_accounts.config.ConfigStore import *
from enrich_accounts.udfs.UDFs import *

def items_subtotal_by_date_and_store(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("created_date"), col("store_id"))

    return df1.agg(sum(col("total_items")).alias("total_items"), sum(col("subtotal")).alias("subtotal"))
