from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from enrich_accounts.config.ConfigStore import *
from enrich_accounts.udfs.UDFs import *
from prophecy.utils import *
from enrich_accounts.graph import *

def pipeline(spark: SparkSession) -> None:
    df_food_dataset = food_dataset(spark)
    df_change_data_types = change_data_types(spark, df_food_dataset)
    df_items_subtotal_by_date_and_store = items_subtotal_by_date_and_store(spark, df_change_data_types)
    store_level_aggregation(spark, df_items_subtotal_by_date_and_store)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/enrich_accounts")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/enrich_accounts", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/enrich_accounts")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
