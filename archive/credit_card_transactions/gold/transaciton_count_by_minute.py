from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

@dp.table(
    name ="161pmprojectcatalog.dev_catlocation.fraud_transaction_autoloader_gold_per_min_dp"
)
def transaciton_count_by_minute():
    transactions=spark.readStream.table("161pmprojectcatalog.dev_catlocation.transactions_streaming_silver_dp")
    transactions_with_watermark=transactions.withWatermark("transaction_timestamp", "5 minutes")

    transaction_count_df=transactions_with_watermark.groupBy(
        F.window("transaction_timestamp","1 minute")
    ).agg(

    F.count("*").alias("transaction_count")
    ).select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("transaction_count")
    )
    return transaction_count_df