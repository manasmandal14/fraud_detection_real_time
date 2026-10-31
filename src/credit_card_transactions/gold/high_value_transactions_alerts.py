from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

@dp.table(
    name="161pmprojectcatalog.dev_catlocation.transactions_customer_gold"
)
def high_value_transactions_alert() -> DataFrame:
    transactions=spark.readStream.table("161pmprojectcatalog.dev_catlocation.transactions_streaming_silver_dp")
    customers =spark.read.table("161pmprojectcatalog.dev_catlocation.customers_silver_dp")

    joined_df = ( transactions.join(F.broadcast(customers), transactions.customer_id == customers.customer_id, "left")
                .filter(F.col("amount")>F.col("transaction_limit"))
                .select(
                    F.concat_ws("-",F.lit("ALERT"),F.col("transaction_id")).alias("alert_id"),
                    F.lit("HIGH_VALUE_TRANSACTION").alias("alert_type"),
                    F.current_timestamp().alias("alert_timestamp"),
              
                    transactions.transaction_id,
                    transactions.customer_id,
                    customers.email.alias("customer_email"),
                    F.concat_ws(" ",F.col("first_name"),F.col("last_name")).alias("customer_name"),
                    transactions.amount.alias("transaction_amount"),
                    customers.transaction_limit,
                    transactions.currency,
                    transactions.merchant_name,
                    transactions.merchant_category,
                    transactions.transaction_type,
                    transactions.payment_channel,
                    transactions.city,
                    transactions.country,
                    transactions.is_international,
                    transactions.transaction_timestamp,
                    transactions.status 
                )
    )
    return joined_df
