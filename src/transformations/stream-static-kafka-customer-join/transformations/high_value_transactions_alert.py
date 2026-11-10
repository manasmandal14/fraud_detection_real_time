from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

env=spark.conf.get("env")
catalog = f"myorg_{env}"

@dp.table(name=f"{catalog}.gold.high_value_transactions")
def high_value_transactions() -> DataFrame:
    transactions= spark.readStream.table(f"{catalog}.silver.silver_kafka_fraud")
    customers =spark.read.table(f"{catalog}.silver.customers")

    joined_df = transactions.join(customers,transactions.customer_id == customers.customer_id,  how="left")
    joined_df= joined_df.filter(F.col("amount") > F.col("transaction_limit"))

    joined_df=  joined_df.select(
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
                    transactions.transaction_ts,
                    transactions.status 
                )
            
    return joined_df

    

    