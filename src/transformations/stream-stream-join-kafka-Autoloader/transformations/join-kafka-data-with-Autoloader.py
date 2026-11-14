from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

env=spark.conf.get("env")
catalog = f"myorg_{env}"

@dp.table(
name=f"{catalog}.gold.fraud_card_kafka_autoloader_join"
,comment="Alert details where transaciton has been performed with value higher than what is configured by customer"
)
def fraud_card_alert() -> DataFrame:
    transactions=spark.readStream.table(f"{catalog}.silver.silver_kafka_fraud")
    fraud_watchlist=spark.readStream.table(f"{catalog}.silver.fraud_tbl_silver_autoloader")

    customers = spark.read.table(f"{catalog}.silver.customers")

    transactions_with_watermark=transactions.withWatermark("transaction_ts", "5 minutes")
    fraud_watchlist_with_watermark=fraud_watchlist.withWatermark("effective_from", "5 minutes")

    fraud_detected=(
            transactions_with_watermark.join(
                    fraud_watchlist_with_watermark,
                    transactions_with_watermark.card_number_hash==fraud_watchlist_with_watermark.entity_id,
                    "inner"
            ).join(
                customers,
                transactions_with_watermark.customer_id==customers.customer_id,
                "left"
            ).select(
                
                # Alert identification
            F.concat_ws("-", F.lit("FRAUD"), F.col("transaction_id"), F.col("watchlist_id")).alias("alert_id"),
            F.lit("FRAUD_WATCHLIST_MATCH").alias("alert_type"),
            F.current_timestamp().alias("alert_timestamp"),
            
            # Transaction details
            transactions_with_watermark.transaction_id,
            transactions_with_watermark.customer_id,
            customers.email.alias("customer_email"),
            F.concat_ws(" ", customers.first_name, customers.last_name).alias("customer_name"),
            transactions_with_watermark.card_number_hash,
            transactions_with_watermark.amount,
            transactions_with_watermark.currency,
            transactions_with_watermark.merchant_id,
            transactions_with_watermark.merchant_name,
            transactions_with_watermark.merchant_category,
            transactions_with_watermark.transaction_type,
            transactions_with_watermark.payment_channel,
            transactions_with_watermark.device_id,
            transactions_with_watermark.city.alias("transaction_city"),
            transactions_with_watermark.country.alias("transaction_country"),
            transactions_with_watermark.transaction_ts,
            transactions_with_watermark.is_international,
            transactions_with_watermark.status.alias("transaction_status"),
            
            # Fraud watchlist details
            F.col("watchlist_id"),
            F.col("watch_type"),
            F.col("risk_level"),
            F.col("action"),
            F.col("reason_code"),
            F.col("reason_description"),
            F.col("effective_from").alias("watchlist_effective_from"),
            F.col("reported_by"),
            F.col("reported_source"),
            fraud_watchlist_with_watermark.city.alias("watchlist_city"),
            fraud_watchlist_with_watermark.country.alias("watchlist_country")

            )


    )
    return fraud_detected






  
