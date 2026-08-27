from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table
def gold_daily_revenue():

    return (
        spark.readStream.table("silver_orders")
        .groupBy("order_date")
        .agg(
            F.sum("total_amount").alias("daily_revenue"),
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("quantity").alias("total_quantity")
        )
    )