from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table
def silver_orders():
    return (
        spark.readStream.table("bronze_orders")
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn(
            "total_amount",
            F.col("total_amount").cast("decimal(12,2)")
        )
        .dropDuplicates(["order_id"])
        .filter(F.col("order_id").isNotNull())
    )