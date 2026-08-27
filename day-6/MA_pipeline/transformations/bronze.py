from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

@dp.table
def bronze_orders():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load("/Volumes/dev/demo/raw-1000-richest/sales/")
        .withColumn("ingestion_data",current_timestamp())
    )

