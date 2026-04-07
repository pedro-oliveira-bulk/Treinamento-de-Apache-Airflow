from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("bronze_order_items") \
    .getOrCreate()

df = spark.read.json("/home/pedro-rafael/BULK/arquivos_json/order_items/order_item.json")

df.write.mode("overwrite").parquet("/home/pedro-rafael/BULK/lakehouse/bronze/order_items")

spark.stop()
