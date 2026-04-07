from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("bronze_orders") \
    .getOrCreate()

df = spark.read.json("/home/pedro-rafael/BULK/arquivos_json/orders/orders.json")

df.write.mode("overwrite").parquet("/home/pedro-rafael/BULK/lakehouse/bronze/orders")

spark.stop()

