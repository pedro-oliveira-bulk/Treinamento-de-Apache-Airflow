from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("bronze_customers") \
    .getOrCreate()

df = spark.read.json("/home/pedro-rafael/BULK/arquivos_json/customers/customers.json")

df.write.mode("overwrite").parquet("/home/pedro-rafael/BULK/lakehouse/bronze/customers")

spark.stop()

