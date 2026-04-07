from pyspark.sql import SparkSession

def remove_prefix(df, prefix):
    for col_name in df.columns:
        if col_name.startswith(prefix):
            df = df.withColumnRenamed(col_name, col_name[len(prefix):])
    return df

spark = SparkSession.builder \
    .appName("silver_order_items") \
    .getOrCreate()

df = spark.read.parquet("/home/pedro-rafael/BULK/lakehouse/bronze/order_items")
df = remove_prefix(df, "order_item_")

df.write.mode("overwrite").parquet("/home/pedro-rafael/BULK/lakehouse/silver/order_items")

spark.stop()

