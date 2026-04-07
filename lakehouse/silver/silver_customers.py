from pyspark.sql import SparkSession

def remove_prefix(df, prefix):
    for col_name in df.columns:
        if col_name.startswith(prefix):
            df = df.withColumnRenamed(col_name, col_name[len(prefix):])
    return df

spark = SparkSession.builder \
    .appName("silver_customers") \
    .getOrCreate()

df = spark.read.parquet("/home/pedro-rafael/BULK/lakehouse/bronze/customers")
df = remove_prefix(df, "customer_")

df.write.mode("overwrite").parquet("/home/pedro-rafael/BULK/lakehouse/silver/customers")

spark.stop()
