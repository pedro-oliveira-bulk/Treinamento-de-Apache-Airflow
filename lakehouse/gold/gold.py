from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("gold").getOrCreate()

customers = spark.read.parquet("/home/pedro-rafael/BULK/lakehouse/silver/customers")
orders = spark.read.parquet("/home/pedro-rafael/BULK/lakehouse/silver/orders")
order_items = spark.read.parquet("/home/pedro-rafael/BULK/lakehouse/silver/order_items")

customers = customers.withColumnRenamed("id", "customer_id")
orders = orders.withColumnRenamed("id", "order_id")

df = orders.join(customers, "customer_id")

df = df.join(order_items, "order_id")

gold_df = df.groupBy("city", "state").agg(
    F.countDistinct("order_id").alias("quantidade_pedidos"),
    F.sum("subtotal").alias("valor_total_pedido")
)

gold_df.write.mode("overwrite").parquet("/home/pedro-rafael/BULK/lakehouse/gold/final")

spark.stop()
