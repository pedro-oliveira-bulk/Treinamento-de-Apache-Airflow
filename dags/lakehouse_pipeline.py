from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "pedro",
    "start_date": datetime(2026, 4, 7),
}

with DAG(
    dag_id="lakehouse_pipeline",
    default_args=default_args,
    schedule=None,
    catchup=False,
    description="Pipeline Lakehouse com Bronze, Silver e Gold",
    tags=["lakehouse", "pyspark", "airflow"],
) as dag:

    bronze_customers = BashOperator(
        task_id="bronze_customers",
        bash_command="spark-submit /home/pedro-rafael/BULK/lakehouse/bronze/bronze_customers.py"
    )

    bronze_orders = BashOperator(
        task_id="bronze_orders",
        bash_command="spark-submit /home/pedro-rafael/BULK/lakehouse/bronze/bronze_orders.py"
    )

    bronze_order_items = BashOperator(
        task_id="bronze_order_items",
        bash_command="spark-submit /home/pedro-rafael/BULK/lakehouse/bronze/bronze_order_items.py"
    )

    silver_customers = BashOperator(
        task_id="silver_customers",
        bash_command="spark-submit /home/pedro-rafael/BULK/lakehouse/silver/silver_customers.py"
    )

    silver_orders = BashOperator(
        task_id="silver_orders",
        bash_command="spark-submit /home/pedro-rafael/BULK/lakehouse/silver/silver_orders.py"
    )

    silver_order_items = BashOperator(
        task_id="silver_order_items",
        bash_command="spark-submit /home/pedro-rafael/BULK/lakehouse/silver/silver_order_items.py"
    )

    gold = BashOperator(
        task_id="gold",
        bash_command="spark-submit /home/pedro-rafael/BULK/lakehouse/gold/gold.py"
    )

    bronze_customers >> silver_customers
    bronze_orders >> silver_orders
    bronze_order_items >> silver_order_items

    [silver_customers, silver_orders, silver_order_items] >> gold

