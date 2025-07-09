from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession

def run_spark_job():
    spark = SparkSession.builder \
        .master("spark://spark:7077") \
        .appName("Airflow-Spark Batch Job") \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
    df.show()
    spark.stop()

#  ===================== DAG Definition  =====================
default_args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=30)
}

with DAG(
    dag_id="spark_batch_job",
    description="This DAG sends messages to Spark for a Druid/Superset demo.",
    schedule='*/1 * * * *',         # Every minute
    start_date=datetime(2022, 2, 26), 
    catchup=False,
    tags=["production"]
) as dag:
    
    spark_task = PythonOperator(
        task_id="run_spark",
        python_callable=run_spark_job
    )

    spark_task
