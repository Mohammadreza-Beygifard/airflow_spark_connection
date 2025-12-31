from pyspark.sql import SparkSession
from scripts.spark.schema.schemas import customers_schema


def run_pyspark_on_standalone():
    spark: SparkSession = SparkSession.getActiveSession()
    if spark:
        spark.stop()
    spark = (
        SparkSession.builder.appName("Spark operator job")
        .master(
            "spark://spark-master:7077"
        )  # As we use a normal python operator gere, we have to explicitly mention the spark master URL
        .getOrCreate()
    )

    customers_df = spark.read.csv(
        path="file:///opt/airflow/data/customers.csv",
        header=True,
        schema=customers_schema,
    )

    customers_df.show(2)

    spark.stop()
