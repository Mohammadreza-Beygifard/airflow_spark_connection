from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from schema.schemas import customers_schema, orders_schema

spark: SparkSession = SparkSession.getActiveSession()
if not spark:
    spark = SparkSession.builder.appName("Spark Submit").getOrCreate()


customers_df = spark.read.csv(
    path="file:///opt/airflow/data/customers.csv",
    header=True,
    schema=customers_schema,
)

customers_df.show(2)

spark.stop()
