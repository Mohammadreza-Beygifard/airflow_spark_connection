import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sin

# Create Spark session
spark: SparkSession = SparkSession.getActiveSession()
if not spark:
    spark = SparkSession.builder.appName("Spark Submit job With PyArrow").getOrCreate()

N = 5_000_000  # adjust depending on machine

df = (
    spark.range(N).withColumn("x", col("id") * 2).withColumn("y", sin(col("id") * 0.01))
)

# Trigger Spark execution once (warm-up)
df.show(5)
print("Row count:", df.count())

# -------------------------------
# WITHOUT Arrow
# -------------------------------
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

start = time.perf_counter()
pdf_without_arrow = df.toPandas()
end = time.perf_counter()

print(f"\n ****** toPandas WITHOUT Arrow: {end - start:.2f} seconds ****** \n")

# -------------------------------
# WITH Arrow
# -------------------------------
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

start = time.perf_counter()
pdf_with_arrow = df.toPandas()
end = time.perf_counter()

print(f"\n ****** toPandas WITH Arrow: {end - start:.2f} seconds ****** \n")

# Cleanup
spark.stop()
