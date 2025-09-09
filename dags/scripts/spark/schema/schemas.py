from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType,
    FloatType,
)


customers_schema = StructType(
    [
        StructField("customer_id", IntegerType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("country", StringType(), True),
    ]
)

orders_schema = StructType(
    [
        StructField("order_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("order_date", DateType(), False),
        StructField("amount", FloatType(), False),
    ]
)
