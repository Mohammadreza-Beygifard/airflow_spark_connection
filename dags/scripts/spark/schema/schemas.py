from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType,
    FloatType,
    TimestampType,
    DoubleType,
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


change_point_schema = StructType(
    [
        StructField("user_id", StringType(), nullable=False),
        StructField("ts", TimestampType(), nullable=False),
        StructField("value", DoubleType(), nullable=False),
        StructField("ds", StringType(), nullable=False),
    ]
)

change_point_out_schema = StructType(
    [
        StructField("ds", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("change_point_ts", TimestampType(), False),
        StructField("cp_index", IntegerType(), False),
        StructField("segment_id", IntegerType(), False),
        StructField("model", StringType(), False),
        StructField("penalty", DoubleType(), False),
    ]
)
