import argparse
import pandas as pd
from schema.schemas import change_point_schema, change_point_out_schema
from pyspark.sql import SparkSession, functions as F
import ruptures as rpt


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--ds", required=True, help="Partition date string YYYY-MM-DD")
    return p.parse_args()


def detect_change_points_factory(ds: str, model: str = "l2", penalty: float = 10.0):
    """
    Returns a function suitable for groupby().applyInPandas(...)
    """

    def detect_change_points(pdf: pd.DataFrame) -> pd.DataFrame:
        #import pyarrow as pa
        # pdf: rows for a single user_id
        if pdf.empty:
            return pd.DataFrame(
                columns=[f.name for f in change_point_out_schema.fields]
            )

        pdf = pdf.sort_values("ts")
        x = pdf["value"].to_numpy()

        # Guard: too short series => no change points
        # (You can tweak this for the meetup)
        if len(x) < 10:
            return pd.DataFrame(
                columns=[f.name for f in change_point_out_schema.fields]
            )

        # ruptures expects 1D or 2D numeric array
        algo = rpt.Pelt(model=model).fit(x)
        bkps = algo.predict(pen=penalty)

        # bkps are segment end indices; last one is len(x) => ignore it as "end"
        rows = []
        seg_id = 0
        for cp in bkps[:-1]:
            seg_id += 1
            idx = int(cp) - 1  # convert segment end to 0-based index into pdf
            idx = max(0, min(idx, len(pdf) - 1))
            rows.append(
                {
                    "ds": ds,
                    "user_id": str(pdf.iloc[0]["user_id"]),
                    "change_point_ts": pdf.iloc[idx]["ts"],
                    "cp_index": int(cp),
                    "segment_id": seg_id,
                    "model": model,
                    "penalty": float(penalty),
                }
            )

        return pd.DataFrame(
            rows, columns=[f.name for f in change_point_out_schema.fields]
        )

    return detect_change_points


def main():
    args = parse_args()

    spark = SparkSession.builder.appName("meetup-change-point-ruptures").getOrCreate()

    # (For attendees to fill)
    # ====== MEETUP TASK (Optimization): enable Arrow ======
    # Attendees should add:
    #spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    #
    # Optional: force Arrow (so it errors rather than silently fallback)
    #spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
    #
    # Optional: batch tuning
    #spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "5000")
    #spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
    #spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "50000")
    # ======================================================

    # Input data is Parquet created by generator; schema: user_id, ts, value, ds
    df = spark.read.schema(change_point_schema).parquet(
        "file:/opt/airflow/data/secondary/events.parquet"
    )

    # Basic sanity + select only what we need
    df = df.select("user_id", "ts", "value", "ds").where(F.col("ds") == args.ds)

    # Partitioning helps stability; not the focus, but harmless:
    # df = df.repartition("user_id")

    # Grouped Pandas UDF — this is where Arrow matters a lot
    detect_fn = detect_change_points_factory(ds=args.ds, model="l2", penalty=10.0)

    result = df.groupBy("user_id").applyInPandas(
        detect_fn, schema=change_point_out_schema
    )

    # Write output
    (
        result.repartition(1)  # small output; keep it simple for meetup
        .write.mode("overwrite")
        .parquet("file:///opt/airflow/data/processed/")
    )

    print("Arrow enabled:", spark.conf.get("spark.sql.execution.arrow.pyspark.enabled"))
    print("Arrow fallback:", spark.conf.get("spark.sql.execution.arrow.pyspark.fallback.enabled"))
    print("Arrow batch:", spark.conf.get("spark.sql.execution.arrow.maxRecordsPerBatch"))

    spark.stop()


if __name__ == "__main__":
    main()
