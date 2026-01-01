import argparse
import os
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True, help="Base output folder")
    p.add_argument("--ds", required=True, help="YYYY-MM-DD partition date")
    p.add_argument("--users", type=int, default=2000)
    p.add_argument("--min_points", type=int, default=60)
    p.add_argument("--max_points", type=int, default=240)

    # Skew controls
    p.add_argument(
        "--skew_user_prob",
        type=float,
        default=0.02,
        help="Probability a user is 'heavy'",
    )
    p.add_argument(
        "--skew_multiplier",
        type=int,
        default=40,
        help="Heavy users have N times more points",
    )

    p.add_argument("--seed", type=int, default=42)
    return p.parse_args()


def gen_user_series(rng: np.random.Generator, n: int):
    """
    Create a 1D series with 1-3 change points via mean shifts and mild trend changes.
    """
    # number of segments
    k = int(rng.integers(2, 5))  # 2..4 segments
    # pick breakpoint positions (sorted)
    cps = sorted(rng.choice(np.arange(10, n - 10), size=k - 1, replace=False).tolist())
    cps = cps + [n]

    series = []
    start = 0
    base = rng.normal(0.0, 1.0)

    for seg_i, end in enumerate(cps, start=1):
        seg_len = end - start

        # mean shift and trend vary by segment
        mean = base + rng.normal(0.0, 2.0) + seg_i * rng.normal(0.2, 0.3)
        trend = rng.normal(0.0, 0.02)  # mild trend
        noise = rng.normal(0.0, 1.0, size=seg_len)

        t = np.arange(seg_len, dtype=np.float64)
        seg = mean + trend * t + noise
        series.append(seg)

        start = end

    x = np.concatenate(series)
    return x


def main():
    args = parse_args()
    random.seed(args.seed)
    np.random.seed(args.seed)
    rng = np.random.default_rng(args.seed)

    ds_date = datetime.strptime(args.ds, "%Y-%m-%d")
    # out_dir = os.path.join(args.out, f"ds={args.ds}")
    out_dir = args.out
    os.makedirs(out_dir, exist_ok=True)

    rows = []
    for i in range(args.users):
        user_id = f"u{i:06d}"

        n = int(rng.integers(args.min_points, args.max_points + 1))

        # introduce skew: a few heavy users
        if rng.random() < args.skew_user_prob:
            n = n * args.skew_multiplier

        values = gen_user_series(rng, n)

        # timestamps: 1-min increments within the day (wrap if long)
        start_ts = ds_date
        for j in range(n):
            ts = start_ts + timedelta(minutes=(j % (24 * 60)))
            rows.append((user_id, ts, float(values[j]), args.ds))

    pdf = pd.DataFrame(rows, columns=["user_id", "ts", "value", "ds"])

    # Write Parquet (requires pyarrow or fastparquet locally)

    out_path = os.path.join(out_dir, "events.parquet")

    pdf["ts"] = pd.to_datetime(pdf["ts"]).astype("datetime64[us]")

    pdf.to_parquet(
        out_path,
        index=False,
        engine="pyarrow",
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )

    print(f"Wrote {len(pdf):,} rows to {out_path}")


if __name__ == "__main__":
    main()
