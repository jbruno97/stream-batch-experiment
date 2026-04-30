"""
Batch job: leitura + agregação em scan único.
"""
import argparse
import json
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="/opt/data/samples/1gb")
    p.add_argument("--scenario", default="B2")
    p.add_argument("--run-id", default="run-0")
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--show-limit", type=int, default=20)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("batch_experiment").getOrCreate()

    started_at = time.time()
    df = spark.read.parquet(args.input)

    # count() + avg() em um único groupBy — um scan só.
    result = (
        df.groupBy("PULocationID")
        .agg(
            avg(col("fare_amount").cast("double")).alias("avg_fare_amount"),
            count("*").alias("row_count"),
        )
    )
    result.show(args.show_limit, truncate=False)
    total_rows = result.agg({"row_count": "sum"}).collect()[0][0] or 0

    duration_sec = time.time() - started_at
    metrics = {
        "run_id": args.run_id,
        "scenario": args.scenario,
        "input": args.input,
        "workers": int(args.workers),
        "row_count": int(total_rows),
        "duration_sec": float(duration_sec),
        "spark_version": spark.version,
        "throughput_rows_per_sec": float(total_rows / duration_sec) if duration_sec > 0 else 0.0,
        "timestamp_unix": int(time.time()),
    }
    print(f"Execution Time: {duration_sec}")
    print("METRICS_JSON:" + json.dumps(metrics, sort_keys=True))
    spark.stop()


if __name__ == "__main__":
    main()
