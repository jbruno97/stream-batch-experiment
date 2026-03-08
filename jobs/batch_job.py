import argparse
import json
import time

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch aggregation experiment job")
    parser.add_argument("--input", default="/opt/data/small/input.csv")
    parser.add_argument("--scenario", default="small")
    parser.add_argument("--run-id", default="run-0")
    parser.add_argument("--show-limit", type=int, default=20)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("batch_experiment").getOrCreate()

    started_at = time.time()
    df = spark.read.option("header", True).csv(args.input)
    total_rows = df.count()

    result = df.groupBy("category").count()
    result.show(args.show_limit, truncate=False)

    duration_sec = time.time() - started_at
    throughput_rows_per_sec = (total_rows / duration_sec) if duration_sec > 0 else 0.0

    metrics = {
        "run_id": args.run_id,
        "scenario": args.scenario,
        "input": args.input,
        "row_count": int(total_rows),
        "duration_sec": float(duration_sec),
        "throughput_rows_per_sec": float(throughput_rows_per_sec),
        "timestamp_unix": int(time.time()),
    }

    print(f"Execution Time: {duration_sec}")
    print("METRICS_JSON:" + json.dumps(metrics, sort_keys=True))
    spark.stop()


if __name__ == "__main__":
    main()
