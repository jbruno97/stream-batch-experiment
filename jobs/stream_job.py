import argparse
import json
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, current_timestamp, from_json, unix_timestamp
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Structured Streaming experiment job")
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--topic", default="taxi-topic")
    parser.add_argument("--starting-offsets", default="earliest")
    parser.add_argument("--duration-sec", type=int, default=30)
    parser.add_argument("--trigger-sec", type=int, default=2)
    parser.add_argument("--scenario", default="S1")
    parser.add_argument("--run-id", default="run-0")
    return parser.parse_args()


def parse_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("stream_experiment").getOrCreate()

    schema = StructType(
        [
            StructField("VendorID", IntegerType(), True),
            StructField("tpep_pickup_datetime", StringType(), True),
            StructField("tpep_dropoff_datetime", StringType(), True),
            StructField("passenger_count", IntegerType(), True),
            StructField("trip_distance", StringType(), True),
            StructField("RatecodeID", StringType(), True),
            StructField("store_and_fwd_flag", StringType(), True),
            StructField("PULocationID", IntegerType(), True),
            StructField("DOLocationID", IntegerType(), True),
            StructField("payment_type", IntegerType(), True),
            StructField("fare_amount", StringType(), True),
            StructField("extra", StringType(), True),
            StructField("mta_tax", StringType(), True),
            StructField("tip_amount", StringType(), True),
            StructField("tolls_amount", StringType(), True),
            StructField("improvement_surcharge", StringType(), True),
            StructField("total_amount", StringType(), True),
            StructField("congestion_surcharge", StringType(), True),
            StructField("Airport_fee", StringType(), True),
            StructField("event_time_ms", LongType(), True),
        ]
    )

    source_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    json_df = source_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    with_latency_df = (
        json_df.withColumn("processing_time_s", unix_timestamp(current_timestamp()))
        .withColumn("event_time_s", col("event_time_ms") / 1000.0)
        .withColumn("latency_s", col("processing_time_s") - col("event_time_s"))
    )
    result_df = with_latency_df.groupBy("PULocationID").agg(
        avg(col("fare_amount").cast("double")).alias("avg_fare_amount")
    )

    query = (
        result_df.writeStream.outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime=f"{args.trigger_sec} seconds")
        .start()
    )

    start_time = time.time()
    seen_batch_ids = set()
    input_rps_values = []
    processed_rps_values = []
    trigger_latency_ms_values = []
    total_input_rows = 0
    micro_batch_count = 0

    while (time.time() - start_time) < args.duration_sec:
        progress = query.lastProgress
        if progress:
            batch_id = progress.get("batchId")
            if batch_id not in seen_batch_ids:
                seen_batch_ids.add(batch_id)
                micro_batch_count += 1
                total_input_rows += int(progress.get("numInputRows", 0))
                input_rps_values.append(parse_float(progress.get("inputRowsPerSecond")))
                processed_rps_values.append(parse_float(progress.get("processedRowsPerSecond")))
                duration_ms = progress.get("durationMs", {})
                trigger_latency_ms_values.append(parse_float(duration_ms.get("triggerExecution")))
        time.sleep(1)

    query.stop()
    elapsed_sec = time.time() - start_time

    avg_input_rps = sum(input_rps_values) / len(input_rps_values) if input_rps_values else 0.0
    avg_processed_rps = sum(processed_rps_values) / len(processed_rps_values) if processed_rps_values else 0.0
    avg_trigger_latency_ms = (
        sum(trigger_latency_ms_values) / len(trigger_latency_ms_values) if trigger_latency_ms_values else 0.0
    )
    throughput_rows_per_sec = (total_input_rows / elapsed_sec) if elapsed_sec > 0 else 0.0

    metrics = {
        "run_id": args.run_id,
        "scenario": args.scenario,
        "topic": args.topic,
        "duration_sec": float(elapsed_sec),
        "configured_duration_sec": int(args.duration_sec),
        "trigger_sec": int(args.trigger_sec),
        "total_input_rows": int(total_input_rows),
        "micro_batch_count": int(micro_batch_count),
        "avg_input_rows_per_sec": float(avg_input_rps),
        "avg_processed_rows_per_sec": float(avg_processed_rps),
        "avg_trigger_latency_ms": float(avg_trigger_latency_ms),
        "throughput_rows_per_sec": float(throughput_rows_per_sec),
        "timestamp_unix": int(time.time()),
    }

    print("METRICS_JSON:" + json.dumps(metrics, sort_keys=True))
    spark.stop()


if __name__ == "__main__":
    main()
