import argparse
import json
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, current_timestamp, from_json, unix_timestamp
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType


PROGRESS_PREFIX = "STREAM_PROGRESS_JSON:"
BATCH_METRICS_PREFIX = "STREAM_BATCH_METRICS_JSON:"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Structured Streaming experiment job")
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--topic", default="taxi-topic")
    parser.add_argument("--starting-offsets", default="earliest")
    parser.add_argument("--duration-sec", type=int, default=30)
    parser.add_argument("--trigger-sec", type=int, default=2)
    parser.add_argument("--scenario", default="S1")
    parser.add_argument("--run-id", default="run-0")
    parser.add_argument("--workers", type=int, default=1)
    return parser.parse_args()


def parse_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("stream_experiment").getOrCreate()

    # O schema espelha o payload do producer para que o Spark calcule latência e throughput em colunas tipadas.
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
    def process_microbatch(batch_df, batch_id: int) -> None:
        # foreachBatch permite expor métricas de latência por evento que o progress do Spark não fornece sozinho.
        batch_count = batch_df.count()
        if batch_count == 0:
            payload = {
                "run_id": args.run_id,
                "scenario": args.scenario,
                "topic": args.topic,
                "batch_id": int(batch_id),
                "avg_latency_s": 0.0,
                "max_latency_s": 0.0,
                "rows_processed": 0,
            }
            print(BATCH_METRICS_PREFIX + json.dumps(payload, sort_keys=True))
            return

        avg_latency_s = batch_df.selectExpr("avg(latency_s) as avg_latency_s").collect()[0]["avg_latency_s"] or 0.0
        max_latency_s = batch_df.selectExpr("max(latency_s) as max_latency_s").collect()[0]["max_latency_s"] or 0.0
        batch_df.groupBy("PULocationID").agg(avg(col("fare_amount").cast("double")).alias("avg_fare_amount")).count()
        payload = {
            "run_id": args.run_id,
            "scenario": args.scenario,
            "topic": args.topic,
            "batch_id": int(batch_id),
            "avg_latency_s": float(avg_latency_s),
            "max_latency_s": float(max_latency_s),
            "rows_processed": int(batch_count),
        }
        print(BATCH_METRICS_PREFIX + json.dumps(payload, sort_keys=True))

    query = (
        with_latency_df.writeStream.outputMode("append")
        .foreachBatch(process_microbatch)
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
                duration_ms = progress.get("durationMs", {})
                progress_payload = {
                    "run_id": args.run_id,
                    "scenario": args.scenario,
                    "topic": args.topic,
                    "workers": int(args.workers),
                    "batch_id": int(batch_id),
                    "timestamp": progress.get("timestamp", ""),
                    "num_input_rows": int(progress.get("numInputRows", 0)),
                    "input_rows_per_second": float(parse_float(progress.get("inputRowsPerSecond"))),
                    "processed_rows_per_second": float(parse_float(progress.get("processedRowsPerSecond"))),
                    "duration_add_batch_ms": float(parse_float(duration_ms.get("addBatch"))),
                    "duration_commit_offsets_ms": float(parse_float(duration_ms.get("commitOffsets"))),
                    "duration_get_batch_ms": float(parse_float(duration_ms.get("getBatch"))),
                    "duration_latest_offset_ms": float(parse_float(duration_ms.get("latestOffset"))),
                    "duration_query_planning_ms": float(parse_float(duration_ms.get("queryPlanning"))),
                    "duration_trigger_execution_ms": float(parse_float(duration_ms.get("triggerExecution"))),
                    "duration_wal_commit_ms": float(parse_float(duration_ms.get("walCommit"))),
                    "state_operator_rows_total": 0,
                    "sources_count": len(progress.get("sources", [])),
                    "sink_description": progress.get("sink", {}).get("description", ""),
                    "backpressure_ms": max(float(parse_float(duration_ms.get("triggerExecution"))) - (args.trigger_sec * 1000.0), 0.0),
                }
                state_operators = progress.get("stateOperators", [])
                if state_operators:
                    progress_payload["state_operator_rows_total"] = int(
                        sum(int(operator.get("numRowsTotal", 0)) for operator in state_operators)
                    )
                # Esta linha é consumida pelo runner e vira o dataset bruto por micro-batch.
                print(PROGRESS_PREFIX + json.dumps(progress_payload, sort_keys=True))
                micro_batch_count += 1
                total_input_rows += int(progress.get("numInputRows", 0))
                input_rps_values.append(parse_float(progress.get("inputRowsPerSecond")))
                processed_rps_values.append(parse_float(progress.get("processedRowsPerSecond")))
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
        "workers": int(args.workers),
        "duration_sec": float(elapsed_sec),
        "configured_duration_sec": int(args.duration_sec),
        "trigger_sec": int(args.trigger_sec),
        "total_input_rows": int(total_input_rows),
        "micro_batch_count": int(micro_batch_count),
        "avg_input_rows_per_sec": float(avg_input_rps),
        "avg_processed_rows_per_sec": float(avg_processed_rps),
        "avg_trigger_latency_ms": float(avg_trigger_latency_ms),
        "spark_version": spark.version,
        "throughput_rows_per_sec": float(throughput_rows_per_sec),
        "timestamp_unix": int(time.time()),
    }

    # Esta linha é consumida pelo runner e vira o resumo da repetição no cenário.
    print("METRICS_JSON:" + json.dumps(metrics, sort_keys=True))
    spark.stop()


if __name__ == "__main__":
    main()
