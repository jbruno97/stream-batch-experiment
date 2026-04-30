"""
Structured Streaming job: Kafka -> agregação por micro-batch.
"""
import argparse
import json
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, current_timestamp, from_json, unix_timestamp
from pyspark.sql.types import (
    IntegerType, LongType, StringType, StructField, StructType,
)

PROGRESS_PREFIX = "STREAM_PROGRESS_JSON:"
BATCH_METRICS_PREFIX = "STREAM_BATCH_METRICS_JSON:"

_SCHEMA = StructType([
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
])


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap-servers", default="kafka:9092")
    p.add_argument("--topic", default="taxi-topic")
    p.add_argument("--starting-offsets", default="earliest")
    p.add_argument("--duration-sec", type=int, default=30)
    p.add_argument("--trigger-sec", type=int, default=2)
    p.add_argument("--scenario", default="S1")
    p.add_argument("--run-id", default="run-0")
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--rate-eps", type=int, default=0,
                   help="Taxa esperada eventos/s — usada para calcular maxOffsetsPerTrigger")
    return p.parse_args()


def _f(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def main() -> None:
    args = parse_args()

    # maxOffsetsPerTrigger derivado da taxa real do cenário (headroom 2x);
    # evita que micro-batches acumulem backlog ilimitado em alta carga.
    max_offsets = (args.rate_eps * args.trigger_sec * 2) if args.rate_eps > 0 else (args.trigger_sec * 10_000)

    spark = SparkSession.builder.appName("stream_experiment").getOrCreate()

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", max_offsets)
        .load()
    )

    parsed = (
        raw.select(from_json(col("value").cast("string"), _SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("proc_s", unix_timestamp(current_timestamp()))
        .withColumn("event_s", col("event_time_ms") / 1000.0)
        .withColumn("latency_s", col("proc_s") - col("event_s"))
    )

    scenario_id = args.scenario
    run_id = args.run_id

    def process_batch(batch_df, batch_id: int) -> None:
        # Materializa uma vez: conta + agrega em cache
        batch_df.cache()
        stats = batch_df.selectExpr(
            "count(*) as n",
            "avg(latency_s) as avg_lat",
            "max(latency_s) as max_lat",
        ).collect()[0]
        n = int(stats["n"])
        avg_lat = float(stats["avg_lat"] or 0.0)
        max_lat = float(stats["max_lat"] or 0.0)

        if n > 0:
            # Agregação de negócio — aproveita o cache
            batch_df.groupBy("PULocationID").agg(
                avg(col("fare_amount").cast("double")).alias("avg_fare")
            ).count()  # força execução sem materializar resultado

        batch_df.unpersist()
        print(BATCH_METRICS_PREFIX + json.dumps({
            "run_id": run_id,
            "scenario": scenario_id,
            "batch_id": int(batch_id),
            "avg_latency_s": avg_lat,
            "max_latency_s": max_lat,
            "rows_processed": n,
        }, sort_keys=True))

    query = (
        parsed.writeStream
        .outputMode("append")
        .foreachBatch(process_batch)
        .trigger(processingTime=f"{args.trigger_sec} seconds")
        .start()
    )

    start = time.time()
    seen: set = set()
    in_rps, proc_rps, trig_ms = [], [], []
    total_rows = 0

    while (time.time() - start) < args.duration_sec:
        prog = query.lastProgress
        if prog:
            bid = prog.get("batchId")
            if bid not in seen:
                seen.add(bid)
                dm = prog.get("durationMs", {})
                payload = {
                    "run_id": run_id,
                    "scenario": scenario_id,
                    "workers": int(args.workers),
                    "batch_id": int(bid),
                    "timestamp": prog.get("timestamp", ""),
                    "num_input_rows": int(prog.get("numInputRows", 0)),
                    "input_rows_per_second": _f(prog.get("inputRowsPerSecond")),
                    "processed_rows_per_second": _f(prog.get("processedRowsPerSecond")),
                    "duration_add_batch_ms": _f(dm.get("addBatch")),
                    "duration_trigger_execution_ms": _f(dm.get("triggerExecution")),
                    "duration_query_planning_ms": _f(dm.get("queryPlanning")),
                    "backpressure_ms": max(_f(dm.get("triggerExecution")) - args.trigger_sec * 1000.0, 0.0),
                    "state_operator_rows_total": sum(
                        int(op.get("numRowsTotal", 0)) for op in prog.get("stateOperators", [])
                    ),
                }
                print(PROGRESS_PREFIX + json.dumps(payload, sort_keys=True))
                total_rows += int(prog.get("numInputRows", 0))
                in_rps.append(_f(prog.get("inputRowsPerSecond")))
                proc_rps.append(_f(prog.get("processedRowsPerSecond")))
                trig_ms.append(_f(dm.get("triggerExecution")))
        time.sleep(0.5)  # poll mais leve

    query.stop()
    elapsed = time.time() - start

    def _avg(lst): return sum(lst) / len(lst) if lst else 0.0

    metrics = {
        "run_id": run_id,
        "scenario": scenario_id,
        "workers": int(args.workers),
        "duration_sec": float(elapsed),
        "configured_duration_sec": int(args.duration_sec),
        "trigger_sec": int(args.trigger_sec),
        "total_input_rows": int(total_rows),
        "micro_batch_count": len(seen),
        "max_offsets_per_trigger": int(max_offsets),
        "avg_input_rows_per_sec": _avg(in_rps),
        "avg_processed_rows_per_sec": _avg(proc_rps),
        "avg_trigger_latency_ms": _avg(trig_ms),
        "spark_version": spark.version,
        "throughput_rows_per_sec": total_rows / elapsed if elapsed > 0 else 0.0,
        "timestamp_unix": int(time.time()),
    }
    print("METRICS_JSON:" + json.dumps(metrics, sort_keys=True))
    spark.stop()


if __name__ == "__main__":
    main()
