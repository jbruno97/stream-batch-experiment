import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, unix_timestamp
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType


RESULTS_FILE = os.getenv("RESULTS_FILE", "/opt/results/stream_results.csv")
LATENCY_FILE = os.getenv("LATENCY_FILE", "/opt/results/latency_results.csv")
RUN_ID = os.getenv("RUN_ID", "stream_run")
SCENARIO = os.getenv("SCENARIO", "S1")


def main() -> None:
    # Inicializa sessao Spark para Structured Streaming.
    spark = SparkSession.builder.appName("TaxiStream").getOrCreate()

    # Schema esperado para os eventos JSON enviados ao Kafka.
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

    df = (
        spark.readStream.format("kafka")
        # Dentro do Docker, o broker e acessado por "kafka:9092".
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "taxi-topic")
        .option("startingOffsets", "latest")
        .load()
    )

    # Converte payload do Kafka (bytes) para colunas estruturadas.
    parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Calcula latencia aproximada por evento.
    with_latency = (
        parsed.withColumn("processing_time_s", unix_timestamp(current_timestamp()))
        .withColumn("event_time_s", col("event_time_ms") / 1000.0)
        .withColumn("latency_s", col("processing_time_s") - col("event_time_s"))
    )

    def write_metrics(batch_df, batch_id: int) -> None:
        # Cada microbatch gera uma linha em stream_results + latencias em arquivo dedicado.
        count = batch_df.count()
        if count == 0:
            return

        avg_latency = batch_df.selectExpr("avg(latency_s) as avg_latency").collect()[0]["avg_latency"]
        os.makedirs("/opt/results", exist_ok=True)

        if not os.path.exists(RESULTS_FILE):
            with open(RESULTS_FILE, "w", encoding="utf-8") as file:
                file.write("run_id,scenario,batch_id,records,avg_latency_s\n")

        with open(RESULTS_FILE, "a", encoding="utf-8") as file:
            file.write(f"{RUN_ID},{SCENARIO},{batch_id},{count},{avg_latency}\n")

        latencies = batch_df.select("latency_s").collect()
        if not os.path.exists(LATENCY_FILE):
            with open(LATENCY_FILE, "w", encoding="utf-8") as file:
                file.write("run_id,scenario,batch_id,latency_s\n")

        with open(LATENCY_FILE, "a", encoding="utf-8") as file:
            for row in latencies:
                file.write(f"{RUN_ID},{SCENARIO},{batch_id},{row['latency_s']}\n")

    # Inicia query com coleta de metricas por microbatch.
    query = with_latency.writeStream.outputMode("append").foreachBatch(write_metrics).start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
