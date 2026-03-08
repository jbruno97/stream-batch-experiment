import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

def main() -> None:
    # Metadados do experimento (passados por variaveis de ambiente).
    run_id = os.getenv("RUN_ID", "batch_run")
    data_path = os.getenv("DATA_PATH", "/opt/data/samples/1gb")
    results_file = os.getenv("RESULTS_FILE", "/opt/results/batch_results.csv")
    scenario = os.getenv("SCENARIO", "B2")

    # Inicializa sessao Spark.
    spark = SparkSession.builder.appName("TaxiBatch").getOrCreate()

    # Inicio da medicao de tempo.
    start = time.time()
    df = spark.read.parquet(data_path)

    # Workload: media de tarifa por zona de origem.
    result = df.groupBy("PULocationID").agg(avg("fare_amount").alias("avg_fare"))
    result.count()

    # Metricas principais do batch.
    rows = df.count()
    execution_time = time.time() - start
    throughput = (rows / execution_time) if execution_time > 0 else 0.0

    # Garante que o arquivo CSV exista e grava uma linha por execucao.
    os.makedirs(os.path.dirname(results_file), exist_ok=True)
    if not os.path.exists(results_file):
        with open(results_file, "w", encoding="utf-8") as file:
            file.write("run_id,scenario,data_path,rows,execution_time_s,throughput_rps\n")

    with open(results_file, "a", encoding="utf-8") as file:
        file.write(f"{run_id},{scenario},{data_path},{rows},{execution_time},{throughput}\n")

    print(f"RUN_ID: {run_id}")
    print(f"SCENARIO: {scenario}")
    print("Rows:", rows)
    print("Execution Time:", execution_time)
    print("Throughput:", throughput)

    # Finaliza sessao Spark.
    spark.stop()


if __name__ == "__main__":
    main()
