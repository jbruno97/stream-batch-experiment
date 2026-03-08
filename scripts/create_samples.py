from pyspark.sql import SparkSession


def main() -> None:
    # Sessao local para preparar amostras de diferentes tamanhos.
    spark = (
        SparkSession.builder.appName("Create Samples")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    df = spark.read.parquet("data/raw/nyc_taxi")

    # Amostras para os cenarios B1/B2/B3 e para o producer stream.
    small = df.sample(fraction=0.01, seed=42)
    medium = df.sample(fraction=0.05, seed=42)
    large = df.sample(fraction=0.10, seed=42)

    small.write.mode("overwrite").parquet("data/samples/200mb")
    medium.write.mode("overwrite").parquet("data/samples/1gb")
    large.write.mode("overwrite").parquet("data/samples/3gb")

    spark.stop()


if __name__ == "__main__":
    main()
