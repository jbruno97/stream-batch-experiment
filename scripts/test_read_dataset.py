from pyspark.sql import SparkSession


def main() -> None:
    # Sessao local apenas para validar leitura do dataset bruto.
    spark = (
        SparkSession.builder.appName("NYC Taxi Test")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    # Le todos os arquivos parquet da pasta.
    df = spark.read.parquet("data/raw/nyc_taxi")
    print("Total rows:", df.count())
    df.show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
