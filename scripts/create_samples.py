import argparse

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create parquet samples from the raw NYC Taxi dataset")
    parser.add_argument("--input", default="data/raw/nyc_taxi")
    parser.add_argument("--output-root", default="data/samples")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    # Sessao local para preparar amostras de diferentes tamanhos.
    spark = (
        SparkSession.builder.appName("Create Samples")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    df = spark.read.parquet(args.input)

    # Amostras para os cenarios B1/B2/B3 e para o producer stream.
    small = df.sample(fraction=0.01, seed=42)
    medium = df.sample(fraction=0.05, seed=42)
    large = df.sample(fraction=0.10, seed=42)

    small.write.mode("overwrite").parquet(f"{args.output_root}/200mb")
    medium.write.mode("overwrite").parquet(f"{args.output_root}/1gb")
    large.write.mode("overwrite").parquet(f"{args.output_root}/3gb")

    spark.stop()


if __name__ == "__main__":
    main()
