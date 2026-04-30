import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from parquet_utils import read_parquet_with_type_normalization


TARGET_SPECS = [
    ("200mb", 200 * 1024 * 1024),
    ("1gb", 1 * 1024 * 1024 * 1024),
    ("3gb", 3 * 1024 * 1024 * 1024),
    ("10gb", 10 * 1024 * 1024 * 1024),
]

TARGET_PARTITIONS = {
    "200mb": 4,
    "1gb": 8,
    "3gb": 16,
    "10gb": 24,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create parquet samples from the raw NYC Taxi dataset")
    parser.add_argument("--input", default="data/raw/nyc_taxi")
    parser.add_argument("--output-root", default="data/samples")
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def estimate_input_size_bytes(input_path: Path) -> int:
    # O volume bruto em parquet é usado como proxy para dimensionar as amostras derivadas do experimento.
    return sum(path.stat().st_size for path in input_path.rglob("*.parquet") if path.is_file())


def build_target_sample(df: DataFrame, raw_size_bytes: int, target_size_bytes: int, seed: int) -> DataFrame:
    if raw_size_bytes <= 0:
        return df

    if target_size_bytes <= raw_size_bytes:
        fraction = max(min(target_size_bytes / raw_size_bytes, 1.0), 0.0001)
        return df.sample(withReplacement=False, fraction=fraction, seed=seed)

    full_repeats = max(target_size_bytes // raw_size_bytes, 1)
    remainder_bytes = target_size_bytes % raw_size_bytes
    combined = df
    for _ in range(int(full_repeats) - 1):
        combined = combined.unionByName(df)

    if remainder_bytes > 0:
        fraction = max(min(remainder_bytes / raw_size_bytes, 1.0), 0.0001)
        combined = combined.unionByName(df.sample(withReplacement=False, fraction=fraction, seed=seed))
    return combined


def write_target_sample(
    df: DataFrame,
    target_dir: Path,
    raw_size_bytes: int,
    target_size_bytes: int,
    seed: int,
    partition_count: int,
) -> None:
    # Alvos grandes são gravados incrementalmente para evitar materializar toda a expansão sintética em memória.
    if target_size_bytes <= raw_size_bytes:
        sample_df = build_target_sample(df, raw_size_bytes, target_size_bytes, seed).coalesce(partition_count)
        sample_df.write.mode("overwrite").parquet(str(target_dir))
        return

    full_repeats = max(int(target_size_bytes // raw_size_bytes), 1)
    remainder_bytes = int(target_size_bytes % raw_size_bytes)
    first_write = True

    for index in range(full_repeats):
        mode = "overwrite" if first_write else "append"
        first_write = False
        df.coalesce(partition_count).write.mode(mode).parquet(str(target_dir))

    if remainder_bytes > 0:
        fraction = max(min(remainder_bytes / raw_size_bytes, 1.0), 0.0001)
        remainder_df = df.sample(withReplacement=False, fraction=fraction, seed=seed).coalesce(partition_count)
        remainder_df.write.mode("append").parquet(str(target_dir))


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_root = Path(args.output_root)
    spark = (
        SparkSession.builder.appName("Create Samples")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        # Essas configurações de escrita reduzem a pressão de memória na construção das amostras grandes.
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("parquet.enable.dictionary", "false")
        .getOrCreate()
    )

    df = read_parquet_with_type_normalization(spark, args.input)
    raw_size_bytes = estimate_input_size_bytes(input_path)

    for index, (folder_name, target_size_bytes) in enumerate(TARGET_SPECS, start=1):
        target_dir = output_root / folder_name
        if not args.force and target_dir.exists() and any(target_dir.rglob("*.parquet")):
            continue
        partition_count = TARGET_PARTITIONS.get(folder_name, 8)
        write_target_sample(
            df=df,
            target_dir=target_dir,
            raw_size_bytes=raw_size_bytes,
            target_size_bytes=target_size_bytes,
            seed=42 + index,
            partition_count=partition_count,
        )

    spark.stop()


if __name__ == "__main__":
    main()
