import os
from pathlib import Path

from pyspark.sql import SparkSession

from parquet_utils import read_parquet_with_type_normalization


def dataset_dir() -> str:
    data_root = Path(os.environ.get("DATA_ROOT", "data"))
    default_dataset = data_root / "raw" / "nyc_taxi"
    return os.environ.get("DATASET_DIR", str(default_dataset))


def main() -> None:
    # Sessao local apenas para validar leitura do dataset bruto.
    spark = (
        SparkSession.builder.appName("NYC Taxi Test")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    # Le todos os arquivos parquet da pasta, tolerando drift de tipos entre meses.
    df = read_parquet_with_type_normalization(spark, dataset_dir())
    print("Total rows:", df.count())
    df.show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
