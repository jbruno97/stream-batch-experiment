from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


NUMERIC_CASTS = {
    "passenger_count": "double",
    "trip_distance": "double",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "airport_fee": "double",
    "PULocationID": "bigint",
    "DOLocationID": "bigint",
    "RatecodeID": "bigint",
    "VendorID": "bigint",
    "payment_type": "bigint",
}


def _normalize_types(df: DataFrame) -> DataFrame:
    for column_name, target_type in NUMERIC_CASTS.items():
        if column_name in df.columns:
            df = df.withColumn(column_name, col(column_name).cast(target_type))
    return df


def read_parquet_with_type_normalization(spark: SparkSession, input_path: str) -> DataFrame:
    base_path = Path(input_path)
    if base_path.is_file():
        return _normalize_types(spark.read.parquet(str(base_path)))

    parquet_files = sorted(path for path in base_path.rglob("*.parquet") if path.is_file())
    if not parquet_files:
        return spark.read.parquet(input_path)

    normalized_frames = [_normalize_types(spark.read.parquet(str(path))) for path in parquet_files]
    combined = normalized_frames[0]
    for frame in normalized_frames[1:]:
        combined = combined.unionByName(frame, allowMissingColumns=True)
    return combined
