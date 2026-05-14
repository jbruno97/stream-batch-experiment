#!/usr/bin/env bash

set -euo pipefail

DATA_ROOT="${DATA_ROOT:-data}"
DATASET_DIR="${DATASET_DIR:-${DATA_ROOT}/raw/nyc_taxi}"
NYC_TAXI_MONTHS="${NYC_TAXI_MONTHS:-2023-01 2023-02 2023-03}"

mkdir -p "${DATASET_DIR}"

echo "Downloading NYC Taxi parquet dataset into ${DATASET_DIR}"

for month in ${NYC_TAXI_MONTHS}; do
  file="${DATASET_DIR}/yellow_tripdata_${month}.parquet"
  url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${month}.parquet"

  if [[ -s "${file}" ]]; then
    echo "Already exists: ${file}"
    continue
  fi

  echo "Downloading ${url}"
  wget -c -O "${file}" "${url}"
done

echo "Dataset downloaded successfully."
