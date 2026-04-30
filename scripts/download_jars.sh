#!/usr/bin/env bash
# Baixa o conector spark-sql-kafka para ./jars/ uma única vez.
# Execute antes do primeiro experimento: bash scripts/download_jars.sh
set -euo pipefail

JAR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/jars"
mkdir -p "$JAR_DIR"

PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
IVY_CACHE="/tmp/.ivy-download"

echo "Baixando $PACKAGE para $JAR_DIR ..."

docker run --rm \
  -v "$JAR_DIR:/opt/jars" \
  -v "$IVY_CACHE:/tmp/.ivy" \
  apache/spark:3.5.1 \
  /opt/spark/bin/spark-submit \
    --packages "$PACKAGE" \
    --conf "spark.jars.ivy=/tmp/.ivy" \
    --conf "spark.jars.packages.resolveAlways=false" \
    /dev/null 2>/dev/null || true

# Copia os JARs resolvidos para ./jars
find "$IVY_CACHE" -name "*.jar" -exec cp -n {} "$JAR_DIR/" \; 2>/dev/null || true

echo "JARs disponíveis em $JAR_DIR:"
ls "$JAR_DIR"/*.jar 2>/dev/null || echo "  (nenhum JAR encontrado — verifique conectividade)"
