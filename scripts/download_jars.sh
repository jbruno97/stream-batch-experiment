#!/usr/bin/env bash
# Baixa o conector spark-sql-kafka para ./jars/ uma única vez.
# Execute antes do primeiro experimento: bash scripts/download_jars.sh
set -euo pipefail

JAR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/jars"
mkdir -p "$JAR_DIR"

PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
IVY_CACHE="${IVY_CACHE:-/tmp/stream-batch-ivy-$(id -u)}"

echo "Baixando $PACKAGE para $JAR_DIR ..."

mkdir -p "$IVY_CACHE/cache" "$IVY_CACHE/jars"
chmod -R ugo+rwX "$IVY_CACHE"

docker run --rm \
  -v "$JAR_DIR:/opt/jars" \
  -v "$IVY_CACHE:/tmp/.ivy" \
  apache/spark:3.5.1 \
  bash -lc "printf 'print(\"jar resolution noop\")\n' > /tmp/noop.py && \
    /opt/spark/bin/spark-submit \
      --packages '$PACKAGE' \
      --conf 'spark.jars.ivy=/tmp/.ivy' \
      --conf 'spark.jars.packages.resolveAlways=false' \
      /tmp/noop.py"

# Copia os JARs resolvidos para ./jars com os nomes esperados pelo runner.
find "$IVY_CACHE/jars" -maxdepth 1 -name "*.jar" -exec cp -f {} "$JAR_DIR/" \;

echo "JARs disponíveis em $JAR_DIR:"
if ! ls "$JAR_DIR"/*.jar >/dev/null 2>&1; then
  echo "  nenhum JAR encontrado" >&2
  echo "Verifique conectividade com Maven Central ou defina IVY_CACHE para um diretório gravável." >&2
  exit 1
fi

ls "$JAR_DIR"/*.jar
