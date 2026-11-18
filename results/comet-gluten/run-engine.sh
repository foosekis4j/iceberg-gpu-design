#!/usr/bin/env bash
# run-engine.sh <spark|comet|gluten> <script.py> <warehouse> <query> <runs> <table>...
# Spark 3.5.9 local[32] (as run.sh spark), Iceberg runtime ICEBERG_JAR (default 1.10.2).
set -euo pipefail
ENGINE_NAME=$1; SCRIPT=$2; shift 2
T=$HOME/iceberg-gpu/tools
SPARK_HOME=$T/spark-3.5.9-bin-hadoop3
ICEBERG_JAR=${ICEBERG_JAR:-$T/iceberg-spark-runtime-3.5_2.12-1.10.2.jar}
COMET_JAR=$T/cg/comet-spark-spark3.5_2.12-1.2.0.jar
GLUTEN_JAR=$T/cg/apache-gluten-1.7.0-bin-spark-3.5/gluten-velox-bundle-spark3.5_2.12-linux_amd64-1.7.0.jar
OFFHEAP=${OFFHEAP:-24g}; HEAP=${HEAP:-40g}
case $ENGINE_NAME in
  spark) JARS=$ICEBERG_JAR; EXTRA=() ;;
  comet) JARS=$ICEBERG_JAR,$COMET_JAR; HEAP=${CHEAP:-16g}
    EXTRA=(--driver-class-path "$COMET_JAR"
      --conf spark.plugins=org.apache.spark.CometPlugin
      --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager
      --conf spark.comet.explain.fallback.enabled=true
      --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=$OFFHEAP
      ${COMET_EXTRA:-}) ;;
  gluten) JARS=$ICEBERG_JAR,$GLUTEN_JAR; HEAP=${GHEAP:-16g}
    EXTRA=(--driver-class-path "$GLUTEN_JAR:$ICEBERG_JAR"
      --conf spark.plugins=org.apache.gluten.GlutenPlugin
      --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager
      --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=$OFFHEAP
      --conf spark.driver.extraJavaOptions=-Dio.netty.tryReflectionSetAccessible=true
      ${GLUTEN_EXTRA:-}) ;;
esac
export ENGINE=$ENGINE_NAME
exec "$SPARK_HOME/bin/spark-submit" --master "local[32]" --driver-memory "$HEAP" --jars "$JARS" "${EXTRA[@]}" \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse="$(dirname "$1")" \
  --conf spark.ui.enabled=false "$SCRIPT" "$@"
