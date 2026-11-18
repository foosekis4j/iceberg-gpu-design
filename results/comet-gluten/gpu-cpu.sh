#!/usr/bin/env bash
# gpu-cpu.sh <gpu|cpu> <query> <runs> <table>...: the PR's `run.sh gpu|cpu` (MorBench) with the
# already-built classpath of the PR worktree (no Gradle run), TornadoVM 8.1.0-jdk21-cuda, libcudf cu13.
set -euo pipefail
MODE=$1; shift
WH=$HOME/iceberg-gpu/wh
export TORNADOVM_HOME=$HOME/.sdkman/candidates/tornadovm/8.1.0-jdk21-cuda
export JAVA_HOME=$HOME/.sdkman/candidates/java/21.0.2-open
export LD_LIBRARY_PATH="$(cat "$HOME/iceberg-gpu/cudf-root-cu13/ld_library_path")${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
CP=$(cat "$HOME/iceberg-pr-hooks/tornadovm/build/runtime-classpath.txt")
case $MODE in
  gpu)
    export LIBCUDF_HOST_DECOMPRESSION=HYBRID
    exec "$JAVA_HOME/bin/java" @"$TORNADOVM_HOME/tornado-argfile" --enable-native-access=ALL-UNNAMED -Xmx24g \
      -Dtornado.device.memory=20GB --add-opens=java.base/java.nio=ALL-UNNAMED \
      -da:tornado... -da:uk.ac.manchester.tornado... -cp "$CP" \
      org.apache.iceberg.tornadovm.bench.MorBench "$WH" gpu "$@" ;;
  cpu)
    CP="$CP:$(ls "$TORNADOVM_HOME"/share/java/tornado/tornado-{api,cudf}-*.jar | tr '\n' ':')"
    exec "$JAVA_HOME/bin/java" -Xmx40g --enable-preview --add-opens=java.base/java.nio=ALL-UNNAMED -Dthreads=32 \
      -cp "$CP" org.apache.iceberg.tornadovm.bench.MorBench "$WH" cpu "$@" ;;
esac
