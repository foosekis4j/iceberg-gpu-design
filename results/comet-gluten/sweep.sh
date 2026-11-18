#!/usr/bin/env bash
# sweep.sh: one JVM per (engine, query, table) cell, capped at CAP seconds, so a stuck cell is a
# DNF and does not block the rest. q1/q2: 6 runs (first is warm-up); rewrite: 4 runs (first warm-up).
# Same spark_bench.py as the PR's `run.sh spark` (the Spark baseline of the Slack post).
set -u
cd "$(dirname "$0")"
BENCH=$HOME/iceberg-pr-hooks/tornadovm/spark_bench.py
WH=$HOME/iceberg-gpu/wh
CAP=${CAP:-900}
cell() { # engine query runs table
  local log="logs/$1-$2-$4.txt"
  local start=$SECONDS
  timeout "$CAP" ./run-engine.sh "$1" "$BENCH" "$WH" "$2" "$3" "$4" > "$log" 2>&1
  local rc=$?
  grep -E "^$4,$1,$2," "$log" >> results.csv
  echo "$1,$2,$4,rc=$rc,$((SECONDS - start))s" | tee -a cells.txt
}
mkdir -p logs
[ -f results.csv ] || echo "table,engine,query,run,warmup,millis,live_rows,checksum,breakdown" > results.csv
for q in ${QUERIES:-q1 q2}; do
  for t in ${TABLES:-none pos05 dv05 eq1m eq10m}; do
    for e in ${ENGINES:-spark comet gluten}; do cell "$e" "$q" 6 "$t"; done
  done
done
if [ -n "${REWRITE:-}" ]; then
  export COMET_EXTRA="--conf spark.comet.write.iceberg.enabled=true"
  for t in ${REWRITE_TABLES:-none pos05 eq1m}; do
    for e in ${ENGINES:-spark comet gluten}; do cell "$e" rewrite 4 "$t"; done
  done
fi
