#!/usr/bin/env bash
# phase2.sh: (a) Gluten's equality-delete cells again with the same 40g heap as Spark (Gluten falls
# back to Spark's reader there, which ran out of a 16g heap); (b) Comet on eq10k (10k equality
# deletes) to see how its equality-delete path scales; (c) GPU and CPU twin on the same tables in
# this session; (d) bin-pack rewrite_data_files for spark, comet (native Iceberg write on), gluten.
set -u
cd "$(dirname "$0")"
BENCH=$HOME/iceberg-pr-hooks/tornadovm/spark_bench.py
WH=$HOME/iceberg-gpu/wh
cell() { # tag engine query runs table [env...]
  local tag=$1 e=$2 q=$3 runs=$4 t=$5; shift 5
  local log="logs/$tag-$q-$t.txt" start=$SECONDS
  env "$@" timeout "${CAP:-600}" ./run-engine.sh "$e" "$BENCH" "$WH" "$q" "$runs" "$t" > "$log" 2>&1
  local rc=$?
  grep -E "^$t,$e,$q," "$log" | sed "s/^$t,$e,/$t,$tag,/" >> results.csv
  echo "$tag,$q,$t,rc=$rc,$((SECONDS - start))s" | tee -a cells.txt
}
for q in q1 q2; do for t in eq1m eq10m; do cell gluten-heap40 gluten $q 6 $t GHEAP=40g; done; done
for q in q1 q2; do cell spark spark $q 6 eq10k; cell comet comet $q 6 eq10k; done
for m in gpu cpu; do for q in q1 q2; do
  start=$SECONDS
  timeout 900 ./gpu-cpu.sh $m $q 6 none pos05 dv05 eq1m eq10m > logs/$m-$q.txt 2>&1
  echo "$m,$q,all,rc=$?,$((SECONDS - start))s" | tee -a cells.txt
done; done
for t in none pos05 eq1m; do
  cell spark spark rewrite 4 $t
  cell comet comet rewrite 4 $t COMET_EXTRA="--conf spark.comet.write.iceberg.enabled=true"
  cell gluten gluten rewrite 4 $t
done
echo PHASE2 DONE | tee -a cells.txt
