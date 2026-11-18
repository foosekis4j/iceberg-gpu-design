# Comet and Gluten on the 100M-row merge-on-read suite (2026-11-19, one RTX 4090 workstation)

The same tables, queries and Spark harness as the Slack and PR numbers (`spark_bench.py` from the PR
worktree, `~/iceberg-gpu/wh`, 64 data files and 100M rows). GPU and CPU twin are the PR's MorBench,
run again in this session. All engines run on one machine: an RTX 4090 and 32 threads, `local[32]`.
Warm medians are taken over 5 runs for q1 and q2 and 3 runs for the rewrite, after one warm-up run.
Every completed cell returns the same row count and checksum as the GPU and the CPU twin.

## Versions

- Spark 3.5.9 on JDK 21.0.2, with iceberg-spark-runtime-3.5_2.12 **1.10.2** for Spark, Comet and
  Gluten. Comet documents testing up to Iceberg 1.11, so the same runtime is used everywhere.
- **Comet 1.2.0** (`comet-spark-spark3.5_2.12`, Maven Central). Its native Iceberg scan is on by
  default. The rewrite adds `spark.comet.write.iceberg.enabled=true`.
- **Gluten 1.7.0** (`apache-gluten-1.7.0-bin-spark-3.5`, the Velox bundle, sha512 checked). Gluten
  warns that its fully tested Spark version is 3.5.5.
- GPU: the PR #18898 code, TornadoVM 8.1.0-jdk21-cuda, libcudf cu13.

Configurations are in `run-engine.sh`.
- Spark: driver 40g.
- Comet and Gluten: driver 16g plus 24g off-heap. The `gluten-heap40` rows use a 40g heap.

## Results (warm median, ms)

| case | GPU | CPU twin | Spark | Comet | Gluten |
|---|---|---|---|---|---|
| q1 none | 95 | 233 | 258 | 181 | 174 |
| q1 pos05 | 115 | 329 | 361 | 237 | 185 |
| q1 dv05 (v3 DV) | 99 | 198 | 359 | 228 | 662 F |
| q1 eq1m | 114 | 513 | 3178 | DNF >600 s | 3356 F |
| q1 eq10m | 116 | 902 | OOM | killed (137) | OOM F |
| q2 none | 260 | 862 | 919 | 610 | 445 |
| q2 pos05 | 307 | 993 | 972 | 654 | 472 |
| q2 dv05 (v3 DV) | 266 | 902 | 1025 | 677 | 1126 F |
| q2 eq1m | 271 | 1255 | 4134 | DNF >600 s | 4194 F (5th run OOM; 5528 with 40g heap) |
| q2 eq10m | 269 | 1567 | OOM | killed (137) | OOM F |
| rewrite none | 2784 | – | 3360 | 2865 (native) | 3145 P |
| rewrite pos05 | 3528 | – | 9806 | 3476 (native) | 5209 P |
| rewrite eq1m | 3960 | – | 16371 | DNF >600 s | 12280 P |
| q1 / q2 eq10k (extra) | – | – | 652 / 1431 | 9701 / 9166 | – |

Legend:
- **F:** Gluten's scan fell back to Spark's reader; the aggregation still ran in Velox.
  - DVs: `Delete file format puffin is not supported`.
  - Equality deletes: `Contains equality delete files`.
- **P:** partial offload.
  - Gluten rejects the rewrite's `IcebergStagedScan` and the `AppendData` write, so only the sort
    runs in Velox.
  - Comet runs the whole rewrite natively: `CometIcebergNativeScan`, `CometSort`,
    `CometIcebergWrite`, then Iceberg's commit (plans from the Spark event logs in `eventlogs/`).
- **DNF:** the cell's 600 s cap expired before the first (warm-up) query finished.
  - Comet's native scan holds about 45 GB RSS while working through 1M equality deletes.
  - With 10k equality deletes it takes 9.7 s, against Spark's 0.65 s.
  - Comet planned all of these scans natively (`plans-comet-q2.log`), so these are Comet's own
    times, not fallbacks.
- **killed (137):** SIGKILL about 15 s after start on eq10m, most likely the kernel OOM killer.
  This isn't confirmed: there's no dmesg access.
- **OOM:** `java.lang.OutOfMemoryError: Java heap space` in Spark's reader. This holds at 40g as
  well, and matches the earlier finding that Spark OOMs at eq10m.

## Caveats

- **Delete caches.** The GPU and CPU-twin JVMs cache delete sets and footers across runs; the GPU's
  warm delete load is about 1 ms. Spark, Comet and Gluten re-read deletes on every query. Cold,
  first-run times are in `results.csv` and the `logs/*` files (`warmup=yes`).
- **Sort order.** pos05 and eq1m carry a write order (k, v1), set by an earlier sorted-rewrite
  benchmark. Every engine's bin-pack rewrite therefore sorts within each group, which is why
  Comet's plan shows `CometSort`.
- **GPU rewrite drift.** The GPU rewrite measured here (2.8, 3.5 and 4.0 s) is slower than the PR
  post's 2.94 s (pos05) and 2.88 s (eq1m). This session doesn't explain the gap; the sort order
  above is a candidate. Spark's pos05 and eq1m rewrites (9.8 and 16.4 s) match the post's 9.20
  and 16.18 s.
- **Orphan files.** The killed Comet eq1m rewrite left about 494 MB of uncommitted Parquet files in
  `wh/eq1m/data` (10 files newer than `snapshots-before-rewrite.txt`). They are not deleted. All
  three tables are back on their original snapshots (checked).
- **Other jobs.** An idle TornadoVM JVM from another project held 418 MiB of GPU memory throughout,
  using about 3% CPU.
- **Gluten heap.** Gluten's eq1m fallback reader ran out of a 16g heap on q2's fifth run. The
  `gluten-heap40` rerun completes but is slower (5528 ms), likely from GC.

Related upstream issues (not established as the cause here):
- apache/datafusion-comet#6524: the sorted-merge native Iceberg scan rebuilds the delete-file
  cache per file.
- apache/gluten#13570: the native Iceberg scan drops rows at random, on main. Every Gluten answer
  here was exact.

## Files and commands

- `results.csv`: every run.
- `summary.csv`: warm medians, from `python3 summarize.py`.
- `cells.txt`: exit code and wall time per cell.
- `logs/`: full logs.
- `plans-*.log`: planned operators, without executing.
- `eventlogs/`: rewrite physical plans.
- Commands:
  - `CAP=600 ./sweep.sh`: q1 and q2 for spark, comet and gluten.
  - `./phase2.sh`: Gluten with a 40g heap, eq10k, GPU and CPU twin, and the rewrites.
  - `./gpu-cpu.sh gpu rewrite 4 none pos05 eq1m`: the GPU rewrite.
