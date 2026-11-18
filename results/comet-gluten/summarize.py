# summarize.py: warm medians (warm-up run dropped) per (query, table, engine) from results.csv and
# the MorBench gpu/cpu CSV lines in logs/{gpu,cpu}-q*.txt; writes summary.csv and prints a table.
import csv
import glob
import statistics
from collections import defaultdict

rows = list(csv.DictReader(open("results.csv")))
for path in glob.glob("logs/gpu-*.txt") + glob.glob("logs/cpu-*.txt"):
    for line in open(path):
        parts = line.strip().split(",")
        if len(parts) >= 8 and parts[1] in ("gpu", "cpu") and parts[3].isdigit():
            rows.append(dict(zip(["table", "engine", "query", "run", "warmup", "millis", "live_rows", "checksum"], parts[:8])))

cells = defaultdict(list)
answers = defaultdict(set)
for r in rows:
    key = (r["query"], r["table"], r["engine"])
    answers[(r["query"], r["table"])].add((r["live_rows"], r["checksum"]))
    if r["warmup"] == "no":
        cells[key].append(float(r["millis"]))

with open("summary.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["query", "table", "engine", "warm_runs", "median_ms", "min_ms", "max_ms"])
    for (q, t, e), v in sorted(cells.items()):
        w.writerow([q, t, e, len(v), round(statistics.median(v), 1), round(min(v), 1), round(max(v), 1)])
        print(f"{q:8} {t:6} {e:14} n={len(v)} median={statistics.median(v):8.1f}")
print("answers per (query, table):")
for k, v in sorted(answers.items()):
    print(k, "AGREE" if len(v) == 1 else f"DIFFER {v}")
