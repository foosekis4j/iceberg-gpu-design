# explain.py <warehouse> <query> <runs, ignored> <table>...: runs q1/q2 once per table and prints
# the physical plan (AQE initial plan, without executing), to see which operators ran natively and which fell back to Spark.
import sys

from pyspark.sql import SparkSession

wh, q, tables = sys.argv[1], sys.argv[2], sys.argv[4:]
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")
for t in tables:
    spark.read.format("iceberg").load(f"{wh}/{t}").createOrReplaceTempView("t")
    if q == "q1":
        sql = "SELECT count(*), sum(v1) FROM t"
    else:
        sql = "SELECT k, count(*) c, sum(v1) a, sum(v2) b, sum(v3) d, sum(v4) e FROM t GROUP BY k"
    df = spark.sql(sql)
    if len(sys.argv) > 5 and False:
        df.collect()
    plan = df._jdf.queryExecution().executedPlan().toString()
    print(f"=== PLAN {t} {q}\n{plan}", flush=True)
