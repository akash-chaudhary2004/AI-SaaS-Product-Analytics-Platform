from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("GoldValidation")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

path = "/app/data/gold/executive_dashboard"

print("=" * 70)
print("GOLD EXECUTIVE DASHBOARD VALIDATION")
print("=" * 70)

df = spark.read.parquet(path)

print("\nRECORD COUNT:")
print(df.count())

print("\nSCHEMA:")
df.printSchema()

print("\nSAMPLE RECORDS:")
df.show(10, truncate=False)

print("\nNULL COUNTS:")
for c in df.columns:
    n = df.filter(df[c].isNull()).count()
    print(f"{c}: {n}")

print("\n" + "=" * 70)
print("EXECUTIVE DASHBOARD VALIDATION COMPLETE")
print("=" * 70)

spark.stop()
