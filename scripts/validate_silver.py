from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("SilverValidation")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

path = "/app/data/silver/events"

print("=" * 70)
print("SILVER DATA VALIDATION")
print("=" * 70)
print(f"Reading: {path}")

df = spark.read.parquet(path)

count = df.count()

print(f"\nSILVER RECORD COUNT: {count:,}")

print("\nSILVER SCHEMA:")
df.printSchema()

print("\nNULL COUNTS:")
for column in df.columns:
    null_count = df.filter(df[column].isNull()).count()
    if null_count > 0:
        print(f"{column}: {null_count:,}")

print("\nDISTINCT EVENT TYPES:")
df.groupBy("event_type").count().orderBy("count", ascending=False).show(20, truncate=False)

print("\nSAMPLE SILVER RECORDS:")
df.show(5, truncate=False)

print("=" * 70)
print("SILVER VALIDATION COMPLETE")
print("=" * 70)

spark.stop()
