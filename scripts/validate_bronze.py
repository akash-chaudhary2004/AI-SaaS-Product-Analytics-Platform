from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("BronzeValidation")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

path = "/app/data/bronze/events"

print("=" * 70)
print("BRONZE DATA VALIDATION")
print("=" * 70)
print(f"Reading: {path}")

df = spark.read.parquet(path)

count = df.count()

print(f"\nBRONZE RECORD COUNT: {count:,}")

print("\nBRONZE SCHEMA:")
df.printSchema()

print("\nSAMPLE RECORDS:")
df.show(5, truncate=False)

print("=" * 70)
print("BRONZE VALIDATION COMPLETE")
print("=" * 70)

spark.stop()
