from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("AllGoldSchemasValidation")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

base_path = "/app/data/gold"

datasets = [
    "country_metrics",
    "executive_dashboard",
    "feedback_metrics",
    "model_performance",
    "prompt_metrics",
    "revenue_metrics",
    "session_metrics",
    "subscription_metrics",
    "user_activity",
]

print("=" * 80)
print("ALL GOLD DATASET SCHEMA VALIDATION")
print("=" * 80)

for dataset in datasets:
    path = f"{base_path}/{dataset}"

    print("\n" + "=" * 80)
    print(f"DATASET: {dataset}")
    print(f"PATH: {path}")
    print("=" * 80)

    df = spark.read.parquet(path)

    print(f"RECORD COUNT: {df.count():,}")
    print("\nSCHEMA:")
    df.printSchema()

    print("\nSAMPLE:")
    df.show(3, truncate=False)

print("\n" + "=" * 80)
print("ALL GOLD SCHEMA VALIDATION COMPLETE")
print("=" * 80)

spark.stop()