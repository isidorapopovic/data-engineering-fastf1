from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BASE_DIR=Path(__file__).resolve().parent.parent

BRONZE_DIR = BASE_DIR / "data" / "bronze"

# Standardized Bronze layout: bronze/<dataset>/season=.../round=.../data.parquet
BRONZE_DATASETS = ("laps", "results", "weather")
for dataset in BRONZE_DATASETS:
	dataset_dir = BRONZE_DIR / dataset
	print(f"\nBRONZE {dataset.upper()} PARTITIONS")
	if dataset_dir.exists():
		for data_file in sorted(dataset_dir.glob("season=*/round=*/data.parquet")):
			print(data_file.relative_to(BRONZE_DIR))
	else:
		print(f"Not found: {dataset_dir}")

SILVER_DIR=BASE_DIR/"data"/"silver"/"laps"

spark=SparkSession.builder.appName("F1BronzeSilver").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

laps = spark.read.parquet(str(SILVER_DIR))




print("\nTOTAL ROWS")
print(laps.count())

print("\nROUNDS")
laps.groupBy("season", "round").count().orderBy("season","round").show(30)

print("\nVALID LAP DISTRIBUTION")
laps.groupBy("is_valid_lap").count().show()

print("\nVALID PIT DISTRIBUTION")
laps.groupBy("is_pit_lap").count().show()

print("\nTYRE COMPOUNDS")
laps.groupBy("tyer_compound").count().orderBy(F.desc("count")).show()

print("\nNULL DRIVER CODES")
print(laps.filter( F.col("driver_code").isNull()).count())


print("\nNULL LAP NUMBERS")
print(laps.filter(F.col("lap_number").isNull()).count())

duplicates=(laps.groupBy("season", "round", "driver_code","lap_number").count().filter(F.col("count")>1))
print("\nDUPLICATES")
print(duplicates.count())



print("\n" + "=" * 70)


spark.stop()

