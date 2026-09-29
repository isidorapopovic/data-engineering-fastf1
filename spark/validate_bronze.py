from pathlib import Path 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

YEAR=2025

BASE_DIR=Path(__file__).resolve().parent.parent

BRONZE_DIR = BASE_DIR / "data" / "bronze" / "laps"
SEASON_DIR = BRONZE_DIR / f"season={YEAR}"

spark=SparkSession.builder.appName("F1BronzeVali").master("local[*]").getOrCreate()


#laps_path = str(BRONZE_DIR / "round=*" / "laps.parquet")

laps_files=sorted(SEASON_DIR.glob("round=*/data.parquet"))

for file in laps_files:
    print(file)

laps_paths= [str(file) for file in laps_files]

laps = spark.read.option("basePath", str(BRONZE_DIR)).parquet(*laps_paths)


print("\n" + "=" * 70)
print("BRONZE VALIDATION")
print("=" * 70)

total_rows=laps.count()

print(f"\n Total laps num {total_rows}")
laps.printSchema()
laps.select("round").distinct().orderBy("round").show()
laps.groupBy("round").agg(F.count("*").alias("num_laps")).orderBy("round").show()
laps.select("Compound").distinct().show()


laps.groupBy("round").agg(F.countDistinct("Driver").alias("drivers")).orderBy("round").show(30)



print("nulls")

cols=["Driver","LapNumber","LapTime","Team","Compound","Position"]

null_counts =laps.select(*[F.count(F.when(F.col(column).isNull(),column)).alias(column) for column in cols])


null_counts.show()

print("duplicates")

duplicates=(laps.groupBy("round","Driver","LapNumber").count().filter(F.col("count")>1))

duplicates_count=duplicates.count()

print(duplicates_count)

if duplicates_count>0:
    duplicates.show()

print("lap range")
laps.select("LapNumber").summary("min","max","mean").show()