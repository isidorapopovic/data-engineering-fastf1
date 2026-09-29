from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BASE_DIR = Path(__file__).resolve().parent.parent

PATH = (
    BASE_DIR
    / "data"
    / "bronze"
    / "telemetry"
    / "season=2025"
    / "round=01"
)


spark = (
    SparkSession.builder
    .appName("TelemetryBronzeValidation")
    .master("local[*]")
    .getOrCreate()
)


df = spark.read.parquet(
    str(PATH / "data.parquet")
)


print("\nROWS")
print(df.count())


print("\nSCHEMA")
df.printSchema()


print("\nDRIVERS")
df.select(
    "Driver"
).distinct().show()


print("\nLAPS PER DRIVER")

df.groupBy(
    "Driver"
).agg(
    F.countDistinct("LapNumber").alias("laps"),
    F.count("*").alias("telemetry_samples")
).orderBy(
    "Driver"
).show(30)


print("\nNULL SPEED")

print(
    df.filter(
        F.col("Speed").isNull()
    ).count()
)


spark.stop()