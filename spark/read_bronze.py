from pathlib import Path
from pyspark.sql import SparkSession


BASE_DIR = Path(__file__).resolve().parent.parent

BRONZE_DIR = (
    BASE_DIR
    / "data"
    / "bronze"
    / "season=2025"
)


spark = (
    SparkSession.builder
    .appName("F1BronzeValidation")
    .master("local[*]")
    .getOrCreate()
)

laps_files = sorted(
    BRONZE_DIR.glob("round=*/laps.parquet")
)


print("\nFOUND FILES")
for file in laps_files:
    print(file)


if not laps_files:
    raise FileNotFoundError(
        f"No laps.parquet files found in {BRONZE_DIR}"
    )


laps_paths = [str(file) for file in laps_files]


laps = spark.read.parquet(*laps_paths)


print("\nSCHEMA")
laps.printSchema()


print("\nTOTAL ROWS")
print(laps.count())


print("\nSAMPLE DATA")
laps.select(
    "Driver",
    "LapNumber",
    "LapTime",
    "Compound",
    "TyreLife",
    "Position"
).show(20, truncate=False)


spark.stop()