"""Inspect bronze telemetry data for 2025, round 1."""

import re
from pathlib import Path

import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BASE_DIR = Path(__file__).resolve().parent.parent
BRONZE_DIR = BASE_DIR / "data" / "bronze" / "telemetry"
SEASON_PATTERN = re.compile(r"(?:season|year)[=_-]?2025", re.IGNORECASE)
ROUND_ONE_PATTERN = re.compile(r"round[=_-]?0?1", re.IGNORECASE)
BRONZE_OUTPUT_DIR = (
    BASE_DIR / "data" / "bronze" / "telemetry_spark" / "season=2025" / "round=01"
)

telemetry_files = sorted(
    file
    for file in BRONZE_DIR.rglob("*.parquet")
    if any(SEASON_PATTERN.fullmatch(part) for part in file.parts)
    and any(ROUND_ONE_PATTERN.fullmatch(part) for part in file.parts)
)

if not telemetry_files:
    raise FileNotFoundError(
        f"No 2025 round 1 telemetry Parquet files found in {BRONZE_DIR}"
    )

first_file = telemetry_files[0]
print(f"\nINSPECTING FIRST TELEMETRY FILE\n{first_file}")
schema = pq.read_schema(first_file)

print("\nCOLUMNS AND TYPES")
for field in schema:
    print(f"{field.name}: {field.type}")


bronze_schema = StructType(
    [
        StructField("Date", TimestampType()),
        StructField("SessionTime", LongType()),
        StructField("DriverAhead", StringType()),
        StructField("DistanceToDriverAhead", DoubleType()),
        StructField("Time", LongType()),
        StructField("RPM", DoubleType()),
        StructField("Speed", DoubleType()),
        StructField("nGear", LongType()),
        StructField("Throttle", DoubleType()),
        StructField("Brake", BooleanType()),
        StructField("DRS", LongType()),
        StructField("Source", StringType()),
        StructField("Distance", DoubleType()),
        StructField("RelativeDistance", DoubleType()),
        StructField("Status", StringType()),
        StructField("X", DoubleType()),
        StructField("Y", DoubleType()),
        StructField("Z", DoubleType()),
    ]
)

spark = SparkSession.builder.appName("BronzeTelemetry").getOrCreate()
source_df = spark.read.parquet(str(first_file))
bronze_df = source_df.select(
    *[col(field.name).cast(field.dataType).alias(field.name) for field in bronze_schema]
)
bronze_df.write.mode("overwrite").parquet(str(BRONZE_OUTPUT_DIR))
print(f"\nSaved Spark bronze telemetry data to {BRONZE_OUTPUT_DIR}")
spark.stop()