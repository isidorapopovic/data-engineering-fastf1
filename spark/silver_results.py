
# root
#  |-- DriverNumber: string (nullable = true)
#  |-- BroadcastName: string (nullable = true)
#  |-- Abbreviation: string (nullable = true)
#  |-- DriverId: string (nullable = true)
#  |-- TeamName: string (nullable = true)
#  |-- TeamColor: string (nullable = true)
#  |-- TeamId: string (nullable = true)
#  |-- FirstName: string (nullable = true)
#  |-- LastName: string (nullable = true)
#  |-- FullName: string (nullable = true)
#  |-- HeadshotUrl: string (nullable = true)
#  |-- CountryCode: string (nullable = true)
#  |-- Position: double (nullable = true)
#  |-- ClassifiedPosition: string (nullable = true)
#  |-- GridPosition: double (nullable = true)
#  |-- Q1: long (nullable = true)
#  |-- Q2: long (nullable = true)
#  |-- Q3: long (nullable = true)
#  |-- Time: long (nullable = true)
#  |-- Status: string (nullable = true)
#  |-- Points: double (nullable = true)
#  |-- Laps: double (nullable = true)

from ast import alias
from pathlib import Path
from xmlrpc.client import boolean 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, IntegerType, DateType, ArrayType, LongType, DoubleType, TimestampNTZType, FloatType
import os 
from pathlib import Path




YEAR=2025

BASE_DIR=Path(__file__).resolve().parent.parent

BRONZE_DIR = BASE_DIR / "data" / "bronze" / "results"

SILVER_DIR=BASE_DIR/"data"/"silver"

spark=SparkSession.builder.appName("F1BronzeSilver").master("local[*]").getOrCreate()

results_files = sorted(BRONZE_DIR.glob(f"season={YEAR}/round=*/data.parquet"))
results_paths = [str(path) for path in results_files]
results = spark.read.parquet(*results_paths)


print("\nBRONZE ROW COUNT")
print(results.count())
results.printSchema()
results.show(5)



schema = StructType([
    StructField("DriverNumber", StringType(), True),
    StructField("BroadcastName", StringType(), True),
    StructField("Abbreviation", StringType(), True),
    StructField("DriverId", StringType(), True),
    StructField("TeamName", StringType(), True),
    StructField("TeamColor", StringType(), True),
    StructField("TeamId", StringType(), True),
    StructField("FirstName", StringType(), True),
    StructField("LastName", StringType(), True),
    StructField("FullName", StringType(), True),
    StructField("HeadshotUrl", StringType(), True),
    StructField("CountryCode", StringType(), True),
    StructField("Position", DoubleType(), True),
    StructField("ClassifiedPosition", StringType(), True),
    StructField("GridPosition", DoubleType(), True),
    StructField("Q1", LongType(), True),
    StructField("Q2", LongType(), True),
    StructField("Q3", LongType(), True),
    StructField("Time", LongType(), True),
    StructField("Status", StringType(), True),
    StructField("Points", DoubleType(), True),
    StructField("Laps", DoubleType(), True),
])


results_schema =spark.read.schema(schema).option("basePath", str(BRONZE_DIR)).parquet(*results_paths)
#results_schema.printSchema()
results_schema.count()


results_silver = (
    results_schema
    .select(
        F.lit(YEAR).cast("int").alias("season"),
        F.col("round").cast("int").alias("round"),

        F.col("DriverId").cast("string").alias("driver_id"),
        F.col("DriverNumber").cast("int").alias("driver_number"),
        F.upper(F.trim("Abbreviation")).alias("driver_code"),

        F.trim("TeamName").alias("team_name"),
        F.col("TeamId").cast("string").alias("team_id"),

        F.trim("FirstName").alias("first_name"),
        F.trim("LastName").alias("last_name"),
        F.trim("FullName").alias("full_name"),
        F.upper(F.trim("CountryCode")).alias("country_code"),

        F.col("GridPosition").cast("int").alias("grid_position"),
        F.col("Position").cast("int").alias("finish_position"),
        F.col("ClassifiedPosition").cast("string").alias("classified_position"),

        F.col("Time").cast("long").alias("time_raw_ns"),

        F.trim("Status").alias("status"),
        F.col("Points").cast("double").alias("points"),
        F.col("Laps").cast("int").alias("laps_completed")
    )


    .withColumn(
        "time_seconds",
        F.col("time_raw_ns") / F.lit(1_000_000_000)
    )

    .withColumn(
        "winner_race_time_seconds",
        F.when(
            F.col("finish_position") == 1,
            F.col("time_seconds") / F.lit(1_000_000_000.0)
        )
    )


    .withColumn( "gap_to_winner_seconds", F.when(F.col("finish_position") > 1,F.col("time_seconds")/ F.lit(1_000_000_000.0)))


    .withColumn(
        "positions_gained",
        F.when(
            (F.col("grid_position") > 0) &
            F.col("finish_position").isNotNull(),
            F.col("grid_position") - F.col("finish_position")
        )
    )

    .withColumn("race_winner", F.col("finish_position") == 1)

    .withColumn("started_on_pole", F.col("grid_position") == 1)

    .withColumn("scored_points", F.col("points") > 0)

    .withColumn("classified_position_numeric",
        F.when(
            F.col("classified_position").rlike(r"^\d+$"),
            F.col("classified_position").cast("int")
        )
    )
)


invalid_rounds = results_silver.filter((F.col("round") <= 0) | (F.col("driver_number").isNull()))
print(invalid_rounds)

invalid_driver_number=results_silver.filter((F.col("driver_number")<=0) | (F.col("driver_number").isNull()))
print(invalid_driver_number)

negative_points = results_silver.filter(F.col("points") < 0)
print(negative_points)

duplicates=results_silver.groupBy("driver_id","season","round").count().filter(F.col("count") > 1)
duplicate_count = duplicates.count()
print("\nDUPLICATE KEYS")
duplicates.show(truncate=False)
results_silver=results_silver.dropDuplicates(["driver_id","season","round"])


results_silver.show(5)





SILVER_DIR = BASE_DIR / "data" / "silver" / "results"

#write
results_silver.write.mode("overwrite").partitionBy("season","round").parquet(str(SILVER_DIR))
print(f"\nSilver weather written to:")
print(SILVER_DIR)


spark.stop()