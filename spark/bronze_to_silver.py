from pathlib import Path 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, IntegerType, DateType, ArrayType, LongType, DoubleType, TimestampNTZType, FloatType
import os 
from pathlib import Path


YEAR=2025

BASE_DIR=Path(__file__).resolve().parent.parent

BRONZE_DIR = (BASE_DIR / "data" / "bronze" / "laps" / f"season={YEAR}")

SILVER_DIR=BASE_DIR/"data"/"silver"

spark=SparkSession.builder.appName("F1BronzeSilver").master("local[*]").getOrCreate()

laps_files=sorted(BRONZE_DIR.glob("round=*/data.parquet"))
laps_paths = [str(path) for path in laps_files]
laps = spark.read.parquet(*laps_paths)


print("\nBRONZE ROW COUNT")
print(laps.count())
laps.printSchema()


sessions_schema = StructType([
    StructField("Time", LongType(), True),
    StructField("Driver", StringType(), True),
    StructField("DriverNumber", StringType(), True),

    StructField("LapTime", LongType(), True),
    StructField("LapNumber", DoubleType(), True),
    StructField("Stint", DoubleType(), True),

    StructField("PitOutTime", LongType(), True),
    StructField("PitInTime", LongType(), True),

    StructField("Sector1Time", LongType(), True),
    StructField("Sector2Time", LongType(), True),
    StructField("Sector3Time", LongType(), True),

    StructField("Sector1SessionTime", LongType(), True),
    StructField("Sector2SessionTime", LongType(), True),
    StructField("Sector3SessionTime", LongType(), True),

    StructField("SpeedI1", DoubleType(), True),
    StructField("SpeedI2", DoubleType(), True),
    StructField("SpeedFL", DoubleType(), True),
    StructField("SpeedST", DoubleType(), True),

    StructField("IsPersonalBest", BooleanType(), True),

    StructField("Compound", StringType(), True),
    StructField("TyreLife", DoubleType(), True),
    StructField("FreshTyre", BooleanType(), True),

    StructField("Team", StringType(), True),

    StructField("LapStartTime", LongType(), True),
    StructField("LapStartDate", TimestampNTZType(), True),

    StructField("TrackStatus", StringType(), True),
    StructField("Position", DoubleType(), True),

    StructField("Deleted", IntegerType(), True),
    StructField("DeletedReason", StringType(), True),

    StructField("FastF1Generated", BooleanType(), True),
    StructField("IsAccurate", BooleanType(), True),

    StructField("round", IntegerType(), True),
])

laps_schema=spark.read.schema(sessions_schema).option("basePath", str(BRONZE_DIR)).parquet(*laps_paths)
laps_schema.printSchema()
laps_schema.count()
#silver_schema=(laps_schema.)


#%%

silver_laps=(laps_schema.select(F.lit(YEAR).cast("int").alias("season"),
             F.col("round").alias("round"),
             F.upper(F.trim(F.col("Driver"))).alias("driver_code"),
             F.col("DriverNumber").cast("int").alias("driver_number"),
             F.lower(F.trim(F.col("Team"))).alias("team_name"),
             F.col("lapNumber").cast("int").alias("lap_number"),
             F.col("Stint").cast("int").alias("stint"),
             F.col("Position").cast("int").alias("position"),
             F.upper(F.trim(F.col("Compound"))).alias("tyer_compound"),
             F.col("TyreLife").alias("tyre_life"),
             F.col("FreshTyre").alias("fresh_tyre"),
             F.col("LapTime").alias("lap_time_raw"),
             F.col("Sector1Time").alias("sector1_time_raw"),
             F.col("Sector2Time").alias("sector2_time_raw"),
             F.col("Sector3Time").alias("sector3_time_raw"),
             F.col("PitInTime").alias("pit_in_time"),
             F.col("PitOutTime").alias("pit_out_time"),
             F.col("SpeedI1").alias("speed_i1"),
             F.col("SpeedI2").alias("speed_i2"),
             F.col("SpeedFL").alias("speed_finish_line"),
             F.col("SpeedST").alias("speed_trap"),
             F.col("TrackStatus").alias("track_status"),
             F.col("LapStartDate").alias("lap_start_timestamp"),
             F.col("IsPersonalBest").alias("is_personal_best"),
             F.col("DeletedReason").alias("deleted_reason"),
             F.col("FastF1Generated").alias("is_fastf1_generated"),
             F.col("IsAccurate").alias("is_accurate"),

             F.when(F.col("Deleted")==1, F.lit(True)).when(F.col("Deleted")==0, F.lit(False)).otherwise(F.lit(None).cast("boolean")).alias("is_deleted")

))

silver_laps = silver_laps.withColumn(
    "is_pit_lap",
    F.col("pit_in_time").isNotNull()
    |
    F.col("pit_out_time").isNotNull()
)

silver_laps=silver_laps.withColumn("is_valid_lap", F.col("lap_number").isNotNull() 
                                   & F.col("driver_code").isNotNull()
                                   & F.col("lap_time_raw").isNotNull()
                                   & (F.col("is_deleted").isNull() | (F.col("is_deleted")==False))
                                   & (F.col("is_accurate").isNull() | (F.col("is_accurate")==True))
                                   )


silver_laps = (silver_laps.filter(F.col("driver_code").isNotNull())
               .filter(F.length(F.col("driver_code")) > 0)
               .filter(F.col("lap_number") > 0)
               .filter(F.col("position").isNull() |  (F.col("position") > 0)) )

VALID_COMPOUNDS = ["SOFT", "MEDIUM", "HARD", "INTERMEDIATE" ]

silver_laps=silver_laps.withColumn("tyer_compound", F.when(F.col("tyer_compound").isin(VALID_COMPOUNDS),
                                                           F.col("tyer_compound"))
                                   .otherwise(F.lit("UNKNOWN")))


dedup_window=(Window.partitionBy("season","round","driver_code","lap_number").orderBy(F.col("is_accurate").desc(),
                                                                                      F.col("is_deleted").asc_nulls_last()))
silver_laps=(silver_laps.withColumn("_row_number", F.row_number().over(dedup_window))
             .filter(F.col("_row_number")==1).drop("_row_number"))




print("\nSILVER SCHEMA")
silver_laps.printSchema()


print("\nSILVER ROW COUNT")
print(silver_laps.count())


print("\nSAMPLE SILVER DATA")

silver_laps.select(
    "round",
    "driver_code",
    "lap_number",
    "tyer_compound",
    "position",
    "is_pit_lap",
    "is_valid_lap",
).show(
    30,
    truncate=False
)

duplicates = (
    silver_laps
    .groupBy(
        "season",
        "round",
        "driver_code",
        "lap_number",
    )
    .count()
    .filter(
        F.col("count") > 1
    )
)

duplicate_count = duplicates.count()

print("\nDUPLICATE KEYS")
duplicates.show(truncate=False)

if duplicate_count > 0:
    raise ValueError(
        f"Found {duplicate_count} duplicate Silver lap keys."
    )


#write
output_path=str(SILVER_DIR/"laps")
silver_laps.write.mode("overwrite").partitionBy("season","round").parquet(output_path)

print(f"Silver laps written to \n{output_path}")
spark.stop()