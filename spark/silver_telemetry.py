
from pathlib import Path
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    BooleanType,
    IntegerType,
    LongType,
    DoubleType,
    TimestampType
)


YEAR = 2025

BASE_DIR = Path(__file__).resolve().parent.parent

BRONZE_DIR = BASE_DIR / "data" / "bronze" / "telemetry"

SILVER_DIR = BASE_DIR / "data" / "silver" / "telemetry"


spark = (
    SparkSession.builder
    .appName("F1BronzeSilverTelemetry")
    .master("local[*]")
    .getOrCreate()
)



import pyarrow as pa
import pyarrow.parquet as pq

# --------------------------------------------------
# BRONZE FILES
# --------------------------------------------------

telemetry_files = []

for round_number in range(1, 25):

    old_dir = (
        BRONZE_DIR
        / str(YEAR)
        / f"round_{round_number:02d}"
    )

    new_dir = (
        BRONZE_DIR
        / f"season={YEAR}"
        / f"round={round_number:02d}"
    )

    # Prefer your existing Bronze structure.
    # Use the partitioned version only if the old one is absent.

    if old_dir.exists():
        files = sorted(old_dir.glob("*.parquet"))

    elif new_dir.exists():
        files = sorted(new_dir.glob("*.parquet"))

    else:
        files = []

    for file in files:
        telemetry_files.append((round_number, file))


if not telemetry_files:
    raise FileNotFoundError(
        f"No telemetry Parquet files found in {BRONZE_DIR}"
    )


print("\nBRONZE TELEMETRY FILES")

for round_number, path in telemetry_files:
    print(f"Round {round_number}: {path}")


# --------------------------------------------------
# BRONZE SCHEMA
# --------------------------------------------------

base_schema = StructType([
    StructField("Date", TimestampType(), True),
    StructField("SessionTime", LongType(), True),
    StructField("DriverAhead", StringType(), True),
    StructField("DistanceToDriverAhead", DoubleType(), True),
    StructField("Time", LongType(), True),
    StructField("RPM", DoubleType(), True),
    StructField("Speed", DoubleType(), True),
    StructField("nGear", LongType(), True),
    StructField("Throttle", DoubleType(), True),
    StructField("Brake", BooleanType(), True),
    StructField("DRS", LongType(), True),
    StructField("Source", StringType(), True),
    StructField("Distance", DoubleType(), True),
    StructField("RelativeDistance", DoubleType(), True),
    StructField("Status", StringType(), True),
    StructField("X", DoubleType(), True),
    StructField("Y", DoubleType(), True),
    StructField("Z", DoubleType(), True),
    StructField("Driver", StringType(), True),
    StructField("LapNumber", DoubleType(), True)
])


# --------------------------------------------------
# READ BRONZE
# --------------------------------------------------

telemetry_dataframes = []

for round_number, path in telemetry_files:

    arrow_schema = pq.read_schema(path)
    fields = list(base_schema.fields)

    # Respect the physical type of LapNumber
    # while reading each Parquet file.

    if "LapNumber" in arrow_schema.names:

        arrow_type = arrow_schema.field("LapNumber").type

        if pa.types.is_integer(arrow_type):
            lap_type = LongType()

        elif pa.types.is_floating(arrow_type):
            lap_type = DoubleType()

        else:
            raise TypeError(
                f"Unsupported LapNumber type: {arrow_type}"
            )

    else:
        lap_type = DoubleType()

    fields[-1] = StructField(
        "LapNumber",
        lap_type,
        True
    )

    schema = StructType(fields)

    df = (
        spark.read
        .schema(schema)
        .parquet(str(path))
        .withColumn(
            "LapNumber",
            F.col("LapNumber").cast("double")
        )
        .withColumn(
            "round",
            F.lit(round_number).cast("int")
        )
    )

    telemetry_dataframes.append(df)


telemetry = reduce(
    lambda df1, df2: df1.unionByName(
        df2,
        allowMissingColumns=True
    ),
    telemetry_dataframes
)


print("\nBRONZE TELEMETRY ROW COUNT")
print(telemetry.count())

print("\nBRONZE TELEMETRY SCHEMA")
telemetry.printSchema()



# --------------------------------------------------
# BRONZE TO SILVER
# --------------------------------------------------

telemetry_silver = telemetry.select(

    F.lit(YEAR).cast("int").alias("season"),

    F.col("round").cast("int").alias("round"),

    F.upper(F.trim(F.col("Driver"))).alias("driver"),

    F.col("LapNumber").cast("int").alias("lap_number"),

    F.col("Date").alias("date"),

    F.col("SessionTime").cast("long").alias("session_time_ns"),

    (
        F.col("SessionTime") / F.lit(1_000_000_000.0)
    ).alias("session_time_seconds"),

    F.col("Time").cast("long").alias("time_ns"),

    (
        F.col("Time") / F.lit(1_000_000_000.0)
    ).alias("time_seconds"),

    F.upper(
        F.trim(F.col("DriverAhead"))
    ).alias("driver_ahead"),

    F.col("DistanceToDriverAhead").cast("double").alias(
        "distance_to_driver_ahead"
    ),

    F.col("RPM").cast("double").alias("rpm"),

    F.col("Speed").cast("double").alias("speed"),

    F.col("nGear").cast("int").alias("gear"),

    F.col("Throttle").cast("double").alias("throttle"),

    F.col("Brake").cast("boolean").alias("brake"),

    F.col("DRS").cast("int").alias("drs"),

    F.lower(
        F.trim(F.col("Source"))
    ).alias("source"),

    F.col("Distance").cast("double").alias("distance"),

    F.col("RelativeDistance").cast("double").alias(
        "relative_distance"
    ),

    F.upper(
        F.trim(F.col("Status"))
    ).alias("status"),

    F.col("X").cast("double").alias("x"),

    F.col("Y").cast("double").alias("y"),

    F.col("Z").cast("double").alias("z")
)


print("\nINITIAL SILVER TELEMETRY SCHEMA")
telemetry_silver.printSchema()

print("\nINITIAL SILVER TELEMETRY SAMPLE")
telemetry_silver.show(3, truncate=False)


# --------------------------------------------------
# DATA CLEANING
# --------------------------------------------------

# Replace invalid numeric values with NULL.
# Keep valid records rather than removing entire rows.

numeric_columns = [
    "distance_to_driver_ahead",
    "rpm",
    "speed",
    "throttle",
    "distance",
    "relative_distance",
    "x",
    "y",
    "z"
]

for column in numeric_columns:

    value = F.col(column)

    telemetry_silver = telemetry_silver.withColumn(
        column,
        F.when(
            value.isNotNull()
            & (~F.isnan(value))
            & (F.abs(value) != float("inf")),
            value
        )
    )


# Apply additional physical constraints.

telemetry_silver = (

    telemetry_silver

    .withColumn(
        "rpm",
        F.when(F.col("rpm") >= 0, F.col("rpm"))
    )

    .withColumn(
        "speed",
        F.when(F.col("speed") >= 0, F.col("speed"))
    )

    .withColumn(
        "throttle",
        F.when(
            F.col("throttle").between(0, 100),
            F.col("throttle")
        )
    )

    .withColumn(
        "gear",
        F.when(
            F.col("gear").between(0, 8),
            F.col("gear")
        )
    )

    .withColumn(
        "distance",
        F.when(
            F.col("distance") >= 0,
            F.col("distance")
        )
    )

    .withColumn(
        "distance_to_driver_ahead",
        F.when(
            F.col("distance_to_driver_ahead") >= 0,
            F.col("distance_to_driver_ahead")
        )
    )

    .withColumn(
        "relative_distance",
        F.when(
            F.col("relative_distance").between(0, 1),
            F.col("relative_distance")
        )
    )

    .withColumn(
        "drs",
        F.when(
            F.col("drs").between(0, 14),
            F.col("drs")
        )
    )

)


# --------------------------------------------------
# ADDITIONAL TELEMETRY FEATURES
# --------------------------------------------------

telemetry_silver = (

    telemetry_silver

    .withColumn(
        "speed_ms",
        F.round(F.col("speed") / F.lit(3.6), 3)
    )

    .withColumn(
        "is_full_throttle",
        F.when(F.col("throttle") >= 99, True)
        .when(F.col("throttle").isNotNull(), False)
    )

    .withColumn(
        "is_braking",
        F.col("brake")
    )

    .withColumn(
        "is_drs_active",
        F.when(F.col("drs").isin(10, 12, 14), True)
        .when(F.col("drs").isNotNull(), False)
    )

)


# --------------------------------------------------
# DATA VALIDITY FLAG
# --------------------------------------------------

telemetry_silver = telemetry_silver.withColumn(

    "is_valid_telemetry",

    (
        F.col("round").isNotNull()
        & (F.col("round") > 0)
        & F.col("date").isNotNull()
        & F.col("session_time_ns").isNotNull()
        & (F.col("session_time_ns") >= 0)
        & F.col("time_ns").isNotNull()
        & (F.col("time_ns") >= 0)
        & F.col("rpm").isNotNull()
        & F.col("speed").isNotNull()
        & F.col("throttle").isNotNull()
        & F.col("gear").isNotNull()
        & F.col("brake").isNotNull()
    )

)


# --------------------------------------------------
# REMOVE DUPLICATES
# --------------------------------------------------

# Include driver in the sample key so telemetry
# from different drivers is not merged accidentally.

duplicate_keys = [
    "season",
    "round",
    "driver",
    "date",
    "session_time_ns",
    "time_ns"
]

telemetry_silver = telemetry_silver.dropDuplicates(
    duplicate_keys
)


# --------------------------------------------------
# SILVER VALIDATION
# --------------------------------------------------

print("\nSILVER TELEMETRY SCHEMA")
telemetry_silver.printSchema()

print("\nSILVER TELEMETRY SAMPLE")
telemetry_silver.show(10, truncate=False)


print("\nTELEMETRY VALIDITY")

telemetry_silver.groupBy(
    "is_valid_telemetry"
).count().show()


print("\nNULL CHECKS")

for column in [
    "driver",
    "date",
    "session_time_ns",
    "time_ns",
    "rpm",
    "speed",
    "gear",
    "throttle",
    "brake",
    "drs",
    "distance"
]:

    null_count = (
        telemetry_silver
        .filter(F.col(column).isNull())
        .count()
    )

    print(f"{column}: {null_count}")


print("\nDUPLICATES")

duplicates = (
    telemetry_silver

    .groupBy(*duplicate_keys)

    .count()

    .filter(F.col("count") > 1)
)

print(duplicates.count())


print("\nINVALID RPM")

invalid_rpm = telemetry_silver.filter(
    F.col("rpm") < 0
)

print(invalid_rpm.count())


print("\nINVALID SPEED")

invalid_speed = telemetry_silver.filter(
    F.col("speed") < 0
)

print(invalid_speed.count())


print("\nINVALID THROTTLE")

invalid_throttle = telemetry_silver.filter(
    (F.col("throttle") < 0)
    | (F.col("throttle") > 100)
)

print(invalid_throttle.count())


print("\nINVALID GEAR")

invalid_gear = telemetry_silver.filter(
    (F.col("gear") < 0)
    | (F.col("gear") > 8)
)

print(invalid_gear.count())


print("\nINVALID SESSION TIME")

invalid_time = telemetry_silver.filter(
    F.col("session_time_seconds") < 0
)

print(f"Negative session times: {invalid_time.count()}")


print("\nINVALID DISTANCE")

invalid_distance = telemetry_silver.filter(
    F.col("distance") < 0
)

print(invalid_distance.count())


# --------------------------------------------------
# RACE COVERAGE
# --------------------------------------------------

print("\nTELEMETRY ROWS PER ROUND")

telemetry_silver.groupBy(
    "season",
    "round"
).count().orderBy(
    "season",
    "round"
).show(50, truncate=False)


print("\nTELEMETRY ROWS PER DRIVER")

telemetry_silver.groupBy(
    "season",
    "round",
    "driver"
).count().orderBy(
    "round",
    "driver"
).show(100, truncate=False)


# --------------------------------------------------
# WRITE SILVER
# --------------------------------------------------

telemetry_silver.write.mode("overwrite").partitionBy(
    "season",
    "round"
).parquet(str(SILVER_DIR))


print("\nSILVER TELEMETRY WRITTEN TO:")
print(SILVER_DIR)


spark.stop()
