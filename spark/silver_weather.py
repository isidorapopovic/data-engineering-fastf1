#weather
 # |-- Time: long (nullable = true)
 # |-- AirTemp: double (nullable = true)
 # |-- Humidity: double (nullable = true)
 # |-- Pressure: double (nullable = true)
 # |-- Rainfall: boolean (nullable = true)
 # |-- TrackTemp: double (nullable = true)
 # |-- WindDirection: long (nullable = true)
 # |-- WindSpeed: double (nullable = true)


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

BRONZE_DIR = BASE_DIR / "data" / "bronze" / "weather"

SILVER_DIR=BASE_DIR/"data"/"silver"

spark=SparkSession.builder.appName("F1BronzeSilver").master("local[*]").getOrCreate()

weather_files=sorted(BRONZE_DIR.glob(f"season={YEAR}/round=*/data.parquet"))
weather_paths = [str(path) for path in weather_files]
weather = spark.read.parquet(*weather_paths)


print("\nBRONZE ROW COUNT")
print(weather.count())
#weather.printSchema()


schema= StructType([StructField("Time", LongType(), True),
                            StructField("AirTemp", DoubleType(), True),
                            StructField("Humidity", DoubleType(), True),
                            StructField("Pressure", DoubleType(), True),
                            StructField("Rainfall", BooleanType(), True),
                            StructField("TrackTemp", DoubleType(), True),
                            StructField("WindDirection", LongType(), True),
                            StructField("WindSpeed", DoubleType(), True),
                            ])





weather_schema=spark.read.schema(schema).option("basePath", str(BRONZE_DIR)).parquet(*weather_paths)
weather_schema.printSchema()
weather_schema.count()

weather_silver = weather_schema.select(
    F.lit(YEAR).cast("int").alias("season"),
    F.col("round").cast("int").alias("round"),
    F.col("Time").alias("time_raw_ns"),
    (F.col("Time") / F.lit(1_000_000_000.0)).alias("session_time_seconds"),
    F.col("AirTemp").cast("double").alias("air_temp"),
    F.col("Humidity").cast("double").alias("humidity"),
    F.col("Pressure").cast("double").alias("pressure"),
    F.col("Rainfall").cast("boolean").alias("rainfall"),
    F.col("TrackTemp").cast("double").alias("track_temp"),
    F.col("WindDirection").cast("int").alias("wind_direction"),
    F.col("WindSpeed").cast("double").alias("wind_speed")
)
    
    

weather_silver.printSchema()

weather_silver.show(3)

weather_silver=( weather_silver.withColumn("humidity", F.when(F.col("humidity").between(0,100),
                                                              F.col("humidity")))
                .withColumn("wind_direction", F.when(F.col("wind_direction").isNotNull(),
                                                     F.pmod(F.col("wind_direction"),F.lit(360))))
                .withColumn("wind_speed", F.when(F.col("wind_speed") >=0, F.col("wind_speed")))
                            
                            )
    
    
weather_silver=(weather_silver.withColumn("track_air_temp_delta", F.round(F.col("track_temp")-F.col("air_temp"),2))
                .withColumn("weather_condition", F.when(F.col("rainfall")==True, F.lit("WET"))
                            .when(F.col("rainfall")==False, F.lit("DRY"))
                            .otherwise(F.lit("UNKNOWN"))))

weather_silver = weather_silver.dropDuplicates(["season", "round", "time_raw_ns"])

weather_silver = weather_silver.withColumn(
    "is_valid_weather",

    (
        F.col("session_time_seconds").isNotNull()
        & F.col("air_temp").isNotNull()
        & F.col("track_temp").isNotNull()
        & F.col("humidity").between(0, 100)
        & (F.col("wind_speed") >= 0)
    )
)

print("\nSILVER WEATHER SCHEMA")
weather_silver.printSchema()

print("\nSILVER WEATHER SAMPLE")
weather_silver.show(10, truncate=False)

weather_silver.groupBy("is_valid_weather").count().show()



print("\nNULL CHECKS")

for column in [
    "session_time_seconds",
    "air_temp",
    "humidity",
    "pressure",
    "track_temp",
    "wind_speed"
]:
    null_count = (
        weather_silver
        .filter(F.col(column).isNull())
        .count()
    )

    print(f"{column}: {null_count}")


print("\nDUPLICATES")

duplicates = (weather_silver
    .groupBy(
        "season",
        "round",
        "time_raw_ns"
    )
    .count()
    .filter(F.col("count") > 1)
)

print(duplicates.count())

invalid_wind_direction = weather_silver.filter((F.col("wind_direction") < 0) | (F.col("wind_direction") >= 360))
print(invalid_wind_direction.count())

invalid_wind_speed = weather_silver.filter(F.col("wind_speed") < 0)
print(invalid_wind_speed.count())

invalid_track_temp = weather_silver.filter((F.col("track_temp") < 0) | (F.col("track_temp") > 70))
print(invalid_track_temp.count())

invalid_time = weather_silver.filter(F.col("session_time_seconds") < 0)
print(f"Negative session times: {invalid_time.count()}")



SILVER_DIR = BASE_DIR / "data" / "silver" / "weather"

#write
weather_silver.write.mode("overwrite").partitionBy("season","round").parquet(str(SILVER_DIR))
print(f"\nSilver weather written to:")
print(SILVER_DIR)


spark.stop()