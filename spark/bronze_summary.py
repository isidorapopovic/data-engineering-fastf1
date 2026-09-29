from pathlib import Path 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

YEAR=2025

BASE_DIR=Path(__file__).resolve().parent.parent

BRONZE_DIR = (BASE_DIR/ "data" / "bronze"/ f"season={YEAR}")

spark=SparkSession.builder.appName("F1BronzeSumm").master("local[*]").getOrCreate()

datasets=["laps","results","weather"]


for ds in datasets:
    files=sorted(BRONZE_DIR.glob(f"round=*/{ds}.parquet"))
    print(f"{ds.upper()}")

    try:
        if not files:
            print("NO FILES")
        paths=[str(file) for file in files]

        df=spark.read.option("basePath", str(BRONZE_DIR)).parquet(*paths)
        rows=df.count()
        columns=len(df.columns)
        rounds=(df.select("round").distinct().count())

        print(f"{rows}")
        print(f"{columns}")
        print(f"{rounds}")
    except Exception as error:

        print(
            f"FAILED TO LOAD: {error}"
        )
print("\n" + "=" * 70)

spark.stop()