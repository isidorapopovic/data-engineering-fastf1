""" import fastf1
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (    StructType,StructField, StringType,IntegerType,DateType,DoubleType, ArrayType, TimestampType
)

import os 
import json 
import pandas as pd 
import numpy as np 

os.environ["JAVA_HOME"] = (
    r"C:\Users\IsidoraPopovicLUKOWA\AppData\Local\Programs"
    r"\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = (
    SparkSession.builder
    .appName("fastf12025")
    .getOrCreate()
)





session = fastf1.get_session(2025, "Monza", "R")
session.load()

laps = session.laps

print(laps.columns)
print(laps.head())

 """