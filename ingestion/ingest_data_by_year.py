from email import message

import fastf1
from fastf1.core import Telemetry
from pandas.core.internals.blocks import F 
import pandas as pd 
from pathlib import Path


YEAR=2025

BASE_DIR =Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
BRONZE_DIR = DATA_DIR / "bronze"
CACHE_DIR = BASE_DIR / "cache"

BRONZE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

fastf1.Cache.enable_cache(str(CACHE_DIR))

print(f"\nLoading {YEAR} FORMULA 1 Schedule")
schedule=fastf1.get_event_schedule(YEAR)
print(f"Found {len(schedule)} events")


schedule_output=BRONZE_DIR/f"season={YEAR}"/"schedule"
schedule_output.mkdir(parents=True, exist_ok=True)

schedule.to_parquet(schedule_output/"schedule.parquet", index=False)

print("Schedule saved")

for _, event in schedule.iterrows():
    round_number=event["RoundNumber"]
    if round_number==0:
        continue
    event_name=event["EventName"]

    print("\n" + "=" * 70)
    print(f"Round {round_number}: {event_name}")
    print("=" * 70)

    try:
        session=fastf1.get_session(YEAR, int(round_number),"R")
        session.load(laps=True,telemetry=False,weather=True,messages=False)

        race_dir=( BRONZE_DIR / f"season={YEAR}"/ f"round={int(round_number):02d}")
        race_dir.mkdir(parents=True, exist_ok=True)


        results = pd.DataFrame(session.results)
        results.to_parquet(race_dir / "results.parquet", index=False,engine="pyarrow",    coerce_timestamps="us",allow_truncated_timestamps=True )
        print( f"Results: {len(results)} rows" )

        laps = pd.DataFrame(session.laps)
        laps.to_parquet(race_dir / "laps.parquet", index=False, engine="pyarrow",    coerce_timestamps="us",allow_truncated_timestamps=True )
        print( f"laps: {len(laps)} rows" )


        weather = pd.DataFrame(session.weather_data)
        weather.to_parquet(race_dir / "weather.parquet", index=False,engine="pyarrow",    coerce_timestamps="us",allow_truncated_timestamps=True )
        print( f"Weather: {len(weather)} rows" )
        
        print(
            f"SUCCESS: {event_name}"
        )


    except Exception as error:
        print(
            f"FAILED: {event_name}" )

        print(
            f"ERROR: {error}"
        )

print("\n" + "=" * 70)
print("2025 INGESTION FINISHED")
print("=" * 70)












