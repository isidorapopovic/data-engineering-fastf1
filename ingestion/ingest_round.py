from datetime import datetime, timezone
import fastf1
import pandas as pd
from config import BASE_DIR, BRONZE_DIR, CACHE_DIR
from ingestion.state import dataset_path
from ingestion.state import round_is_complete


fastf1.Cache.enable_cache(str(CACHE_DIR))

def ingest_round (year: int, round_number: int, force: bool = False,)-> dict:
    print(f"\nIngesting season={year}, " f"round={round_number}")


    success_file = (
    BRONZE_DIR
    / "_state"
    / f"season={year}"
    / f"round={round_number:02d}"
    / "_SUCCESS")

    success_file.parent.mkdir(
        parents=True,
        exist_ok=True,)
    success_file.unlink(missing_ok=True)
    


    
    session=fastf1.get_session(year, round_number, "R",)
    session.load(laps=True, telemetry=False, weather=True, messages=False,)
    ingested_at = datetime.now(timezone.utc)
    results=pd.DataFrame(session.results).copy()
    results["ingested_at"] = ingested_at
    laps=pd.DataFrame(session.laps).copy()
    laps["ingested_at"] = ingested_at
    weather=pd.DataFrame(session.weather_data).copy()
    weather["ingested_at"] = ingested_at

    datasets={"results":results, "laps":laps, "weather":weather}
    for name, dataframe in datasets.items():
        if dataframe.empty:
            raise ValueError(f"No {name} data loaded for {year} round {round_number}")
    row_counts={}

    for dataset_name, dataframe in datasets.items():

        output = dataset_path(
            BRONZE_DIR,
            dataset_name,
            year,
            round_number,
        )

        output.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        dataframe.to_parquet(
            output,
            index=False,
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
        )

        row_counts[dataset_name] = len(
            dataframe
        )

        print(
            f"{dataset_name}: "
            f"{len(dataframe)} rows"
        )

    success_file.touch()
    return row_counts
