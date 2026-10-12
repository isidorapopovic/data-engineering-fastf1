import argparse 
import json
import time
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import fastf1

BASE_DIR=Path(__file__).resolve().parent.parent
BRONZE_DIR=BASE_DIR / "data" / "bronze" /"telemetry"
CACHE_DIR=BASE_DIR / "cache"

BRONZE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

NUMERIC_COLUMNS = [
    "Speed",
    "RPM",
    "nGear",
    "Throttle",
    "DRS",
    "Distance",
    "RelativeDistance",
    "X",
    "Y",
    "Z",
    "DistanceToDriverAhead",
]


def ingest_telemetry(
    season: int, round_number: int, session_name: str = "R"
) -> Path:
    """Load one FastF1 session and persist its lap telemetry as raw Parquet."""
    fastf1.Cache.enable_cache(str(CACHE_DIR))
    session = fastf1.get_session(season, round_number, session_name)
    session.load(telemetry=True)

    telemetry_frames = []
    for _, lap in session.laps.iterlaps():
        try:
            telemetry = lap.get_car_data().add_distance()
        except (KeyError, ValueError):
            # Some laps (for example, incomplete laps) may not have car data.
            continue

        if telemetry.empty:
            continue

        telemetry = telemetry.copy()
        telemetry["Season"] = season
        telemetry["Round"] = round_number
        telemetry["Session"] = session_name
        telemetry["Driver"] = lap["Driver"]
        telemetry["LapNumber"] = lap["LapNumber"]
        telemetry_frames.append(telemetry)

    if not telemetry_frames:
        raise RuntimeError(
            f"No telemetry found for {season} round {round_number} ({session_name})."
        )

    data = pd.concat(telemetry_frames, ignore_index=True)
    available_numeric_columns = [
        column for column in NUMERIC_COLUMNS if column in data.columns
    ]
    metadata_columns = [
        "Season", "Round", "Session", "Driver", "LapNumber"
    ]
    data = data[metadata_columns + available_numeric_columns]

    output_dir = BRONZE_DIR / str(season) / f"round_{round_number:02d}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{session_name.lower()}_telemetry.parquet"
    data.to_parquet(output_path, index=False)
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest raw telemetry for one FastF1 season round and session."
    )
    parser.add_argument("--season", type=int, default=2024)
    parser.add_argument(
        "--round",
        dest="round_number",
        type=int,
        default=1,
        help="Season round number (default: 1)",
    )
    parser.add_argument(
        "--all-rounds",
        action="store_true",
        help="Ingest the selected session for every scheduled round.",
    )
    parser.add_argument("--session", default="R", help="FastF1 session code (default: R)")
    args = parser.parse_args()

    if args.all_rounds:
        schedule = fastf1.get_event_schedule(args.season)
        round_numbers = sorted(
            set(schedule.loc[schedule["RoundNumber"] > 0, "RoundNumber"].astype(int))
        )
        for round_number in round_numbers:
            output_path = ingest_telemetry(args.season, round_number, args.session)
            print(f"Telemetry written to {output_path}")
    else:
        output_path = ingest_telemetry(args.season, args.round_number, args.session)
        print(f"Telemetry written to {output_path}")


if __name__ == "__main__":
    main()
