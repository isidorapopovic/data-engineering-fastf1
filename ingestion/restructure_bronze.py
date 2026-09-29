from pathlib import Path
import shutil

BASE_DIR = Path(__file__).resolve().parent.parent

BRONZE_DIR = BASE_DIR / "data" / "bronze"
YEAR = 2025

OLD_SEASON_DIR = BRONZE_DIR / f"season={YEAR}"

DATASETS = {
    "laps": "laps.parquet",
    "results": "results.parquet",
    "weather": "weather.parquet",
}


def restructure_bronze():

    round_dirs = sorted(OLD_SEASON_DIR.glob("round=*"))

    print(f"Found {len(round_dirs)} rounds.")

    for round_dir in round_dirs:

        round_name = round_dir.name

        print(f"\nProcessing {round_name}")

        for dataset, old_filename in DATASETS.items():

            source = round_dir / old_filename

            destination = (
                BRONZE_DIR
                / dataset
                / f"season={YEAR}"
                / round_name
                / "data.parquet"
            )

            if not source.exists():
                print(f"  SKIP: {old_filename} not found")
                continue

            destination.parent.mkdir(
                parents=True,
                exist_ok=True
            )

            if destination.exists():
                print(f"  EXISTS: {dataset}")
                continue

            shutil.copy2(
                source,
                destination
            )

            print(f"  COPIED: {dataset}")


if __name__ == "__main__":
    restructure_bronze()