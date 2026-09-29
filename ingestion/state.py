from pathlib import Path


DATASETS = [
    "laps",
    "results",
    "weather",
]


def dataset_path(
    bronze_dir: Path,
    dataset: str,
    year: int,
    round_number: int,
) -> Path:

    return (
        bronze_dir
        / dataset
        / f"season={year}"
        / f"round={round_number:02d}"
        / "data.parquet"
    )


def round_is_complete(
    bronze_dir: Path,
    year: int,
    round_number: int,
) -> bool:

    return all(
        dataset_path(
            bronze_dir,
            dataset,
            year,
            round_number,
        ).exists()
        for dataset in DATASETS
    )