from pathlib import Path

import pandas as pd


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


def dataset_is_valid(path: Path) -> bool:
    """
    Verify that a Parquet file exists,
    is readable, and is not empty.
    """

    if not path.is_file():
        return False

    try:
        dataframe = pd.read_parquet(path)

        return (
            len(dataframe) > 0
            and len(dataframe.columns) > 0
        )

    except Exception:
        return False


def round_is_complete(
    bronze_dir: Path,
    year: int,
    round_number: int,
) -> bool:

    return all(
        dataset_is_valid(
            dataset_path(
                bronze_dir,
                dataset,
                year,
                round_number,
            )
        )
        for dataset in DATASETS
    )