from datetime import datetime, timezone

import fastf1
import pandas as pd

from config import YEAR, BRONZE_DIR
from ingestion.state import round_is_complete


def get_completed_rounds(year: int) -> list[int]:

    schedule = fastf1.get_event_schedule(year)

    today = datetime.now(timezone.utc).date()

    completed = []

    for _, event in schedule.iterrows():

        round_number = int(event["RoundNumber"])

        if round_number <= 0:
            continue

        event_date = event["EventDate"]

        if pd.isna(event_date):
            continue

        # A race scheduled today may still be running or awaiting publication.
        if event_date.date() < today:
            completed.append(round_number)

    return completed


def get_missing_rounds(
    year: int,
) -> list[int]:

    available = get_completed_rounds(year)

    return [
        round_number
        for round_number in available
        if not round_is_complete(
            BRONZE_DIR,
            year,
            round_number,
        )
    ]


if __name__ == "__main__":

    completed = get_completed_rounds(YEAR)
    missing = get_missing_rounds(YEAR)

    print("Completed F1 rounds:")
    print(completed)

    print("\nMissing Bronze rounds:")
    print(missing)
