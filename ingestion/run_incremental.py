from datetime import datetime, timezone

from config import YEAR, BASE_DIR

from ingestion.discover_rounds import (
    get_missing_rounds,
)

from ingestion.ingest_round import (
    ingest_round,
)

from ingestion.run_logger import (
    new_run_id,
    log_run,
)


LOG_FILE = (
    BASE_DIR
    / "metadata"
    / "pipeline_runs.jsonl"
)


def main():

    missing_rounds = get_missing_rounds(
        YEAR
    )

    if not missing_rounds:

        print(
            f"No new rounds for {YEAR}."
        )

        return

    print(
        f"Rounds to ingest: "
        f"{missing_rounds}"
    )

    for round_number in missing_rounds:

        run_id = new_run_id()

        started_at = datetime.now(
            timezone.utc
        )

        try:

            counts = ingest_round(
                YEAR,
                round_number,
            )

            status = "SUCCESS"
            error = None

        except Exception as exc:

            counts = {}

            status = "FAILED"

            error = str(exc)

            print(
                f"Round {round_number} "
                f"failed: {exc}"
            )

        finished_at = datetime.now(
            timezone.utc
        )

        log_run(
            LOG_FILE,
            {
                "run_id": run_id,
                "season": YEAR,
                "round": round_number,
                "status": status,
                "started_at": started_at,
                "finished_at": finished_at,
                "laps_rows": counts.get(
                    "laps"
                ),
                "results_rows": counts.get(
                    "results"
                ),
                "weather_rows": counts.get(
                    "weather"
                ),
                "error": error,
            }
        )


if __name__ == "__main__":
    main()