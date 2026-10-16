import uuid

from datetime import datetime, timezone

from orchestration.metadata_db import (
    get_connection,
    initialize_database,
)


VALID_STATUSES = {
    "PENDING",
    "RUNNING",
    "SUCCESS",
    "FAILED",
    "SKIPPED",
}


def utc_now():
    return datetime.now(
        timezone.utc
    ).isoformat()


def create_run(
    season: int,
    round_number: int,
    dataset: str,
    stage: str,
):
    """
    Register a new processing attempt.

    Returns the unique run_id.
    """

    initialize_database()

    run_id = str(uuid.uuid4())

    with get_connection() as conn:

        previous_attempt = conn.execute(
            """
            SELECT COALESCE(
                MAX(attempt),
                0
            )

            FROM pipeline_runs

            WHERE season = ?
              AND round_number = ?
              AND dataset = ?
              AND stage = ?
            """,
            (
                season,
                round_number,
                dataset,
                stage,
            ),
        ).fetchone()[0]

        attempt = previous_attempt + 1

        conn.execute(
            """
            INSERT INTO pipeline_runs (

                run_id,
                season,
                round_number,
                dataset,
                stage,
                status,
                attempt

            )

            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                season,
                round_number,
                dataset,
                stage,
                "PENDING",
                attempt,
            ),
        )

    return run_id


def update_run_status(
    run_id: str,
    status: str,
    row_count=None,
    error_message=None,
):
    """
    Update a processing attempt's status.
    """

    if status not in VALID_STATUSES:
        raise ValueError(
            f"Invalid pipeline status: {status}"
        )

    with get_connection() as conn:

        current = conn.execute(
            """
            SELECT status
            FROM pipeline_runs
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchone()

        if current is None:
            raise ValueError(
                f"Unknown run_id: {run_id}"
            )

        if current["status"] in {
            "SUCCESS",
            "FAILED",
            "SKIPPED",
        }:
            raise ValueError(
                "Cannot update a completed run."
            )

        transitions = {
            "PENDING": {
                "RUNNING",
                "FAILED",
                "SKIPPED",
            },
            "RUNNING": {
                "SUCCESS",
                "FAILED",
            },
        }

        if status not in transitions[current["status"]]:
            raise ValueError(
                f"Invalid transition: "
                f"{current['status']} -> {status}"
            )

        started_at = (
            utc_now()
            if status == "RUNNING"
            else None
        )

        finished_at = (
            utc_now()
            if status in {
                "SUCCESS",
                "FAILED",
                "SKIPPED",
            }
            else None
        )

        cursor = conn.execute(
            """
            UPDATE pipeline_runs

            SET
                status = ?,

                started_at = COALESCE(
                    started_at,
                    ?
                ),

                finished_at = ?,

                row_count = ?,

                error_message = ?

            WHERE run_id = ?
            """,
            (
                status,
                started_at,
                finished_at,
                row_count,
                error_message,
                run_id,
            ),
        )

        if cursor.rowcount != 1:
            raise RuntimeError(
                "Failed to update pipeline run."
            )


def mark_running(run_id):
    update_run_status(
        run_id,
        "RUNNING",
    )


def mark_success(
    run_id,
    row_count=None,
):
    update_run_status(
        run_id,
        "SUCCESS",
        row_count=row_count,
    )


def mark_failed(
    run_id,
    error_message,
):
    update_run_status(
        run_id,
        "FAILED",
        error_message=str(error_message),
    )


def get_run(run_id):
    """
    Retrieve one execution record.
    """

    with get_connection() as conn:

        result = conn.execute(
            """
            SELECT *
            FROM pipeline_runs
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchone()

    return dict(result) if result else None


def get_latest_run(
    season,
    round_number,
    dataset,
    stage,
):
    """
    Retrieve the most recent attempt
    for one dataset and processing stage.
    """

    with get_connection() as conn:

        result = conn.execute(
            """
            SELECT *

            FROM pipeline_runs

            WHERE season = ?
              AND round_number = ?
              AND dataset = ?
              AND stage = ?

            ORDER BY
                attempt DESC,
                created_at DESC

            LIMIT 1
            """,
            (
                season,
                round_number,
                dataset,
                stage,
            ),
        ).fetchone()

    return dict(result) if result else None


def dataset_has_succeeded(
    season,
    round_number,
    dataset,
    stage,
):
    """
    Check whether any attempt for this
    dataset/stage completed successfully.
    """

    with get_connection() as conn:

        result = conn.execute(
            """
            SELECT 1

            FROM pipeline_runs

            WHERE season = ?
              AND round_number = ?
              AND dataset = ?
              AND stage = ?
              AND status = 'SUCCESS'

            LIMIT 1
            """,
            (
                season,
                round_number,
                dataset,
                stage,
            ),
        ).fetchone()

    return result is not None


def list_recent_runs(limit=20):
    """
    Return recent processing attempts.
    """

    with get_connection() as conn:

        rows = conn.execute(
            """
            SELECT *

            FROM pipeline_runs

            ORDER BY
                created_at DESC,
                rowid DESC

            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    return [
        dict(row)
        for row in rows
    ]