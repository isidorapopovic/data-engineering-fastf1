import pytest

import orchestration.metadata_db as metadata_db

from orchestration.pipeline_state import (
    create_run,
    mark_running,
    mark_success,
    mark_failed,
    get_run,
    get_latest_run,
    dataset_has_succeeded,
)


@pytest.fixture
def temporary_database(
    tmp_path,
    monkeypatch,
):
    """
    Redirect metadata writes into
    a temporary test database.
    """

    monkeypatch.setattr(
        metadata_db,
        "METADATA_DIR",
        tmp_path,
    )

    monkeypatch.setattr(
        metadata_db,
        "DB_PATH",
        tmp_path / "test_metadata.db",
    )

    metadata_db.initialize_database()

    return tmp_path / "test_metadata.db"


def test_successful_run(temporary_database):

    run_id = create_run(
        season=2026,
        round_number=18,
        dataset="laps",
        stage="bronze",
    )

    mark_running(run_id)

    mark_success(
        run_id,
        row_count=1000,
    )

    result = get_run(run_id)

    assert result["status"] == "SUCCESS"
    assert result["row_count"] == 1000
    assert result["started_at"] is not None
    assert result["finished_at"] is not None


def test_failed_run(temporary_database):

    run_id = create_run(
        season=2026,
        round_number=18,
        dataset="weather",
        stage="bronze",
    )

    mark_running(run_id)

    mark_failed(
        run_id,
        "Simulated ingestion failure",
    )

    result = get_run(run_id)

    assert result["status"] == "FAILED"
    assert (
        result["error_message"]
        == "Simulated ingestion failure"
    )


def test_retry_attempt(temporary_database):

    first = create_run(
        2026,
        18,
        "results",
        "bronze",
    )

    mark_running(first)
    mark_failed(first, "Network timeout")

    second = create_run(
        2026,
        18,
        "results",
        "bronze",
    )

    mark_running(second)
    mark_success(second, row_count=20)

    latest = get_latest_run(
        2026,
        18,
        "results",
        "bronze",
    )

    assert latest["attempt"] == 2
    assert latest["status"] == "SUCCESS"

    assert dataset_has_succeeded(
        2026,
        18,
        "results",
        "bronze",
    )


def test_invalid_transition(temporary_database):

    run_id = create_run(
        2026,
        18,
        "laps",
        "bronze",
    )

    mark_running(run_id)
    mark_success(run_id, row_count=1000)

    with pytest.raises(ValueError):
        mark_failed(
            run_id,
            "Cannot fail completed run",
        )