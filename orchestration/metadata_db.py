import sqlite3
from pathlib import Path
from contextlib import contextmanager

from config import BASE_DIR


METADATA_DIR = BASE_DIR / "metadata"

DB_PATH = METADATA_DIR / "pipeline_metadata.db"


@contextmanager
def get_connection():
    """
    Create a SQLite database connection.
    """

    METADATA_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    connection = sqlite3.connect(
        DB_PATH,
        timeout=30,
    )

    connection.row_factory = sqlite3.Row

    connection.execute(
        "PRAGMA busy_timeout = 30000"
    )

    connection.execute(
        "PRAGMA foreign_keys = ON"
    )

    try:
        with connection:
            yield connection
    finally:
        connection.close()


def initialize_database():
    """
    Create the metadata tables if they
    do not already exist.
    """

    with get_connection() as conn:

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_runs (

                run_id TEXT PRIMARY KEY,

                season INTEGER NOT NULL,

                round_number INTEGER NOT NULL,

                dataset TEXT NOT NULL,

                stage TEXT NOT NULL,

                status TEXT NOT NULL
                    CHECK (
                        status IN (
                            'PENDING',
                            'RUNNING',
                            'SUCCESS',
                            'FAILED',
                            'SKIPPED'
                        )
                    ),

                started_at TEXT,

                finished_at TEXT,

                row_count INTEGER,

                error_message TEXT,

                attempt INTEGER NOT NULL DEFAULT 1,

                created_at TEXT NOT NULL
                    DEFAULT CURRENT_TIMESTAMP

            )
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_pipeline_runs_lookup

            ON pipeline_runs (
                season,
                round_number,
                dataset,
                stage,
                status
            )
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_pipeline_runs_created

            ON pipeline_runs (
                created_at
            )
            """
        )

    print(
        f"Metadata database initialized: {DB_PATH}"
    )


if __name__ == "__main__":
    initialize_database()
