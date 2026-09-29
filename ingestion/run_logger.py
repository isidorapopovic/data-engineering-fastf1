import json
import uuid

from datetime import datetime, timezone
from pathlib import Path


def new_run_id():

    return str(uuid.uuid4())


def log_run(
    log_file: Path,
    record: dict,
):

    log_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with log_file.open(
        "a",
        encoding="utf-8",
    ) as file:

        file.write(
            json.dumps(
                record,
                default=str,
            )
            + "\n"
        )