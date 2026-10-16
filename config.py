import os
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo


BASE_DIR = Path(__file__).resolve().parent

#YEAR = 2026
YEAR = int(
    os.getenv(
        "F1_SEASON",
        datetime.now(
            ZoneInfo("Europe/Belgrade")
        ).year
    )
)


DATA_DIR = BASE_DIR / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"

CACHE_DIR = BASE_DIR / "cache"
LOG_DIR = BASE_DIR / "logs"

for path in [
    DATA_DIR,
    BRONZE_DIR,
    SILVER_DIR,
    CACHE_DIR,
    LOG_DIR,
]:
    path.mkdir(parents=True, exist_ok=True)