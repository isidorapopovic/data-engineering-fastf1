import fastf1
from pathlib import Path

YEAR = 2025
ROUND = 1

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"
BRONZE_DIR = DATA_DIR / "bronze"
CACHE_DIR = BASE_DIR / "cache"

OUTPUT_DIR = (
    BRONZE_DIR
    / "telemetry"
    / f"season={YEAR}"
    / f"round={ROUND:02d}"
)


OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

session = fastf1.get_session(
    YEAR,
    ROUND,
    "R"
)

session.load(
    laps=True,
    telemetry=True,
    weather=False,
    messages=False
)

print("Session loaded.")


driver = session.laps["Driver"].dropna().iloc[0]

driver_laps = session.laps.pick_drivers(driver)

print(f"Driver: {driver}")
print(f"Laps available: {len(driver_laps)}")


lap = driver_laps.iloc[0]

telemetry = lap.get_telemetry()

print("\nTELEMETRY COLUMNS")
print(telemetry.columns.tolist())

print("\nSHAPE")
print(telemetry.shape)

print("\nSAMPLE")
print(telemetry.head())


output_file = OUTPUT_DIR / "telemetry.parquet"

telemetry.to_parquet(
    output_file,
    index=False
)

print(f"\nSaved telemetry to:")
print(output_file)