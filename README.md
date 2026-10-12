# Formula 1 Data Engineering Pipeline

A local data engineering project that collects Formula 1 race data with
[FastF1](https://docs.fastf1.dev/), writes Parquet files in a Bronze layer,
transforms selected datasets with PySpark into Silver, and builds analytical
models with dbt and DuckDB.

## Pipeline overview

```text
FastF1
  ├── race results, laps, weather ──> data/bronze/ ──> PySpark ──> data/silver/
  └── car telemetry ───────────────> data/bronze/telemetry/

data/silver/ ──> dbt + DuckDB ──> analytical models
```

The standard race ingestion and Silver scripts use the season configured by
`YEAR` in `config.py` (currently 2025). The telemetry ingestion script accepts
its season as a command-line argument.

## Requirements

- Windows with PowerShell (commands below use PowerShell)
- Python and pip
- Java 17 for local PySpark
- Network access when FastF1 needs to download data
- `dbt-duckdb` for the dbt stage

Install the Python requirements from the repository root:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install dbt-duckdb
```

Make sure Java 17 is installed and available on `PATH` before running Spark.
On PowerShell, if script activation is restricted, invoke the environment's
Python directly instead:

```powershell
.\.venv\Scripts\python.exe --version
```

## Run the race-data pipeline

Run commands from the repository root.

### 1. Ingest missing completed race rounds

```powershell
.\.venv\Scripts\python.exe -m ingestion.run_incremental
```

The script uses `YEAR` in `config.py`, checks the FastF1 event schedule, and
ingests rounds that do not yet have all three Bronze datasets. It logs each
round attempt to `metadata/pipeline_runs.jsonl`. The runner catches and logs a
round's ingestion error, so check the log as well as the console output.

Expected Bronze layout:

```text
data/bronze/
  laps/season=2025/round=01/data.parquet
  results/season=2025/round=01/data.parquet
  weather/season=2025/round=01/data.parquet
```

Change `YEAR` in `config.py` when you want the standard pipeline to use another
season.

### 2. Build the Silver datasets

Run the PySpark scripts from the repository root:

```powershell
.\.venv\Scripts\python.exe .\spark\bronze_to_silver.py
.\.venv\Scripts\python.exe .\spark\silver_results.py
.\.venv\Scripts\python.exe .\spark\silver_weather.py
```

These scripts write Parquet outputs under `data/silver/laps`,
`data/silver/results`, and `data/silver/weather`. They use overwrite mode, so
each run replaces the corresponding Silver output.

### 3. Build and test dbt models

```powershell
Push-Location .\f1_dbt\f1_dbt
dbt build --profiles-dir .
Pop-Location
```

`dbt build` runs the project's models and tests against the DuckDB profile in
`f1_dbt/f1_dbt/profiles.yml`. The local DuckDB file is created in the dbt
project directory.

## Ingest telemetry

Telemetry is a separate ingestion path; it is not included in
`ingestion.run_incremental`.

Ingest the race session for one round:

```powershell
.\.venv\Scripts\python.exe -m ingestion.ingest_telemetry --season 2025 --round 1
```

Ingest the selected session for all rounds in the FastF1 season schedule:

```powershell
.\.venv\Scripts\python.exe -m ingestion.ingest_telemetry --season 2025 --all-rounds
```

The default session is `R` (race). Select another FastF1 session code with
`--session`, for example `--session Q` for qualifying. Outputs are written
under `data/bronze/telemetry/<season>/round_<round>/<session>_telemetry.parquet`.
The all-rounds option iterates every scheduled round; it does not skip existing
files and can fail when data for a scheduled session is unavailable.

## Docker option

The Compose service starts a Spark container with the repository mounted at
`/app`. From the repository root:

```powershell
docker compose up -d --build
docker compose exec spark python3 spark/bronze_to_silver.py
docker compose exec spark python3 spark/silver_results.py
docker compose exec spark python3 spark/silver_weather.py
docker compose down
```

The Docker image installs the packages in `requirements.txt`; it does not
install `dbt-duckdb`. Run the dbt commands locally unless you add dbt to the
container setup.

## Project layout

```text
config.py                 Shared paths and standard pipeline season
ingestion/                FastF1 ingestion and run logging
spark/                    Bronze inspection and Bronze-to-Silver transforms
data/bronze/              Raw Parquet inputs
data/silver/              Cleaned Parquet outputs
f1_dbt/f1_dbt/            dbt models, tests, and DuckDB profile
cache/                    FastF1 download cache
metadata/                 Ingestion run log
```

Generated data, cache files, DuckDB databases, and local run output are
excluded from Git by `.gitignore`.

## Current limitations

- `spark/silver_telemetry.py` is not yet compatible with the current output of
  `ingestion.ingest_telemetry`: it expects a different telemetry schema and a
  `round` column that the ingested Parquet does not provide. The telemetry
  Bronze data is therefore not part of the documented Silver/dbt pipeline yet.
- Telemetry ingestion processes each scheduled round sequentially and
  overwrites that round/session's output if run again.
- FastF1 availability, network access, and local Java/Spark setup affect
  ingestion and transformation runs.
