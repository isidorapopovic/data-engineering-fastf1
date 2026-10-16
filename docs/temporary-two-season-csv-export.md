# Temporary two-season, two-round CSV export

This experiment processed race-session data for rounds **1 and 7 of both 2025
and 2026** through Bronze, Silver, and the complete dbt project. It generated
four combined CSV files: laps, results, weather, and sample-level telemetry.
Each file contains all four season/round combinations.

The CSVs, raw downloads, caches, portable Java runtime, generated Spark/dbt
workspace, DuckDB database, and execution logs remain local under ignored
`data/`, `cache/`, and `artifacts/` paths. They are deliberately not committed.
This commit contains the temporary scripts, supporting ingestion changes,
tests, and this report. It does not publish or deploy the Airflow pipeline.

## Scope and output counts

| Season | Round | Race | Laps | Results | Weather | Telemetry samples | Telemetry drivers |
|---|---:|---|---:|---:|---:|---:|---:|
| 2025 | 1 | Australian Grand Prix | 927 | 20 | 178 | 768,420 | 20 |
| 2025 | 7 | Emilia Romagna Grand Prix | 1,207 | 20 | 150 | 662,460 | 20 |
| 2026 | 1 | Australian Grand Prix | 1,006 | 22 | 148 | 685,960 | 22 |
| 2026 | 7 | Barcelona Grand Prix | 1,236 | 22 | 150 | 730,730 | 22 |
| **Total** | | | **4,376** | **84** | **626** | **2,847,570** | **84 driver/race combinations** |

The four files are saved locally in:

```text
artifacts/export_2025_2026_rounds_01_07/csv/
  laps_2025_2026_rounds_01_07.csv
  results_2025_2026_rounds_01_07.csv
  weather_2025_2026_rounds_01_07.csv
  telemetry_2025_2026_rounds_01_07.csv
```

The telemetry CSV is 211,361,842 bytes (approximately 211 MB). It is not a
per-lap or per-driver aggregate. Every downloaded source sample is retained,
including repeated samples and samples before or after the race. Completeness
here means matching the downloaded source counts; it does not guarantee that
the upstream feed has no missing samples or placeholder values.

## Why an alternative telemetry source was needed

FastF1 returned HTTP 401 from the primary F1 car-data endpoint and HTTP 404
from its mirror for the requested telemetry sessions. The failure occurred
before the telemetry response could be parsed. Changing a DataFrame schema
cannot resolve that HTTP access failure.

OpenF1's public historical API successfully supplied driver-level `car_data`.
The downloads used these session keys:

| Season | Round | OpenF1 session key |
|---|---:|---:|
| 2025 | 1 | 9693 |
| 2025 | 7 | 9987 |
| 2026 | 1 | 11234 |
| 2026 | 7 | 11307 |

Native OpenF1 telemetry is kept separate from FastF1-format telemetry:

```text
data/bronze/telemetry/season=<year>/round=<round>/r_telemetry_openf1.parquet
```

Each download has a schema/coverage manifest. Per-driver Parquet caches under
`artifacts/temp_openf1_<year>/` allow successful downloads to be reused after a
transient failure. The temporary downloader retries connection failures,
HTTP 429, and server errors, and reports incomplete driver coverage.

Results, laps, and weather reused readable local FastF1 Bronze files, including
cached session data from the earlier ingestion runs.

## Bronze preparation

The experiment reads both supported local race-data layouts:

```text
data/bronze/season=<year>/round=<round>/<dataset>.parquet
data/bronze/<dataset>/season=<year>/round=<round>/data.parquet
```

`ingest_round()` and `ingest_telemetry()` gained an optional `output_dir` so
backfills can use explicit destinations without changing the existing default
paths used by orchestration.

The successful export uses an isolated workspace:

```text
artifacts/export_2025_2026_rounds_01_07/
  data/bronze/
  data/silver/
  spark/
  f1_dbt/f1_dbt/
  profiles/
  logs/
  gold.duckdb
  telemetry_coverage.json
  manifest.json
  csv/
```

Bronze preparation copies the selected inputs into that workspace. Physical
Parquet types are normalized for Spark: durations use nanosecond integers,
timestamps use microseconds, and nullable booleans remain nullable. Preparation
checks that copying/normalization does not change source row counts.

## Silver transformations

### Laps, results, and weather

The experiment runs isolated copies of the project's existing PySpark scripts:

- `spark/bronze_to_silver.py`
- `spark/silver_results.py`
- `spark/silver_weather.py`

These perform column selection, naming, type conversion, cleaning, quality
flags, and the existing dataset-specific deduplication. Silver laps therefore
reflect the project's filtering/deduplication rules, rather than promising
an unchanged count of every raw lap record.

Java was initially unavailable, so a checksum-verified portable Java 17 runtime
was downloaded under ignored `artifacts/runtime/`. Spark then computed the
transformations successfully, but Windows Hadoop could not write Parquet
without `winutils.exe`. A temporary PyArrow writer persists the Spark-computed
Arrow tables as season/round-partitioned Parquet. No unofficial Hadoop binary
was installed. These adapters affect only the generated workspace copies.

The isolated results output also corrects an existing unit conversion problem:
`winner_race_time_seconds` and `gap_to_winner_seconds` had been divided by
one billion twice. They now use the already-converted seconds values.

### Telemetry

A temporary pandas adapter transforms the native OpenF1 schema into the column
contract required by `stg_telemetry`. It:

- Preserves every downloaded sample and driver identity.
- Retains UTC sample timestamps and source provenance in Silver.
- Assigns a stable sample index within each driver's session sequence.
- Converts speed, RPM, gear, throttle, brake, and DRS to suitable nullable types.
- Applies basic physical range checks without dropping whole sample rows.
- Uses OpenF1 lap start/duration boundaries to associate samples with known laps.
- Leaves lap number and lap-relative time null when no supported lap interval
  can be assigned.
- Leaves distance null because the retrieved OpenF1 car-data responses do not
  provide that measurement.

OpenF1 lap boundaries were necessary because the cached FastF1 lap files lacked
absolute `LapStartDate` values. The supporting OpenF1 lap responses were saved
in the isolated Bronze workspace under `data/bronze/openf1_laps/`.

The lap matching is an attribution step, not a source measurement. Samples
outside a known lap interval are preserved without inventing a lap. The
coverage manifest records unassigned sample counts per driver.

## 2026 DRS and overtaking-mode semantics

The 2026 OpenF1 responses have a null `drs` field. Null means unavailable,
not that a measured DRS state is disabled. The downloaded responses do not
provide an overtaking-mode or active-aero state.

No such state was inferred from speed, throttle, acceleration, or legacy DRS
values. The isolated telemetry lap mart was adapted to return null DRS-active
sample counts when all source DRS values are null, rather than reporting a
measured zero. The production dbt model was not changed by this experiment.

References checked during investigation:

- [OpenF1 API and car-data schema](https://openf1.org/docs/#car-data)
- [FastF1 discussion of 2026 aero/overtaking telemetry](https://github.com/theOehrly/Fast-F1/issues/864)
- [Formula 1's 2026 terminology](https://corp.formula1.com/f1-2026-regulations-terminology-update/)

## Gold transformations and CSV export

The temporary runner copies the dbt project into the isolated workspace and
creates a separate DuckDB profile/database. It runs `dbt deps` and **the full
`dbt build`**, including staging, intermediate, dimension, fact, analytics, and
data-test nodes. A dbt-duckdb adapter was installed into the local virtual
environment for this execution.

Two staging models originally referenced relative Silver paths. Their isolated
copies now use explicit Silver path variables, allowing the database views to
be queried from outside the dbt project directory.

**Build result: 23 models and 66 data tests passed; 89 successful nodes,
zero warnings, zero errors, zero skipped nodes.**

The four CSVs are exported from the dbt-built staging relations after this full
Gold build. This intentionally preserves each dataset's staging grain. In
particular, telemetry is exported from `stg_telemetry`, because the valid-row
intermediate/fact models exclude samples without a valid lap assignment.
Exporting only that filtered fact would lose some of the samples requested.
The complete Gold fact and mart models are still built in `gold.duckdb`.

CSV files are initially written to a pending directory and published as the
final four-file directory only after the export checks succeed.

## Validation performed

- All four CSVs were read back independently with DuckDB.
- Row totals were checked against the export manifest.
- Each CSV was checked for exactly the four requested season/round pairs.
- All 84 telemetry driver/race row counts were compared against their
  downloaded source counts.
- The 2026 telemetry CSV rows were checked to retain null DRS values.
- dbt's `run_results.json` was checked for all 89 successful build/test nodes.
- Three isolated downloader tests passed, covering duplicate/unknown-channel
  preservation, null DRS handling, incomplete-download behavior, and session
  selection.
- Three isolated export-helper tests passed, covering four-file combined
  exports, failed publication for incomplete race coverage, and nanosecond
  duration normalization.
- Python compilation and Git whitespace checks passed.

The local `manifest.json` records output counts, engine choices, temporary
corrections, semantics, and verification results. `telemetry_coverage.json`
provides per-driver source row counts and unassigned-lap counts. Full dbt
artifacts and logs remain in the isolated workspace.

## Temporary files added

These are investigation/backfill utilities, not a replacement production DAG:

| File | Purpose |
|---|---|
| `ingestion/backfill_rounds.py` | Targeted race/telemetry backfills with round-first output folders and an available-results fallback. |
| `temp_download_2026_telemetry.py` | Resumable native OpenF1 telemetry downloads; supports both 2025 and 2026 despite its original filename. |
| `temp_run_layers_export.py` | Earlier isolated 2024/2025 export experiment and shared preparation helpers; its default scope is not the final successful export scope. |
| `temp_export_2025_2026.py` | Final isolated 2025/2026 runner, telemetry adapter, Spark output adapter, dbt build, and CSV export. |
| `tests/test_temp_download_2026_telemetry.py` | Focused tests for the temporary OpenF1 downloader. |
| `tests/test_temp_layers_export.py` | Focused tests for the earlier helper's normalization and export behavior. |

## Running or extending the experiment

Download/reuse OpenF1 telemetry:

```powershell
.\.venv\Scripts\python.exe temp_download_2026_telemetry.py --season 2025
.\.venv\Scripts\python.exe temp_download_2026_telemetry.py --season 2026
```

The final runner uses the prepared local Bronze inputs, supporting OpenF1 lap
boundary files, portable Java directory, dbt-duckdb installation, and workspace:

```powershell
.\.venv\Scripts\python.exe temp_export_2025_2026.py
```

This is a temporary experiment, not a one-command clean-machine installer.
It does not currently fetch missing OpenF1 lap boundaries automatically or
install Java/dbt itself. The export step refuses to replace an existing final
`csv/` directory. Retain the published files or choose a new experiment
workspace before attempting another final publication.

Before promoting this work into production, unify the Bronze path convention,
add the OpenF1 adapter and its source/provenance contract to orchestration,
manage the runtime dependencies explicitly, and add dedicated tests for the
final runner's lap-attribution and temporary fixes. Do not feed native OpenF1
Parquet directly into the existing FastF1 Silver reader without an adapter.
