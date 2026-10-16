The DAG targets Airflow 3. Use Linux, WSL2, or containers with Java 17+ and
install `requirements-airflow.txt` using the constraints appropriate to your
Airflow/Python version. Configure a dbt `f1_dbt` profile with the DuckDB adapter;
`DBT_PROFILES_DIR` can point to the profile directory. The existing Windows
virtual environment is not an Airflow deployment.

Set `F1_PROJECT_ROOT` to the mounted repository root on the scheduler and every
worker. Set `AIRFLOW__CORE__DAGS_FOLDER` to `<repository>/airflow/dag`.
Mount the same persistent `data`, `cache`, `metadata`, dbt project and DuckDB
target on every worker. The SQLite metadata database assumes shared local
storage; use a single worker deployment for this project. Keep all runs of
this pipeline under the DAG's `max_active_runs=1` constraint.

The schedule is Tuesday 09:00 Europe/Belgrade, including daylight saving time.
Catchup is disabled. The default season is `F1_SEASON`, falling back to the
current Belgrade year. Discovery considers only races before today's UTC
date, avoiding partially published race-day data. No pending rounds causes
all processing tasks to be skipped successfully.

Trigger a historical run with these DAG parameters:

```json
{"season": 2024, "rounds": [1, 7], "force": false}
```

An empty `rounds` list discovers all completed races in that season.
`force=true` re-downloads selected rounds. A round is complete only after
the full DAG, including `dbt build` tests, succeeds. Successful Bronze files
are reused on retry; incomplete or incompatible telemetry is downloaded again.
Each processing task attempt is recorded in `metadata/pipeline_metadata.db`.
Stage records cover the selected batch; a failed batch is retried together.

Silver transformations rebuild the available Bronze partitions for the selected
season. Dynamic partition overwrite preserves other seasons and untouched
partitions. dbt builds the complete project across all available Silver seasons.
Existing historical telemetry without `Session` or `SampleIndex` must be
re-ingested via a backfill before it can pass the new checks.

FastF1 network access is required. Source HTTP errors fail the task and use
Airflow's three retries at 15-minute intervals; they never mark a round complete.
No external notifications are configured.

Local isolated checks (no download, Java, dbt or Airflow required):

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p test_f1_pipeline.py
```

In the configured Airflow environment, validate DAG loading before enabling it:

```bash
airflow dags list-import-errors
airflow dags show f1_weekly_pipeline
```
