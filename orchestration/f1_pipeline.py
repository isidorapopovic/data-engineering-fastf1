"""Project pipeline stages shared by Airflow and local callers."""
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import sys
from zoneinfo import ZoneInfo

from config import BASE_DIR, BRONZE_DIR, SILVER_DIR
from ingestion.state import DATASETS, dataset_path
from orchestration.metadata_db import get_connection, initialize_database
from orchestration.pipeline_state import create_run, mark_running, mark_success, mark_failed

LOG = logging.getLogger(__name__)
TELEMETRY_COLUMNS = {'Date', 'SessionTime', 'Time', 'Session', 'SampleIndex',
                     'Driver', 'LapNumber', 'Speed', 'RPM', 'nGear', 'Throttle', 'Brake', 'DRS', 'Distance'}
BRONZE_COLUMNS = {
    'laps': {'Driver', 'LapNumber', 'LapTime', 'Compound'},
    'results': {'DriverId', 'Abbreviation', 'Position'},
    'weather': {'Time', 'AirTemp', 'TrackTemp', 'Rainfall'},
}
SILVER_COLUMNS = {
    'laps': {'driver_code', 'lap_number', 'is_valid_lap'},
    'results': {'driver_id', 'driver_code'},
    'weather': {'time_raw_ns', 'is_valid_weather'},
    'telemetry': {'session', 'driver', 'lap_number', 'sample_index', 'timestamp_utc',
                  'time_seconds', 'speed', 'rpm', 'gear', 'throttle', 'brake', 'drs', 'distance'},
}


def telemetry_path(year, number):
    return BRONZE_DIR / 'telemetry' / str(year) / f'round_{number:02d}' / 'r_telemetry.parquet'


def parquet_rows(path, required=()):
    import pyarrow.parquet as pq
    table = pq.ParquetFile(path).read()
    missing = set(required) - set(table.column_names)
    if missing or not table.num_rows:
        raise ValueError(f'Invalid dataset {path}: rows={table.num_rows}, missing={sorted(missing)}')
    return table.num_rows


def _valid(path, required=()):
    try:
        parquet_rows(path, required)
        return True
    except (OSError, ValueError):
        return False


def discover(params):
    from ingestion.discover_rounds import get_completed_rounds
    initialize_database()
    year = params.get('season') or int(os.getenv('F1_SEASON', datetime.now(ZoneInfo('Europe/Belgrade')).year))
    completed = set(get_completed_rounds(year))
    requested = set(params.get('rounds') or completed)
    if requested - completed:
        raise ValueError(f'Rounds are not completed races in {year}: {sorted(requested - completed)}')
    force = bool(params.get('force', False))
    with get_connection() as conn:
        done = {row[0] for row in conn.execute(
            "SELECT round_number FROM pipeline_runs WHERE season=? AND stage='complete' AND status='SUCCESS' "
            "AND rowid IN (SELECT MAX(rowid) FROM pipeline_runs GROUP BY season,round_number)",
            (year,),
        )}
    rounds = []
    for number in sorted(requested):
        bronze_ok = all(_valid(dataset_path(BRONZE_DIR, ds, year, number), BRONZE_COLUMNS[ds]) for ds in DATASETS)
        bronze_ok = bronze_ok and _valid(telemetry_path(year, number), TELEMETRY_COLUMNS)
        silver_ok = all(any((SILVER_DIR / ds / f'season={year}' / f'round={number}').glob('*.parquet'))
                        for ds in SILVER_COLUMNS)
        if force or number not in done or not bronze_ok or not silver_ok:
            rounds.append(number)
    LOG.info('Season %s, rounds to process: %s', year, rounds)
    return {'season': year, 'rounds': rounds, 'force': force}


def ingest_bronze(plan):
    from ingestion.ingest_round import ingest_round
    from ingestion.ingest_telemetry import ingest_telemetry
    for number in plan['rounds']:
        if plan['force'] or not all(_valid(dataset_path(BRONZE_DIR, ds, plan['season'], number), BRONZE_COLUMNS[ds]) for ds in DATASETS):
            ingest_round(plan['season'], number, force=plan['force'])
        if plan['force'] or not _valid(telemetry_path(plan['season'], number), TELEMETRY_COLUMNS):
            ingest_telemetry(plan['season'], number)


def validate_bronze(plan):
    for number in plan['rounds']:
        for ds in DATASETS:
            parquet_rows(dataset_path(BRONZE_DIR, ds, plan['season'], number), BRONZE_COLUMNS[ds])
        parquet_rows(telemetry_path(plan['season'], number), TELEMETRY_COLUMNS)


def transform_silver(plan):
    env = dict(os.environ, F1_SEASON=str(plan['season']))
    for script in ('bronze_to_silver.py', 'silver_results.py', 'silver_weather.py', 'silver_telemetry.py'):
        subprocess.run([sys.executable, str(BASE_DIR / 'spark' / script)],
                       cwd=BASE_DIR, env=env, check=True)


def validate_silver(plan):
    import pyarrow.parquet as pq
    for ds, required in SILVER_COLUMNS.items():
        for number in plan['rounds']:
            folder = SILVER_DIR / ds / f"season={plan['season']}" / f'round={number}'
            files = sorted(folder.glob('*.parquet'))
            if not files:
                raise ValueError(f'Missing Silver partition: {folder}')
            total = 0
            keys_seen = set()
            for path in files:
                table = pq.ParquetFile(path).read()
                missing = required - set(table.column_names)
                if missing:
                    raise ValueError(f'{path}: missing columns {sorted(missing)}')
                total += table.num_rows
                keys = {'laps': ['driver_code', 'lap_number'], 'results': ['driver_id'],
                        'weather': ['time_raw_ns'],
                        'telemetry': ['driver', 'session', 'lap_number', 'sample_index']}[ds]
                for row in table.select(keys).to_pylist():
                    key = tuple(row[k] for k in keys)
                    if None in key or key in keys_seen:
                        raise ValueError(f'{path}: null or duplicate key {key}')
                    keys_seen.add(key)
            if not total:
                raise ValueError(f'Empty Silver partition: {folder}')


def build_gold(plan):
    project = BASE_DIR / 'f1_dbt' / 'f1_dbt'
    variables = {f'silver_{ds}_path': (SILVER_DIR / ds / '**' / '*.parquet').as_posix()
                 for ds in SILVER_COLUMNS}
    executable = os.environ.get('F1_DBT_EXECUTABLE') or shutil.which(
        'dbt', path=str(Path(sys.executable).parent) + os.pathsep + os.environ.get('PATH', ''))
    if not executable:
        raise RuntimeError('Install dbt-duckdb in the worker environment or set F1_DBT_EXECUTABLE.')
    command = [executable]
    subprocess.run(command + ['deps'], cwd=project, check=True)
    subprocess.run(command + ['build', '--vars', json.dumps(variables)], cwd=project, check=True)


def run_stage(stage, plan, airflow_run_id='local', attempt=1):
    """Record each attempt and propagate failures to Airflow for retries."""
    initialize_database()
    ids = []
    LOG.info("Airflow run %s, task attempt %s, stage %s", airflow_run_id, attempt, stage)
    for number in plan['rounds']:
        run_id = create_run(plan['season'], number, 'all', stage)
        ids.append(run_id)
        mark_running(run_id)
    operations = {'bronze': ingest_bronze, 'validate_bronze': validate_bronze,
                  'silver': transform_silver, 'validate_silver': validate_silver, 'gold': build_gold,
                  'complete': lambda _: None}
    try:
        operations[stage](plan)
    except Exception as exc:
        _finish(ids, 'FAILED', str(exc))
        raise
    _finish(ids, 'SUCCESS', None)
    return plan


def _finish(ids, status, error):
    for run_id in ids:
        if status == 'SUCCESS':
            mark_success(run_id)
        else:
            mark_failed(run_id, error)
