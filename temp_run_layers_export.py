"""Temporary Bronze -> Spark Silver -> all dbt models -> four combined CSVs.

Run: .venv/Scripts/python.exe temp_run_layers_export.py
Check prerequisites/data without downloads: add --check
Reuse only local Bronze data: add --offline
Scope: race sessions, seasons 2024/2025, rounds 1/7. Telemetry is NOT aggregated.
"""
import argparse
from datetime import datetime
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
SEASONS = (2024, 2025)
ROUNDS = (1, 7)
DATASETS = ('laps', 'results', 'weather', 'telemetry')
REQUIRED = {
    'laps': {'Driver', 'LapNumber', 'LapTime', 'Compound'},
    'results': {'DriverId', 'Abbreviation', 'Position'},
    'weather': {'Time', 'AirTemp', 'TrackTemp', 'Rainfall'},
    'telemetry': {'Date', 'SessionTime', 'Time', 'Driver', 'LapNumber', 'Speed',
                  'RPM', 'nGear', 'Throttle', 'Brake', 'DRS', 'Distance'},
}


def candidates(dataset, season, number):
    bronze = ROOT / 'data' / 'bronze'
    if dataset == 'telemetry':
        return [bronze / dataset / f'season={season}' / f'round={number:02d}' / filename
                for filename in ('r_telemetry.parquet', 'telemetry.parquet', 'data.parquet')] + [
                    bronze / dataset / str(season) / f'round_{number:02d}' / 'r_telemetry.parquet']
    return [bronze / f'season={season}' / f'round={number:02d}' / f'{dataset}.parquet',
            bronze / dataset / f'season={season}' / f'round={number:02d}' / 'data.parquet']


def find_source(dataset, season, number):
    import pyarrow.parquet as pq
    for path in candidates(dataset, season, number):
        try:
            source = pq.ParquetFile(path)
            if source.metadata.num_rows and REQUIRED[dataset] <= set(source.schema_arrow.names):
                return path
        except (OSError, ValueError):
            pass
    return None


def prerequisites():
    errors = []
    for name in ('fastf1', 'pandas', 'pyarrow', 'pyspark', 'duckdb'):
        if importlib.util.find_spec(name) is None:
            errors.append(f'Missing Python package: {name}')
    dbt = shutil.which('dbt', path=str(Path(sys.executable).parent) + os.pathsep + os.environ.get('PATH', ''))
    if not dbt:
        errors.append('Missing dbt CLI: install dbt-duckdb into this Python environment.')
    java = shutil.which('java')
    if not java and os.getenv('JAVA_HOME'):
        java = str(Path(os.environ['JAVA_HOME']) / 'bin' / ('java.exe' if os.name == 'nt' else 'java'))
        if not Path(java).is_file():
            java = None
    if not java:
        errors.append('Java not found: configure JAVA_HOME for Java 17+ or add java to PATH.')
    return dbt, errors


def inventory():
    sources, missing = {}, []
    for season in SEASONS:
        for number in ROUNDS:
            for dataset in DATASETS:
                path = find_source(dataset, season, number)
                key = (dataset, season, number)
                if path:
                    sources[key] = path
                else:
                    missing.append(key)
    return sources, missing


def fetch_missing(missing):
    from ingestion.ingest_round import ingest_round
    from ingestion.ingest_telemetry import ingest_telemetry
    errors = []
    for season in SEASONS:
        for number in ROUNDS:
            required = {ds for ds, y, r in missing if (y, r) == (season, number)}
            race_dir = ROOT / 'data' / 'bronze' / f'season={season}' / f'round={number:02d}'
            if required - {'telemetry'}:
                try:
                    ingest_round(season, number, output_dir=race_dir)
                except Exception as exc:
                    errors.append(f'{season}/{number} race data: {exc}')
            if 'telemetry' in required:
                try:
                    folder = ROOT / 'data' / 'bronze' / 'telemetry' / f'season={season}' / f'round={number:02d}'
                    ingest_telemetry(season, number, output_dir=folder)
                except Exception as exc:
                    errors.append(f'{season}/{number} telemetry: {exc}')
    for error in errors:
        print('DOWNLOAD FAILED:', error, flush=True)


def normalize(source, dataset, season, number, workspace):
    """Normalize physical types in the isolated copy; preserve every source row."""
    import pandas as pd
    import pyarrow.parquet as pq
    df = pq.ParquetFile(source).read().to_pandas()
    original_rows = len(df)
    if dataset == 'telemetry':
        df['Season'], df['Round'] = season, number
        if 'Session' not in df:
            df['Session'] = 'R'  # All accepted source filenames represent race sessions.
        if set(df['Session'].dropna().str.upper()) != {'R'}:
            raise ValueError(f'{source}: expected race telemetry only.')
        if df['Driver'].isna().any() or df['LapNumber'].isna().any():
            raise ValueError(f'{source}: telemetry rows lack driver or lap identity.')
        if 'SampleIndex' not in df:
            df['SampleIndex'] = df.groupby(['Session', 'Driver', 'LapNumber'], sort=False).cumcount()
        for column in ('SampleIndex', 'nGear', 'DRS'):
            df[column] = df[column].astype('Int64')
        df['LapNumber'] = df['LapNumber'].astype('float64')
        for column in ('RPM', 'Speed', 'Throttle', 'Distance', 'RelativeDistance',
                       'DistanceToDriverAhead', 'X', 'Y', 'Z'):
            if column in df:
                df[column] = pd.to_numeric(df[column], errors='coerce').astype('float64')
        df['Brake'] = df['Brake'].astype('boolean')
        target = workspace / 'data' / 'bronze' / 'telemetry' / str(season) / f'round_{number:02d}' / 'r_telemetry.parquet'
    else:
        target = workspace / 'data' / 'bronze' / dataset / f'season={season}' / f'round={number:02d}' / 'data.parquet'
        if dataset == 'laps' and 'Deleted' in df:
            df['Deleted'] = df['Deleted'].astype('boolean')
    # Spark schemas use nanosecond integer durations and microsecond timestamps.
    for column in df:
        if pd.api.types.is_timedelta64_dtype(df[column]):
            values = df[column].to_numpy(dtype='timedelta64[ns]').astype('int64')
            df[column] = pd.Series(values, index=df.index).astype('Int64').mask(df[column].isna())
    target.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(target, index=False, coerce_timestamps='us', allow_truncated_timestamps=True)
    if pq.ParquetFile(target).metadata.num_rows != original_rows:
        raise ValueError(f'Row count changed while preparing {source}')
    return original_rows


def prepare_code(workspace):
    spark_dir = workspace / 'spark'
    spark_dir.mkdir(parents=True)
    for name in ('bronze_to_silver.py', 'silver_results.py', 'silver_weather.py', 'silver_telemetry.py'):
        text = (ROOT / 'spark' / name).read_text(encoding='utf-8-sig')
        if name == 'silver_telemetry.py':
            # The normal pipeline deduplicates. This one-off export retains ALL samples.
            old = 'telemetry_silver = telemetry_silver.dropDuplicates(\n    duplicate_keys\n)'
            if old not in text:
                raise RuntimeError('Telemetry script changed; review the sample-preservation adapter.')
            text = text.replace(old, '# Sample-preserving temporary export: no deduplication.')
        (spark_dir / name).write_text(text, encoding='utf-8')
    project = workspace / 'f1_dbt' / 'f1_dbt'
    shutil.copytree(ROOT / 'f1_dbt' / 'f1_dbt', project,
                    ignore=shutil.ignore_patterns('target', 'logs', 'dbt_packages', '*.duckdb*',
                                                 'profiles.yml', '.user.yml', 'powerbi_exports', '.env*'))
    profiles = workspace / 'profiles'
    profiles.mkdir()
    database = workspace / 'gold.duckdb'
    (profiles / 'profiles.yml').write_text(
        'f1_dbt:\n  target: temp_export\n  outputs:\n    temp_export:\n      type: duckdb\n'
        f'      path: {json.dumps(database.as_posix())}\n      schema: main\n      threads: 2\n', encoding='utf-8')
    return project, profiles, database


def build_layers(workspace, dbt):
    from orchestration import f1_pipeline as pipeline
    pipeline.BASE_DIR = workspace
    pipeline.SILVER_DIR = workspace / 'data' / 'silver'
    for season in SEASONS:
        plan = {'season': season, 'rounds': list(ROUNDS), 'force': False}
        pipeline.transform_silver(plan)
        pipeline.validate_silver(plan)
    project, profiles, database = prepare_dbt_paths(workspace)
    variables = {f'silver_{ds}_path': (workspace / 'data' / 'silver' / ds / '**' / '*.parquet').as_posix()
                 for ds in DATASETS}
    command = [dbt, '--profiles-dir', str(profiles)]
    subprocess.run(command + ['deps'], cwd=project, check=True)
    subprocess.run(command + ['build', '--vars', json.dumps(variables)], cwd=project, check=True)
    return database


def prepare_dbt_paths(workspace):
    return workspace / 'f1_dbt' / 'f1_dbt', workspace / 'profiles', workspace / 'gold.duckdb'


def export_csvs(database, workspace, expected_telemetry):
    import duckdb
    output = workspace / '_csv_in_progress'
    output.mkdir()
    # Staging models are dbt-built relations retaining original dataset grain.
    # Intermediate/fact telemetry filters invalid rows, so it is not used here.
    expected_pairs = {(y, r) for y in SEASONS for r in ROUNDS}
    report = {}
    with duckdb.connect(str(database)) as conn:
        for dataset in DATASETS:
            relation = f'main.stg_{dataset}'
            query = f'SELECT * FROM {relation} WHERE season IN (2024,2025) AND round IN (1,7)'
            pairs = set(conn.execute(f'SELECT DISTINCT season, round FROM ({query})').fetchall())
            if pairs != expected_pairs:
                raise ValueError(f'{dataset}: missing season/round pairs: {expected_pairs - pairs}')
            if dataset == 'telemetry':
                for season, number in sorted(expected_pairs):
                    actual = dict(conn.execute(f'SELECT driver_code, count(*) FROM {relation} WHERE season=? AND round=? GROUP BY driver_code', [season, number]).fetchall())
                    if actual != expected_telemetry[(season, number)]:
                        raise ValueError(f'Telemetry sample counts changed for {season}/{number}: {actual}')
            order = 'season, round, driver_code, lap_number, sample_index' if dataset == 'telemetry' else 'season, round'
            path = output / f'{dataset}_2024_2025_rounds_01_07.csv'
            conn.execute(f'COPY ({query} ORDER BY {order}) TO ? (FORMAT CSV, HEADER TRUE)', [str(path)])
            count = conn.execute(f'SELECT count(*) FROM ({query})').fetchone()[0]
            report[dataset] = {'csv': str(workspace / 'csv' / path.name), 'rows': count}
    output.rename(workspace / 'csv')
    for dataset, details in report.items():
        print('EXPORTED', dataset, details['rows'], 'rows ->', details['csv'], flush=True)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='Report dependencies and missing Bronze inputs only.')
    parser.add_argument('--offline', action='store_true', help='Do not attempt missing-data downloads.')
    args = parser.parse_args()
    dbt, errors = prerequisites()
    for error in errors:
        print('PREREQUISITE:', error)
    if importlib.util.find_spec('pyarrow') is None:
        raise SystemExit(1)
    sources, missing = inventory()
    for dataset, season, number in missing:
        print('MISSING BRONZE:', dataset, season, number)
    print(f'Found {len(sources)}/16 required nonempty Bronze files.')
    if args.check:
        raise SystemExit(bool(errors or missing))
    if errors:
        raise SystemExit('Fix prerequisites above, then retry. No pipeline data was changed.')
    if missing and not args.offline:
        fetch_missing(missing)
        sources, missing = inventory()
    if missing:
        raise SystemExit(f'Cannot produce complete CSVs: {len(missing)} Bronze inputs remain unavailable. No partial CSVs exported.')
    workspace = ROOT / 'artifacts' / ('temp_layers_' + datetime.now().strftime('%Y%m%d_%H%M%S_%f'))
    workspace.mkdir(parents=True)
    prepare_code(workspace)
    import pyarrow.parquet as pq
    expected = {}
    provenance = []
    for (dataset, season, number), source in sources.items():
        count = normalize(source, dataset, season, number, workspace)
        provenance.append({'dataset': dataset, 'season': season, 'round': number, 'source': str(source), 'rows': count})
        if dataset == 'telemetry':
            df = pq.ParquetFile(source).read(columns=['Driver']).to_pandas()
            expected[(season, number)] = df['Driver'].str.strip().str.upper().value_counts().to_dict()
    database = build_layers(workspace, dbt)
    report = export_csvs(database, workspace, expected)
    (workspace / 'run_manifest.json').write_text(json.dumps({'inputs': provenance, 'exports': report}, indent=2), encoding='utf-8')
    print('DONE:', workspace / 'csv')


if __name__ == '__main__':
    main()
