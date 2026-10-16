"""Temporary, resumable 2025/2026 rounds 1/7 Bronze -> Silver -> dbt -> 4 CSVs."""
from pathlib import Path
import json
import os
import subprocess
import sys
import pandas as pd
import pyarrow.parquet as pq

import temp_run_layers_export as original

ROOT = Path(__file__).resolve().parent
WORK = ROOT / 'artifacts' / 'export_2025_2026_rounds_01_07'
SEASONS = (2025, 2026)
ROUNDS = (1, 7)


def command(args, cwd, log, env=None):
    print('RUN', *map(str, args), 'LOG', log, flush=True)
    with log.open('w', encoding='utf-8') as file:
        result = subprocess.run(list(map(str, args)), cwd=cwd, env=env, stdout=file, stderr=subprocess.STDOUT)
    if result.returncode:
        print(log.read_text(encoding='utf-8', errors='replace')[-14000:], flush=True)
        raise RuntimeError(f'Command failed ({result.returncode}); see {log}')


def prepare():
    WORK.mkdir(parents=True, exist_ok=True)
    (WORK / 'logs').mkdir(exist_ok=True)
    if not (WORK / 'spark').exists():
        original.prepare_code(WORK)
    adapters = {
        'bronze_to_silver.py': ('silver_laps.write.mode("overwrite").partitionBy("season","round").parquet(output_path)', '_write_arrow(silver_laps, output_path)'),
        'silver_results.py': ('results_silver.write.mode("overwrite").partitionBy("season","round").parquet(str(SILVER_DIR))', '_write_arrow(results_silver, str(SILVER_DIR))'),
        'silver_weather.py': ('weather_silver.write.mode("overwrite").partitionBy("season","round").parquet(str(SILVER_DIR))', '_write_arrow(weather_silver, str(SILVER_DIR))'),
    }
    helper = ('def _write_arrow(frame, path):\n'
              '    import pyarrow.parquet as pq\n'
              '    pq.write_to_dataset(frame.toArrow(), root_path=path, partition_cols=["season", "round"],\n'
              '                        basename_template="part-{i}.parquet", existing_data_behavior="overwrite_or_ignore")\n\n')
    for name, (before, after) in adapters.items():
        script = WORK / 'spark' / name
        text = script.read_text().replace('local[*]', 'local[2]')
        if 'spark.ui.enabled' not in text:
            text = text.replace('.getOrCreate()', '.config("spark.ui.enabled", "false").getOrCreate()')
        if before in text:
            text = helper + text.replace(before, after)
        script.write_text(text)
    telemetry_mart = WORK / 'f1_dbt/f1_dbt/models/marts/analytics/mart_telemetry_lap.sql'
    text = telemetry_mart.read_text()
    text = text.replace('count(*) filter (\n            where drs in (10, 12, 14)\n        ) as drs_active_samples',
                        'case when count(drs) = 0 then cast(null as bigint) else count(*) filter (where drs in (10, 12, 14)) end as drs_active_samples')
    telemetry_mart.write_text(text)
    for dataset in ('results', 'weather'):
        model = WORK / f'f1_dbt/f1_dbt/models/staging/stg_{dataset}.sql'
        model.write_text(model.read_text().replace(f'../../data/silver/{dataset}/**/*.parquet',
                                                  '{{ var("silver_' + dataset + '_path") }}'))
    for season in SEASONS:
        for number in ROUNDS:
            for dataset in ('laps', 'weather', 'results'):
                source = original.find_source(dataset, season, number)
                if source is None:
                    raise RuntimeError(f'Missing {dataset} Bronze for {season}/{number}')
                original.normalize(source, dataset, season, number, WORK)
                print('BRONZE', season, number, dataset, source, flush=True)


def silver_telemetry(season, number):
    source = ROOT / 'data/bronze/telemetry' / f'season={season}' / f'round={number:02d}' / 'r_telemetry_openf1.parquet'
    if not source.exists():
        raise RuntimeError(f'Missing telemetry: {source}; download it first.')
    raw = pq.ParquetFile(source).read().to_pandas()
    native_copy = WORK / 'data/bronze/openf1_telemetry' / f'season={season}' / f'round={number:02d}' / 'data.parquet'
    native_copy.parent.mkdir(parents=True, exist_ok=True)
    import shutil
    shutil.copy2(source, native_copy)
    raw['date'] = pd.to_datetime(raw['date'], utc=True).astype('datetime64[ns, UTC]')
    raw = raw.sort_values(['driver_number', 'date'], kind='stable')
    raw['sample_index'] = raw.groupby('driver_number', sort=False).cumcount()
    laps_source = original.find_source('laps', season, number)
    laps = pq.ParquetFile(laps_source).read().to_pandas()
    laps['lap_start'] = pd.to_datetime(laps['LapStartDate'], utc=True).astype('datetime64[ns, UTC]')
    laps['lap_end'] = laps['lap_start'] + pd.to_timedelta(laps['LapTime'])
    boundaries = WORK / 'data/bronze/openf1_laps' / f'season={season}' / f'round={number:02d}' / 'data.parquet'
    if not laps['lap_start'].notna().any():
        if not boundaries.exists():
            raise ValueError(f'Need absolute OpenF1 lap boundaries: {boundaries}')
        laps = pq.ParquetFile(boundaries).read().to_pandas()
        laps['lap_start'] = pd.to_datetime(laps['date_start'], utc=True, format='mixed')
        laps['lap_end'] = laps['lap_start'] + pd.to_timedelta(laps['lap_duration'], unit='s')
        laps['LapNumber'] = laps['lap_number']
        mapping = raw[['driver_number','driver_code']].drop_duplicates().set_index('driver_number')['driver_code']
        laps['Driver'] = laps['driver_number'].map(mapping)
    chunks = []
    coverage = {}
    for driver, samples in raw.groupby('driver_code', sort=False):
        samples = samples.copy()
        windows = laps.loc[(laps['Driver'] == driver) & laps['lap_start'].notna(),
                           ['lap_start', 'lap_end', 'LapNumber']].sort_values('lap_start').drop_duplicates('lap_start')
        # Missing duration can use the next observed lap boundary, never a guessed duration.
        windows['lap_end'] = windows['lap_end'].fillna(windows['lap_start'].shift(-1))
        if windows.empty:
            samples['LapNumber'] = pd.NA
            samples['lap_start'] = pd.NaT
        else:
            samples = pd.merge_asof(samples.sort_values('date'), windows, left_on='date', right_on='lap_start', direction='backward')
            matched = samples['lap_end'].notna() & (samples['date'] <= samples['lap_end'])
            samples.loc[~matched, 'LapNumber'] = pd.NA
            samples.loc[~matched, 'lap_start'] = pd.NaT
        coverage[driver] = {'rows': len(samples), 'unassigned_lap_rows': int(samples['LapNumber'].isna().sum())}
        chunks.append(samples)
    joined = pd.concat(chunks, ignore_index=True)
    joined['lap_start'] = pd.to_datetime(joined['lap_start'], utc=True)
    output = pd.DataFrame({
        'season': season, 'round': number, 'session': 'R', 'driver': joined['driver_code'],
        'driver_number': joined['driver_number'].astype('Int64'),
        'lap_number': pd.to_numeric(joined['LapNumber'], errors='coerce').astype('Int64'),
        'sample_index': joined['sample_index'].astype('Int64'),
        'timestamp_utc': joined['date'],
        'time_seconds': (joined['date'] - joined['lap_start']).dt.total_seconds(),
        'speed': pd.to_numeric(joined['speed'], errors='coerce').astype('float64'),
        'rpm': pd.to_numeric(joined['rpm'], errors='coerce').astype('float64'),
        'gear': pd.to_numeric(joined['n_gear'], errors='coerce').astype('Int64'),
        'throttle': pd.to_numeric(joined['throttle'], errors='coerce').astype('float64'),
        'brake': joined['brake'].map({0: False, 100: True}).astype('boolean'),
        'drs': pd.to_numeric(joined['drs'], errors='coerce').astype('Int64'),
        'distance': pd.Series(pd.NA, index=joined.index, dtype='Float64'),
        'source': 'openf1',
    })
    for column in ('speed', 'rpm'):
        output.loc[output[column] < 0, column] = float('nan')
    output.loc[~output['gear'].between(0, 8), 'gear'] = pd.NA
    output.loc[~output['throttle'].between(0, 100), 'throttle'] = float('nan')
    target = WORK / 'data/silver/telemetry' / f'season={season}' / f'round={number}' / 'data.parquet'
    target.parent.mkdir(parents=True, exist_ok=True)
    output.to_parquet(target, index=False, coerce_timestamps='us', allow_truncated_timestamps=True)
    if pq.ParquetFile(target).metadata.num_rows != len(raw):
        raise RuntimeError('Telemetry sample count changed in Silver')
    print('SILVER_TELEMETRY', season, number, len(raw), 'rows', len(coverage), 'drivers', flush=True)
    return coverage


def run_spark():
    java = next((ROOT / 'artifacts/runtime/java17').glob('*/bin/java.exe')).parents[1]
    env = dict(os.environ, JAVA_HOME=str(java), SPARK_LOCAL_IP='127.0.0.1',
               PYSPARK_PYTHON=sys.executable, PYSPARK_DRIVER_PYTHON=sys.executable)
    env['PATH'] = str(java / 'bin') + os.pathsep + env.get('PATH', '')
    for season in SEASONS:
        env['F1_SEASON'] = str(season)
        for script in ('bronze_to_silver.py', 'silver_results.py', 'silver_weather.py'):
            output = WORK / 'data/silver' / {'bronze_to_silver.py':'laps','silver_results.py':'results','silver_weather.py':'weather'}[script] / f'season={season}'
            marker = WORK / 'logs' / f'{season}_{script}.success'
            if marker.exists() and output.exists():
                continue
            command([sys.executable, WORK / 'spark' / script], WORK, WORK / 'logs' / f'{season}_{script}.log', env)
            marker.touch()


def run_dbt():
    import shutil
    dbt = shutil.which('dbt', path=str(Path(sys.executable).parent))
    if not dbt:
        raise RuntimeError('Install dbt-duckdb into the current virtual environment')
    project, profiles, database = original.prepare_dbt_paths(WORK)
    variables = {f'silver_{ds}_path': (WORK / 'data/silver' / ds / '**/*.parquet').as_posix() for ds in original.DATASETS}
    command([dbt, 'deps', '--profiles-dir', profiles], project, WORK / 'logs/dbt_deps.log')
    command([dbt, 'build', '--profiles-dir', profiles, '--vars', json.dumps(variables)], project, WORK / 'logs/dbt_build.log')
    return database


def correct_result_units():
    # The existing Spark script divides these derived seconds by 1e9 twice.
    # Correct the isolated output, keeping the production script unchanged.
    for path in (WORK / 'data/silver/results').glob('season=*/round=*/*.parquet'):
        frame = pq.ParquetFile(path).read().to_pandas()
        frame['gap_to_winner_seconds'] = frame['time_seconds'].where(frame['finish_position'] > 1)
        frame['winner_race_time_seconds'] = frame['time_seconds'].where(frame['finish_position'] == 1)
        frame.to_parquet(path, index=False)


def export(database, coverage):
    import duckdb
    folder = WORK / 'csv'
    pending = WORK / 'csv_pending'
    pending.mkdir(exist_ok=True)
    report = {}
    with duckdb.connect(str(database)) as conn:
        for dataset in original.DATASETS:
            relation = f'main.stg_{dataset}'
            query = f'SELECT * FROM {relation} WHERE season IN (2025,2026) AND round IN (1,7)'
            pairs = set(conn.execute(f'SELECT DISTINCT season,round FROM ({query})').fetchall())
            if pairs != {(y,r) for y in SEASONS for r in ROUNDS}:
                raise ValueError(f'{dataset}: missing one of the four races')
            if dataset == 'telemetry':
                for (season,number), expected in coverage.items():
                    actual = dict(conn.execute(f'SELECT driver_code,count(*) FROM {relation} WHERE season=? AND round=? GROUP BY driver_code',[season,number]).fetchall())
                    if actual != {d:c['rows'] for d,c in expected.items()}:
                        raise ValueError(f'Telemetry counts changed {season}/{number}')
            name = f'{dataset}_2025_2026_rounds_01_07.csv'
            path = pending / name
            # Parameterized export path; quotes in paths are not interpolated as SQL.
            conn.execute(f'COPY ({query} ORDER BY season,round) TO ? (FORMAT CSV, HEADER TRUE)', [str(path)])
            report[dataset] = {'rows': conn.execute(f'SELECT count(*) FROM ({query})').fetchone()[0], 'path': str(folder/name)}
    if folder.exists():
        raise RuntimeError(f'Export folder already exists: {folder}')
    pending.rename(folder)
    (WORK / 'manifest.json').write_text(json.dumps({'exports':report, 'telemetry_coverage': {f'{y}/{r}': v for (y,r),v in coverage.items()},
        'scope':'2025 and 2026 rounds 1 and 7; raw OpenF1 telemetry samples retained; exports from dbt staging relations after full project build',
        'engines':'PySpark for laps/results/weather (PyArrow writer); pandas for the OpenF1 telemetry adapter; dbt-duckdb for the entire Gold project.',
        'isolated_fixes':'Corrected double division of result gap/winner seconds. DRS summary stays null if no DRS samples exist.',
        'telemetry_semantics':'Unmatched lap/time and unavailable distance are null. 2026 DRS is null; no overtaking-mode or active-aero data inferred.'},indent=2),encoding='utf-8')
    print('EXPORTS',json.dumps(report),flush=True)


def main():
    prepare()
    coverage = {(y,r):silver_telemetry(y,r) for y in SEASONS for r in ROUNDS}
    run_spark()
    correct_result_units()
    export(run_dbt(), coverage)


if __name__ == '__main__':
    main()
