"""Temporary 2026 telemetry recovery through OpenF1's public historical API.

Run: .venv/Scripts/python.exe temp_download_2026_telemetry.py
Native OpenF1 data is saved separately; this does not fabricate FastF1 lap/time
fields or equate missing DRS to overtaking mode. Every returned row is retained,
including pre/post-race samples and duplicates. Per-driver cache enables resume.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import time

import pandas as pd
import pyarrow.parquet as pq
import requests

ROOT = Path(__file__).resolve().parent
API = 'https://api.openf1.org/v1/'
RACES = {1: ('2026-03-08', 'Melbourne'), 7: ('2026-06-14', 'Catalunya')}
RACES_2025 = {1: ('2025-03-16', 'Melbourne'), 7: ('2025-05-18', 'Imola')}
CACHE = ROOT / 'artifacts' / 'temp_openf1_2026'


def get(endpoint, params):
    for attempt in range(4):
        try:
            response = requests.get(API + endpoint, params=params, timeout=(15, 90))
        except requests.RequestException:
            if attempt == 3:
                raise
            print('RETRY connection', endpoint, params, flush=True)
            time.sleep(2 ** attempt)
            continue
        if response.status_code == 429 or response.status_code >= 500:
            if attempt < 3:
                delay = min(float(response.headers.get('Retry-After', 2 ** attempt)), 30)
                print('RETRY', response.status_code, endpoint, params, 'wait', delay, flush=True)
                time.sleep(delay)
                continue
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            raise ValueError(f'Unexpected {endpoint} response type')
        return data


def select_session(sessions, number, season=2026):
    date, circuit = (RACES if season == 2026 else RACES_2025)[number]
    matches = [s for s in sessions if s['date_start'].startswith(date)
               and s['circuit_short_name'] == circuit and s['session_name'] == 'Race'
               and s['year'] == season and not s.get('is_cancelled', False)]
    if len(matches) != 1:
        raise ValueError(f'Round {number}: expected one race on {date} at {circuit}, got {len(matches)}')
    return matches[0]


def download_round(number, session):
    season = session.get('year', 2026)
    key = session['session_key']
    folder = CACHE / f'session={key}'
    folder.mkdir(parents=True, exist_ok=True)
    drivers = get('drivers', {'session_key': key})
    drivers = {int(d['driver_number']): d for d in drivers}
    if not drivers:
        raise ValueError(f'No drivers returned for session {key}')
    frames, coverage, failures, empty = [], {}, {}, []
    for driver_number, driver in sorted(drivers.items()):
        cache = folder / f'driver={driver_number}.parquet'
        try:
            if cache.exists():
                frame = pq.ParquetFile(cache).read().to_pandas()
            else:
                rows = get('car_data', {'session_key': key, 'driver_number': driver_number})
                if not rows:
                    empty.append(driver_number)
                    print('NO SAMPLES', number, driver_number, driver['name_acronym'], flush=True)
                    continue
                frame = pd.DataFrame(rows)
                if not {'date', 'driver_number', 'session_key', 'speed', 'rpm', 'n_gear', 'throttle', 'brake'} <= set(frame):
                    raise ValueError(f'Unexpected car-data columns: {list(frame)}')
                if set(frame['session_key']) != {key} or set(frame['driver_number']) != {driver_number}:
                    raise ValueError('Response has another session or driver')
                frame['date'] = pd.to_datetime(frame['date'], utc=True, format='mixed')
                frame.to_parquet(cache, index=False, coerce_timestamps='us', allow_truncated_timestamps=True)
            frame['season'] = season
            frame['round'] = number
            frame['session'] = 'R'
            frame['driver_code'] = driver['name_acronym']
            frame['source'] = 'openf1'
            frames.append(frame)
            coverage[str(driver_number)] = {'driver_code': driver['name_acronym'], 'rows': len(frame)}
            print('DOWNLOADED', number, driver_number, driver['name_acronym'], len(frame), 'rows', flush=True)
        except Exception as exc:
            failures[str(driver_number)] = str(exc)
            print('DRIVER FAILED', number, driver_number, str(exc), flush=True)
    manifest = {
        'source': 'OpenF1 public historical car_data', 'api': API,
        'session_metadata': session, 'round': number,
        'downloaded_at': datetime.now(timezone.utc).isoformat(),
        'coverage': coverage, 'empty_driver_responses': empty, 'failed_drivers': failures,
        'semantics': 'DRS is retained as provided. Null does not mean disabled. No overtaking-mode or active-aero state is inferred.',
        'sample_scope': 'Every sample returned by the session-key/driver query, including pre/post-race samples; no deduplication or aggregation.',
        'pipeline_compatibility': 'Native OpenF1 schema: needs a separate adapter for existing FastF1 Silver/dbt models.',
    }
    if failures or not frames:
        (folder / 'incomplete_manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
        raise RuntimeError(f'Round {number}: incomplete driver downloads; resume by rerunning this script.')
    data = pd.concat(frames, ignore_index=True)
    target_dir = ROOT / 'data' / 'bronze' / 'telemetry' / f'season={season}' / f'round={number:02d}'
    target_dir.mkdir(parents=True, exist_ok=True)
    output = target_dir / 'r_telemetry_openf1.parquet'
    pending = target_dir / 'r_telemetry_openf1.parquet.pending'
    data.to_parquet(pending, index=False, coerce_timestamps='us', allow_truncated_timestamps=True)
    verified = pq.ParquetFile(pending).read()
    if verified.num_rows != sum(c['rows'] for c in coverage.values()):
        raise ValueError('Combined Parquet count differs from driver response counts')
    pending.replace(output)
    manifest.update({'rows': len(data), 'drivers_with_samples': len(coverage),
                     'columns': {field.name: str(field.type) for field in verified.schema},
                     'null_counts': {name: verified[name].null_count for name in verified.column_names},
                     'output': str(output), 'first_sample': str(data['date'].min()),
                     'last_sample': str(data['date'].max())})
    (target_dir / 'openf1_telemetry_manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    print('VERIFIED', output, len(data), 'rows', len(coverage), 'drivers; empty drivers:', empty, flush=True)
    return manifest


def main():
    global CACHE
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rounds', nargs='+', type=int, choices=tuple(RACES), default=[1, 7])
    parser.add_argument('--season', type=int, choices=(2025, 2026), default=2026)
    args = parser.parse_args()
    CACHE = ROOT / 'artifacts' / f'temp_openf1_{args.season}'
    sessions = get('sessions', {'year': args.season, 'session_name': 'Race'})
    failures = {}
    for number in args.rounds:
        try:
            download_round(number, select_session(sessions, number, args.season))
        except Exception as exc:
            failures[number] = str(exc)
            print('ROUND FAILED', number, repr(exc), flush=True)
    if failures:
        raise SystemExit(json.dumps(failures))


if __name__ == '__main__':
    main()
