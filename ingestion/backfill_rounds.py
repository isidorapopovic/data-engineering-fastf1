"""Backfill selected races into the round-first Bronze layout."""
import argparse
import json
from datetime import datetime, timezone

import fastf1
import pandas as pd

from config import BRONZE_DIR
from ingestion.ingest_round import ingest_round
from ingestion.ingest_telemetry import ingest_telemetry


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--season', type=int, required=True)
    parser.add_argument('--rounds', type=int, nargs='+', required=True)
    args = parser.parse_args()
    errors = {}
    for number in sorted(set(args.rounds)):
        if number < 1:
            parser.error('Round numbers must be positive.')
        race_dir = BRONZE_DIR / f'season={args.season}' / f'round={number:02d}'
        telemetry_dir = BRONZE_DIR / 'telemetry' / f'season={args.season}' / f'round={number:02d}'
        race_dir.mkdir(parents=True, exist_ok=True)
        telemetry_dir.mkdir(parents=True, exist_ok=True)
        for dataset in ('race', 'telemetry'):
            try:
                if dataset == 'race':
                    counts = ingest_round(args.season, number, force=True, output_dir=race_dir)
                    print('SAVED', race_dir, json.dumps(counts), flush=True)
                else:
                    path = ingest_telemetry(args.season, number, output_dir=telemetry_dir)
                    print('SAVED', path, flush=True)
            except Exception as exc:
                errors[f'{number}/{dataset}'] = str(exc)
                print('FAILED', number, dataset, repr(exc), flush=True)
                if dataset == 'race':
                    # Results use a separate source and may still be available.
                    try:
                        session = fastf1.get_session(args.season, number, 'R')
                        session.load(laps=False, telemetry=False, weather=False, messages=False)
                        results = pd.DataFrame(session.results).copy()
                        if results.empty:
                            raise ValueError('No race results returned.')
                        results['ingested_at'] = datetime.now(timezone.utc)
                        path = race_dir / 'results.parquet'
                        results.to_parquet(path, index=False, coerce_timestamps='us', allow_truncated_timestamps=True)
                        print('SAVED_PARTIAL', path, len(results), 'rows', flush=True)
                    except Exception as result_exc:
                        errors[f'{number}/results'] = str(result_exc)
    if errors:
        print('BACKFILL_ERRORS', json.dumps(errors), flush=True)
        raise SystemExit(1)


if __name__ == '__main__':
    main()
