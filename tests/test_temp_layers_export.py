"""Local checks for the temporary four-file export, without Java or downloads."""
from pathlib import Path
import tempfile
import unittest

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import temp_run_layers_export as runner


class ExportTests(unittest.TestCase):
    def test_telemetry_normalization_preserves_rows_and_nanosecond_times(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / 'raw.parquet'
            df = pd.DataFrame({'Driver': ['VER', 'VER', 'HAM'], 'LapNumber': [1., 1., 1.],
                               'Time': pd.to_timedelta([0, 1, 0], unit='s'),
                               'SessionTime': pd.to_timedelta([10, 11, 10], unit='s'),
                               'Date': pd.to_datetime(['2024-03-02'] * 3),
                               'nGear': [4, 4, 5], 'DRS': [0, 0, 0],
                               'RPM': [9000, 9000, 9500], 'Speed': [180, 180, 190],
                               'Throttle': [100, 100, 100], 'Brake': [False] * 3,
                               'Distance': [0., 50., 0.]})
            df.to_parquet(source, index=False)
            count = runner.normalize(source, 'telemetry', 2024, 1, root / 'work')
            target = root / 'work/data/bronze/telemetry/2024/round_01/r_telemetry.parquet'
            table = pq.ParquetFile(target).read()
            self.assertEqual(count, 3)
            self.assertEqual(table['SampleIndex'].to_pylist(), [0, 1, 0])
            self.assertEqual(table['SessionTime'].to_pylist(), [10_000_000_000, 11_000_000_000, 10_000_000_000])
            self.assertEqual(table['RPM'].type, pa.float64())

    def test_four_combined_csvs_preserve_every_driver_sample(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            database = root / 'gold.duckdb'
            expected = {}
            rows = []
            for season in runner.SEASONS:
                for number in runner.ROUNDS:
                    expected[(season, number)] = {'VER': 3, 'HAM': 3}
                    for driver in ('VER', 'HAM'):
                        for sample in range(3):
                            rows.append({'season': season, 'round': number, 'driver_code': driver,
                                         'lap_number': 1, 'sample_index': sample})
            with duckdb.connect(str(database)) as conn:
                table = pa.Table.from_pylist(rows)
                conn.register('samples', table)
                conn.execute('CREATE TABLE stg_telemetry AS SELECT * FROM samples')
                for dataset in ('laps', 'results', 'weather'):
                    conn.execute(f'CREATE TABLE stg_{dataset} AS SELECT DISTINCT season, round FROM samples')
            report = runner.export_csvs(database, root, expected)
            self.assertEqual(len(list((root / 'csv').glob('*.csv'))), 4)
            self.assertEqual(report['telemetry']['rows'], 24)
            csv = pd.read_csv(report['telemetry']['csv'])
            self.assertEqual(set(zip(csv.season, csv['round'])), {(2024, 1), (2024, 7), (2025, 1), (2025, 7)})
            self.assertEqual(csv.groupby('driver_code').size().to_dict(), {'HAM': 12, 'VER': 12})

    def test_count_mismatch_does_not_publish_csv_folder(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            database = root / 'gold.duckdb'
            with duckdb.connect(str(database)) as conn:
                for dataset in runner.DATASETS:
                    conn.execute(f'CREATE TABLE stg_{dataset} AS SELECT 2024 AS season, 1 AS round')
            with self.assertRaisesRegex(ValueError, 'missing season/round'):
                runner.export_csvs(database, root, {})
            self.assertFalse((root / 'csv').exists())


if __name__ == '__main__':
    unittest.main()
