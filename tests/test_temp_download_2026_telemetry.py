"""Isolated tests for the temporary OpenF1 downloader."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow.parquet as pq
import temp_download_2026_telemetry as download


class DownloadTests(unittest.TestCase):
    def test_preserves_duplicate_samples_unknown_channels_and_null_drs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            row = {'date': '2026-03-08T04:00:00+00:00', 'driver_number': 63,
                   'session_key': 11234, 'speed': 200, 'rpm': 10000, 'n_gear': 6,
                   'throttle': 100, 'brake': 0, 'drs': None, 'future_channel': 123}
            with patch.object(download, 'ROOT', root), patch.object(download, 'CACHE', root / 'cache'), \
                 patch.object(download, 'get', side_effect=[
                     [{'driver_number': 63, 'name_acronym': 'RUS'}], [row, row]]):
                manifest = download.download_round(1, {'session_key': 11234})
            table = pq.ParquetFile(manifest['output']).read()
            self.assertEqual(table.num_rows, 2)
            self.assertEqual(table['drs'].null_count, 2)
            self.assertEqual(table['future_channel'].to_pylist(), [123, 123])
            self.assertNotIn('OvertakeMode', table.column_names)
            self.assertNotIn('LapNumber', table.column_names)

    def test_failure_does_not_publish_combined_parquet(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch.object(download, 'ROOT', root), patch.object(download, 'CACHE', root / 'cache'), \
                 patch.object(download, 'get', side_effect=[
                     [{'driver_number': 63, 'name_acronym': 'RUS'}], RuntimeError('HTTP failure')]):
                with self.assertRaisesRegex(RuntimeError, 'incomplete'):
                    download.download_round(1, {'session_key': 11234})
            self.assertFalse(list((root / 'data').rglob('*.parquet')))
            self.assertTrue((root / 'cache/session=11234/incomplete_manifest.json').exists())

    def test_session_selection_rejects_wrong_circuit(self):
        with self.assertRaises(ValueError):
            download.select_session([{'date_start': '2026-06-14T13:00:00+00:00',
                                      'circuit_short_name': 'Madring', 'session_name': 'Race',
                                      'year': 2026}], 7)


if __name__ == '__main__':
    unittest.main()
