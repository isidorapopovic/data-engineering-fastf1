"""Isolated checks for scheduling, backfills, recovery and telemetry retention."""
import importlib.util
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from orchestration import f1_pipeline as pipeline
from orchestration import metadata_db


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        for target, name, value in (
            (pipeline, 'BRONZE_DIR', root / 'bronze'),
            (pipeline, 'SILVER_DIR', root / 'silver'),
            (metadata_db, 'METADATA_DIR', root / 'metadata'),
            (metadata_db, 'DB_PATH', root / 'metadata' / 'runs.db'),
        ):
            p = patch.object(target, name, value)
            p.start()
            self.addCleanup(p.stop)
        self.plan = {'season': 2024, 'rounds': [1, 7], 'force': False}

    def seed(self):
        for number in self.plan['rounds']:
            for ds, columns in pipeline.BRONZE_COLUMNS.items():
                self.write(pipeline.dataset_path(pipeline.BRONZE_DIR, ds, 2024, number), columns)
            self.write(pipeline.telemetry_path(2024, number), pipeline.TELEMETRY_COLUMNS)
            for ds in pipeline.SILVER_COLUMNS:
                self.write(pipeline.SILVER_DIR / ds / 'season=2024' / f'round={number}' / 'data.parquet', {'x'})

    @staticmethod
    def write(path, columns):
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({c: [1] for c in columns}), path)

    def test_completed_rounds_skip_and_force_backfill(self):
        self.seed()
        pipeline.run_stage('complete', self.plan)
        with patch('ingestion.discover_rounds.get_completed_rounds', return_value=[1, 7, 8]):
            self.assertEqual(pipeline.discover(self.plan)['rounds'], [])
            forced = dict(self.plan, force=True)
            self.assertEqual(pipeline.discover(forced)['rounds'], [1, 7])
            pipeline.telemetry_path(2024, 7).unlink()
            self.assertEqual(pipeline.discover(self.plan)['rounds'], [7])
            with self.assertRaises(ValueError):
                pipeline.discover(dict(self.plan, rounds=[99]))

    def test_failed_attempt_is_recorded_and_remains_pending(self):
        self.seed()
        pipeline.run_stage('complete', self.plan)
        with patch.object(pipeline, 'build_gold', side_effect=RuntimeError('dbt failed')):
            with self.assertRaisesRegex(RuntimeError, 'dbt failed'):
                pipeline.run_stage('gold', self.plan, attempt=2)
        with metadata_db.get_connection() as conn:
            rows = conn.execute("SELECT status,attempt,error_message FROM pipeline_runs WHERE stage='gold'").fetchall()
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(tuple(row) == ('FAILED', 1, 'dbt failed') for row in rows))
        with patch('ingestion.discover_rounds.get_completed_rounds', return_value=[1, 7]):
            self.assertEqual(pipeline.discover(self.plan)['rounds'], [1, 7])

    def test_bronze_rejects_incompatible_telemetry(self):
        self.seed()
        self.write(pipeline.telemetry_path(2024, 1), {'Speed'})
        with self.assertRaisesRegex(ValueError, 'missing='):
            pipeline.validate_bronze(self.plan)

    def test_silver_duplicate_keys_fail(self):
        self.seed()
        folder = pipeline.SILVER_DIR / 'laps' / 'season=2024' / 'round=1'
        pq.write_table(pa.table({'driver_code': ['VER', 'VER'], 'lap_number': [1, 1],
                                 'is_valid_lap': [True, True]}), folder / 'data.parquet')
        with self.assertRaisesRegex(ValueError, 'duplicate key'):
            pipeline.validate_silver(self.plan)

    def test_valid_silver_partitions_pass(self):
        for ds, columns in pipeline.SILVER_COLUMNS.items():
            for number in self.plan['rounds']:
                self.write(pipeline.SILVER_DIR / ds / 'season=2024' / f'round={number}' / 'data.parquet', columns)
        pipeline.validate_silver(self.plan)

    def test_metadata_attempts_increment_across_retries(self):
        with patch.object(pipeline, 'build_gold', side_effect=RuntimeError('failed')):
            with self.assertRaises(RuntimeError):
                pipeline.run_stage('gold', self.plan)
        with patch.object(pipeline, 'build_gold'):
            pipeline.run_stage('gold', self.plan)
        with metadata_db.get_connection() as conn:
            rows = conn.execute("SELECT attempt,status FROM pipeline_runs WHERE round_number=1 ORDER BY rowid").fetchall()
        self.assertEqual([tuple(row) for row in rows], [(1, 'FAILED'), (2, 'SUCCESS')])

    def test_spark_uses_selected_season_and_propagates_failure(self):
        with patch.object(pipeline.subprocess, 'run') as run:
            pipeline.transform_silver(self.plan)
        self.assertEqual(run.call_count, 4)
        self.assertTrue(all(c.kwargs['env']['F1_SEASON'] == '2024' and c.kwargs['check'] for c in run.call_args_list))

    def test_telemetry_preserves_timing_brake_and_sample_index(self):
        from ingestion import ingest_telemetry as module
        raw = pd.DataFrame({'Date': pd.to_datetime(['2024-03-02T15:00:00', '2024-03-02T15:00:01']),
                            'SessionTime': pd.to_timedelta([10, 11], unit='s'),
                            'Time': pd.to_timedelta([0, 1], unit='s'), 'Brake': [False, True],
                            'Speed': [200., 190.], 'RPM': [10000., 9000.], 'nGear': [6, 5],
                            'Throttle': [100., 0.], 'DRS': [0, 0], 'Distance': [0., 50.]})
        lap = types.SimpleNamespace(get_car_data=lambda: types.SimpleNamespace(add_distance=lambda: raw))
        class Lap:
            def __getitem__(self, key):
                return {'Driver': 'VER', 'LapNumber': 1.}[key]
            get_car_data = staticmethod(lap.get_car_data)
        session = types.SimpleNamespace(load=lambda **kw: None,
                                        laps=types.SimpleNamespace(iterlaps=lambda: [(0, Lap())]))
        with patch.object(module, 'BRONZE_DIR', pipeline.BRONZE_DIR / 'telemetry'), \
             patch.object(module.fastf1, 'get_session', return_value=session), \
             patch.object(module.fastf1.Cache, 'enable_cache'):
            path = module.ingest_telemetry(2024, 1)
        table = pq.ParquetFile(path).read()
        self.assertTrue(pipeline.TELEMETRY_COLUMNS <= set(table.column_names))
        self.assertEqual(table['SampleIndex'].to_pylist(), [0, 1])
        self.assertEqual(table['SessionTime'].to_pylist(), [10_000_000_000, 11_000_000_000])
        self.assertEqual(table['Brake'].to_pylist(), [False, True])


class DagDefinitionTests(unittest.TestCase):
    def test_graph_and_schedule_without_executing_task_bodies(self):
        edges = set()
        configuration = {}
        class Node:
            def __init__(self, name):
                self.name = name
            def __rshift__(self, other):
                edges.add((self.name, other.name))
                return other
        def decorate(fn):
            def call(*args):
                node = Node(fn.__name__)
                for arg in args:
                    edges.add((arg.name, node.name))
                return node
            return call
        class Task:
            __call__ = staticmethod(decorate)
            short_circuit = staticmethod(decorate)
        def dag(**kwargs):
            configuration.update(kwargs)
            return lambda fn: fn
        sdk = types.ModuleType('airflow.sdk')
        sdk.dag, sdk.task = dag, Task()
        sdk.Param = lambda *a, **kw: (a, kw)
        sdk.get_current_context = lambda: self.fail('Task executed during parsing')
        pendulum = types.ModuleType('pendulum')
        pendulum.datetime = lambda *a, **kw: (a, kw)
        path = Path(__file__).resolve().parents[1] / 'airflow' / 'dag' / 'f1_weekly_pipeline.py'
        with patch.dict(sys.modules, {'airflow.sdk': sdk, 'pendulum': pendulum}):
            spec = importlib.util.spec_from_file_location('weekly_dag_test', path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        self.assertEqual(configuration['schedule'], '0 9 * * 2')
        self.assertEqual(configuration['start_date'][1]['tz'], 'Europe/Belgrade')
        self.assertFalse(configuration['catchup'])
        self.assertEqual(configuration['max_active_runs'], 1)
        for edge in [('has_rounds', 'ingest_bronze'), ('ingest_bronze', 'validate_bronze'),
                     ('validate_bronze', 'transform_silver'), ('transform_silver', 'validate_silver'),
                     ('validate_silver', 'build_gold'), ('build_gold', 'finalize')]:
            self.assertIn(edge, edges)


if __name__ == '__main__':
    unittest.main()
