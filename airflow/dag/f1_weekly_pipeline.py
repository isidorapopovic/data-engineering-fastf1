"""Airflow 3 pipeline; set F1_PROJECT_ROOT to the repository on every worker.

Manual backfill: {"season": 2024, "rounds": [1, 7], "force": false}.
Deployment instructions: airflow/README.md.
"""
from datetime import timedelta
import os
from pathlib import Path
import sys

import pendulum
from airflow.sdk import Param, dag, get_current_context, task


def _pipeline():
    # Project imports run inside tasks, never during DAG parsing.
    root = Path(os.environ.get('F1_PROJECT_ROOT', Path(__file__).resolve().parents[2]))
    if not (root / 'config.py').is_file():
        raise RuntimeError('Set F1_PROJECT_ROOT to the mounted repository root.')
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    from orchestration import f1_pipeline
    return f1_pipeline


def _stage(name, plan):
    context = get_current_context()
    return _pipeline().run_stage(name, plan, context['run_id'], context['ti'].try_number)


@dag(
    dag_id='f1_weekly_pipeline',
    schedule='0 9 * * 2',
    start_date=pendulum.datetime(2026, 10, 13, tz='Europe/Belgrade'),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=12),
    default_args={
        'owner': 'f1-data-engineering',
        'retries': 3,
        'retry_delay': timedelta(minutes=15),
        'execution_timeout': timedelta(hours=4),
    },
    params={
        'season': Param(None, type=['null', 'integer'], minimum=2018,
                        description='Null uses F1_SEASON or the current Belgrade year.'),
        'rounds': Param([], type='array', uniqueItems=True,
                        items={'type': 'integer', 'minimum': 1},
                        description='Empty discovers completed races; otherwise limit to these rounds.'),
        'force': Param(False, type='boolean', description='Re-ingest selected rounds.'),
    },
    tags=['formula1', 'fastf1', 'pyspark', 'dbt'],
)
def f1_weekly_pipeline():
    @task
    def check_new_rounds():
        return _pipeline().discover(get_current_context()['params'])

    @task.short_circuit
    def has_rounds(plan):
        return bool(plan['rounds'])

    @task
    def ingest_bronze(plan):
        return _stage('bronze', plan)

    @task
    def validate_bronze(plan):
        return _stage('validate_bronze', plan)

    @task
    def transform_silver(plan):
        return _stage('silver', plan)

    @task
    def validate_silver(plan):
        return _stage('validate_silver', plan)

    @task
    def build_gold(plan):
        return _stage('gold', plan)

    @task
    def finalize(plan):
        return _stage('complete', plan)

    plan = check_new_rounds()
    gate = has_rounds(plan)
    bronze = ingest_bronze(plan)
    validated = validate_bronze(bronze)
    silver = transform_silver(validated)
    checked = validate_silver(silver)
    gold = build_gold(checked)
    gate >> bronze
    finalize(gold)


f1_weekly_pipeline()
