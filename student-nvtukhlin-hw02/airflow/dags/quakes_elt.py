from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

DBT = "/home/airflow/dbt-venv/bin/dbt"
DBT_ARGS = (
    "--target candidate --project-dir /opt/project/dbt "
    "--vars '{\"start_date\": \"{{ params.start_date }}\", \"end_date\": \"{{ params.end_date }}\"}'"
)

with DAG(
    dag_id="quakes_elt",
    start_date=datetime(2025, 3, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["hw02", "usgs"],
    params={
        "start_date": Param("2025-03-03", type="string", pattern=r"^\d{4}-\d{2}-\d{2}$"),
        "end_date": Param("2025-03-06", type="string", pattern=r"^\d{4}-\d{2}-\d{2}$"),
        "cases": Param("", type="string"),
    },
) as dag:
    load_raw = BashOperator(
        task_id="load_raw",
        bash_command="cd /opt/project/loader && python load_raw.py --cases '{{ params.cases }}'",
    )

    dbt_build_candidate = BashOperator(
        task_id="dbt_build_candidate",
        bash_command=f"{DBT} seed {DBT_ARGS} && {DBT} run {DBT_ARGS}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT} test {DBT_ARGS}",
    )

    publish = BashOperator(
        task_id="publish",
        bash_command=(
            "cd /opt/project/loader && python publish.py "
            "--start-date '{{ params.start_date }}' --end-date '{{ params.end_date }}' "
            "--run-id '{{ run_id }}'"
        ),
    )

    load_raw >> dbt_build_candidate >> dbt_test >> publish
