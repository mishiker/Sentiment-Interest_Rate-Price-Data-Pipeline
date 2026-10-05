import pendulum
from datetime import timedelta
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

PIPELINE = "/home/airflow/pipeline-venv/bin/python /opt/airflow/project/src/main.py"

with DAG(
    dag_id="market_data_pipeline",
    schedule="30 22 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="Europe/Warsaw"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["market-data"],
) as dag:

    prices = BashOperator(
        task_id="prices",
        bash_command=f"{PIPELINE} prices",
        execution_timeout=timedelta(minutes=30),
    )

    fred = BashOperator(
        task_id="fred",
        bash_command=f"{PIPELINE} fred",
        execution_timeout=timedelta(minutes=30),
    )

    news = BashOperator(
        task_id="news",
        bash_command=f"{PIPELINE} news",
        execution_timeout=timedelta(minutes=45),
    )

    sentiment = BashOperator(
        task_id="sentiment",
        bash_command=f"{PIPELINE} sentiment",
        execution_timeout=timedelta(hours=2),
    )
    
    features = BashOperator(
        task_id="features",
        bash_command=f"{PIPELINE} features",
        execution_timeout=timedelta(minutes=15),
    )
    news >> sentiment
    [prices, fred, sentiment] >> features