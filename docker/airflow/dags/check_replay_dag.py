from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from tasks.functions import check_replay


# Создание DAG
with DAG(
    '1_check_replay_dag',
    default_args={
        'owner': 'geronimo',
        'start_date': datetime(2024, 10, 7),
        'retries': 0,
    },
    schedule_interval='*/5 * * * *',  # Запускаем каждые 5 минут
    tags=['wrtd'],
    catchup=False,
) as dag:
    # Старт
    start = DummyOperator(task_id='start')
    # Проверяем реплеи
    check_replay = BranchPythonOperator(
        task_id='check_replay',
        python_callable=check_replay,
        provide_context=True,
    )
    # Если есть новый реплей, то триггерим следующий даг
    trigger_next_dag = TriggerDagRunOperator(
        task_id='trigger_next_dag',
        trigger_dag_id='work_in_db_dag',  # Здесь указываем айди следующего дага
        wait_for_completion=False,
    )
    # Если нового реплея нет, то завершаем даг
    end = DummyOperator(task_id='end')
    # Определение последовательности задач
    start >> check_replay >> [trigger_next_dag, end]
    trigger_next_dag >> end