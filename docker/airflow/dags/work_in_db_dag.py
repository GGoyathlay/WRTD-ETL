from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from tasks.functions import *
from airflow.models import Variable

replay_num = Variable.get("current_replay", default_var=0)
html_data = parsing_replay_html(replay_num)
json_data = parsing_replay_json(replay_num)
# Создание DAG
with DAG(
    '2_work_in_db_dag',
    default_args={
        'owner': 'geronimo',
        'start_date': datetime(2024, 10, 7),
        'retries': 0,
    },
    schedule_interval=None,  # Запускаем только по триггеру
    tags=['wrtd'],
    catchup=False,
) as dag:
    # Старт
    start = DummyOperator(task_id='start')
    # Первая таска, проверяем, есть ли такой реплей в базе. Если есть, то завершаем работу дага
    is_exists = BranchPythonOperator(
        task_id='is_exists',
        python_callable=is_exists,
        op_kwargs={
            'replay_number': replay_num
        },
        provide_context=True,
    )
    load_data_to_db = PythonOperator(
        task_id='load_data_to_db',
        python_callable=load_data_to_db,
        op_kwargs={
            'html_data': html_data,
            'json_data': json_data
        },
        provide_context=True,
    )
    # Если есть данные для записи в сообщение на отправку, то триггерим следующий даг
    trigger_next_dag = TriggerDagRunOperator(
        task_id='trigger_next_dag',
        trigger_dag_id='create_message_dag',  # Здесь указываем айди следующего дага
        wait_for_completion=False,
    )
    # Завершаем даг
    end = DummyOperator(task_id='end')
    # Определение последовательности задач
    start >> is_exists >> [load_data_to_db, end]
    load_data_to_db >> trigger_next_dag >> end
