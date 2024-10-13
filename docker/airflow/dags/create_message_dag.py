from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from tasks.functions import *
from airflow.models import Variable

replay_num = Variable.get("current_replay", default_var=0)
with DAG(
    '3_create_message_dag',
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
    # Первая таска, формируем сообщение для отправки
    data_message = PythonOperator(
        task_id='data_message',
        python_callable=data_message,
        op_kwargs={
            'replay_number': replay_num
        },
        provide_context=True,
    )
    end = DummyOperator(task_id='end')
    start >> data_message >> end