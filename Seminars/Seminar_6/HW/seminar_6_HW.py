from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operator.http import SimpleHttpOperator
from datetime import datetime, timedelta
import random

# Параметры DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Функция для PythonOperator
def generate_and_square_number():
    num = random.randint(1, 100)
    squared = num ** 2
    print(f'Сгенерировано число: {num}')
    print(f'Квадрат данного число: {num}')


# Создание DAG
with DAG(
    'lesson_6_etl_operators',
    default_args=default_args,
    description='DAG with BashOperator, PythonOperator, and SimpleHttpOperator',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['lesson_6', 'operators'],
) as dag:
    
    # 1. BashOperator: Генерация случайного числа
    generate_random_number = BashOperator(
        task_id='generate_random_number',
        bash_command="echo $(( RANDOM % 100 + 1 ))"
    )

    # 2. PythonOperator: Возведение случайного числа в квадрат
    square_random_number = PythonOperator(
        task_id='square_random_number',
        python_callable=generate_and_square_number
    )

    # 3. SimpleHttpOperator: Отправка запроса к API
    fetch_weather = SimpleHttpOperator(
        task_id='fetch_weather',
        http_conn_id='http_default',  # Предварительно настроить подключение
        endpoint='weather/Male',
        method='GET',
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code == 200,
        log_response=True,
    )


    # Задаем последовательность выполнения
    generate_random_number >> square_random_number >> fetch_weather