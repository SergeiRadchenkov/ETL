import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta


API_KEY = "9d35adb52527e622fb6ba9487a8e3126"
CITY = "Москве"
LAT = "55.7558"  # широта для Москвы
LON = "37.6173"  # долгота для Москвы


def fetch_weather_data(**kwargs):
    """
    Запрашивает данные о погоде по API и возвращает температуру.
    """
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    response.raise_for_status()  # Если код ответа не 200, выбрасывает ошибку
    data = response.json()
    temperature = data['main']['temp']
    print(f"Температура в {CITY}: {temperature}°C")
    return temperature


def decide_branch(**kwargs):
    """
    Решает, по какой ветке двигаться в зависимости от температуры.
    """
    ti = kwargs['ti']
    temperature = ti.xcom_pull(task_ids='fetch_weather')
    if temperature > 15:
        return 'warm_branch'
    else:
        return 'cold_branch'
    

def print_warm():
    print("Тепло")

def print_cold():
    print("Холодно")


# Определяем DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    dag_id='weather_etl',
    default_args=default_args,
    description='ETL для получения температуры и ветвления',
    schedule_interval='@daily',
    start_date=datetime(2024, 11, 25),
    catchup=False,
) as dag:
    # 1. Получить данные о погоде
    fetch_weather = PythonOperator(
        task_id='fetch_weather',
        python_callable=fetch_weather_data,
        provide_context=True,
    )
    # 2. Ветвление
    branch = BranchPythonOperator(
        task_id='branch_decision',
        python_callable=decide_branch,
        provide_context=True,
    )
    # 3. Операторы для веток
    warm_branch = PythonOperator(
        task_id='warm_branch',
        python_callable=print_warm,
    )
    cold_branch = PythonOperator(
        task_id='cold_branch',
        python_callable=print_cold,
    )
    # 4. Конечная задача
    end = DummyOperator(
        task_id='end',
    )

    # Задаем порядок выполнения задач
    fetch_weather >> branch
    branch >> warm_branch >> end
    branch >> cold_branch >> end
