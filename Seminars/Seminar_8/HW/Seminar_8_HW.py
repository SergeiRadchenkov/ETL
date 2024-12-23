from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, Float, Date, inspect, insert
from sqlalchemy.orm import sessionmaker
import pandas as pd
from datetime import datetime
import os

# Подключение к базе данных MySQL
engine = create_engine("mysql://root:23061985@localhost:33061/spark")

# Создание сессии
Session = sessionmaker(bind=engine)
session = Session()

# Пути к файлам
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
booking_file = os.path.join(BASE_DIR, "booking.csv")
client_file = os.path.join(BASE_DIR, "client.csv")
hotel_file = os.path.join(BASE_DIR, "hotel.csv")


# Функция загрузки данных из CSV
def load_data(file_path, **kwargs):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Файл {file_path} не найден!")
    df = pd.read_csv(file_path)
    return df.to_dict("records")


# Функция трансформации данных
def transform_data(ti):
    # Получение данных из XCom
    booking_dict = ti.xcom_pull(task_ids='load_booking_data')
    client_dict = ti.xcom_pull(task_ids='load_client_data')
    hotel_dict = ti.xcom_pull(task_ids='load_hotel_data')

    # Преобразование в DataFrame
    booking_df = pd.DataFrame(booking_dict)
    client_df = pd.DataFrame(client_dict)
    hotel_df = pd.DataFrame(hotel_dict)

    # Приведение дат к единому формату
    booking_df['booking_date'] = booking_df['booking_date'].replace('/', '-')
    # booking_df['booking_date'] = booking_df['booking_date'].apply(lambda x: pd.to_datetime(x, errors='coerce').strftime('%Y-%m-%d') if pd.to_datetime(x, errors='coerce') is not pd.NaT else x)

    # Приведение валют к одной
    exchange_rate_USD = 107.0  # Пример курса валют
    exchange_rate_EUR = 111.0  
    exchange_rate_GBP = 150.0  
    booking_df.loc[booking_df['currency'] == 'USD', 'booking_cost'] *= exchange_rate_USD
    booking_df.loc[booking_df['currency'] == 'EUR', 'booking_cost'] *= exchange_rate_EUR
    booking_df.loc[booking_df['currency'] == 'GBP', 'booking_cost'] *= exchange_rate_GBP
    booking_df['currency'] = 'RUB'

    # Объединение таблиц
    combined_df = booking_df.merge(client_df, on='client_id').merge(hotel_df, on='hotel_id')

    # Переименование столбцов
    combined_df.rename(columns={
        'name_x': 'client_name',  # 'name_x' из client.csv
        'name_y': 'hotel_name',  # 'name_y' из hotel.csv
        'address': 'hotel_location'  # 'address' из hotel.csv
    }, inplace=True)

    ti.xcom_push(key='combined_data', value=combined_df.to_dict())
    # return combined_df.to_dict()


# Функция загрузки данных в MySQL
def load_to_mysql(ti):
    combined_dict = ti.xcom_pull(task_ids='transform_data_task', key='combined_data')

    if not combined_dict:
        raise ValueError("Преобразованные данные из XCom отсутствуют!")

    combined_df = pd.DataFrame(combined_dict)

    # Заполнение пропущенных значений в DataFrame
    combined_df['booking_cost'] = pd.to_numeric(combined_df['booking_cost'], errors='coerce')  # Преобразование стоимости в числовой формат
    combined_df['booking_cost'].fillna(0, inplace=True)  # Заполнение пропущенных значений для booking_cost
    combined_df['currency'].fillna('Unknown', inplace=True)  # Заполнение пропущенных значений для currency
    combined_df['booking_date'].fillna('Unknown', inplace=True)  # Заполнение пропущенных значений для booking_date
    combined_df.rename(columns={'name': 'client_name'}, inplace=True)

    # Определение метаданных для таблицы
    metadata = MetaData()

    # Создание инспектора
    inspector = inspect(engine)

    # Проверка, существует ли таблица
    if not inspector.has_table('combined_data'):
        # Если таблица не существует, создаем её
        combined_data_table = Table(
            'combined_data', metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('client_id', Integer),
            Column('hotel_id', Integer),
            Column('booking_date', Date),
            Column('booking_cost', Float),
            Column('currency', String(3)),
            Column('client_name', String(255)),
            Column('hotel_name', String(255)),
            Column('hotel_location', String(255)),
        )
        # Создание таблицы
        metadata.create_all(engine)
        print("Таблица 'combined_data' была успешно создана!")

    else:
        print("Таблица 'combined_data' уже существует!")

    # Преобразуем DataFrame в список словарей
    records = combined_df.to_dict(orient='records')

    # Вставка данных в таблицу через insert()
    combined_data_table = Table('combined_data', metadata, autoload_with=engine)

    # Вставка записей в таблицу
    with engine.connect() as connection:
        connection.execute(insert(combined_data_table), records)

    print("Данные успешно загружены в таблицу MySQL 'combined_data'!")


# Определение DAG
with DAG(
    dag_id="etl_mysql_pipeline",
    schedule_interval=None,
    start_date=datetime(2024, 11, 27),
    catchup=False,
) as dag:
    # Операторы
    load_booking = PythonOperator(
        task_id='load_booking_data',
        python_callable=load_data,
        op_args=[booking_file],
    )
    load_client = PythonOperator(
        task_id='load_client_data',
        python_callable=load_data,
        op_args=[client_file],
    )
    load_hotel = PythonOperator(
        task_id='load_hotel_data',
        python_callable=load_data,
        op_args=[hotel_file],
    )
    transform_data_task = PythonOperator(
        task_id='transform_data_task',
        python_callable=transform_data,
    )
    load_to_db = PythonOperator(
        task_id='load_to_mysql_task',
        python_callable=load_to_mysql,
    )

    # Определение последовательности
    [load_booking, load_client, load_hotel] >> transform_data_task >> load_to_db
