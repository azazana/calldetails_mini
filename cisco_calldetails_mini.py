from airflow import DAG

from airflow.operators.python import PythonOperator

from requests import request

from datetime import datetime, timedelta

import psycopg2



default_args = {

    'owner': 'airflow',

    'depends_on_past': False,

    'start_date': datetime(2025, 1, 31)

}


dag = DAG(

    dag_id='cisco_calldetails_mini',

    default_args=default_args,

    schedule_interval='@daily',

    description='Получение истории звонков cisco',

    catchup=False,

    max_active_runs=1

)


def get_max_timestamp_from_postgresql(pg_connection, logger):
    """
    Получает максимальное значение dateTimeOrigination из PostgreSQL таблицы.
    Возвращает timestamp или None если таблица пустая/не существует.
    """
    pg_cursor = pg_connection.cursor()
    
    try:
        # Проверяем, существует ли таблица
        pg_cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'calldetails_mini_3'
            )
        """)
        table_exists = pg_cursor.fetchone()[0]
        
        if table_exists:
            # Получаем максимальное значение dateTimeOrigination из PostgreSQL
            pg_cursor.execute("SELECT MAX(\"dateTimeOrigination\") FROM calldetails_mini_3")
            max_timestamp_result = pg_cursor.fetchone()[0]
            return max_timestamp_result
        else:
            return None
    finally:
        pg_cursor.close()


def get_timestamp_filter(max_timestamp, logger):
    """
    Определяет timestamp фильтр на основе максимальной даты из PostgreSQL.
    Если таблица пустая/не существует, возвращает timestamp для 01.01.2025.
    """
    if max_timestamp is not None:
        timestamp_filter = int(max_timestamp)
        logger.info(f"Найдена максимальная дата в PostgreSQL: {timestamp_filter}")
        
        # Конвертируем timestamp в читаемый формат для логирования
        max_datetime = datetime.fromtimestamp(timestamp_filter)
        current_datetime = datetime.now()
        logger.info(f"Берем данные после: {max_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Текущее время: {current_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        # Таблица пустая или не существует, берем данные с 01.01.2025
        start_date = datetime(2025, 1, 1)
        timestamp_filter = int(start_date.timestamp())
        logger.info(f"Таблица PostgreSQL пустая/не существует, берем данные с 01.01.2025")
        logger.info(f"Timestamp фильтр: {timestamp_filter}")
        
        # Конвертируем timestamp в читаемый формат для логирования
        start_datetime = datetime.fromtimestamp(timestamp_filter)
        current_datetime = datetime.now()
        logger.info(f"Период выборки: с {start_datetime.strftime('%Y-%m-%d %H:%M:%S')} по {current_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    
    return timestamp_filter


def log_data_statistics(data, column_names, date_column, logger):
    """
    Логирует статистику по загруженным данным.
    """
    if data:
        min_timestamp = min(row[column_names.index(date_column)] for row in data)
        max_timestamp = max(row[column_names.index(date_column)] for row in data)
        min_datetime = datetime.fromtimestamp(int(min_timestamp))
        max_datetime = datetime.fromtimestamp(int(max_timestamp))
        
        logger.info(f"Успешно перенесено {len(data)} записей в PostgreSQL")
        logger.info(f"Диапазон загруженных данных: с {min_datetime.strftime('%Y-%m-%d %H:%M:%S')} по {max_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        logger.info("Нет новых данных для переноса")


def create_postgresql_table_if_not_exists(pg_cursor, column_names, logger):
    """
    Создает таблицу calldetails_mini_3 в PostgreSQL, если она не существует.
    """
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS calldetails_mini_3 (
            {', '.join([f'"{col}" TEXT' for col in column_names])}
        )
    """
    pg_cursor.execute(create_table_query)
    logger.info(f"Таблица calldetails_mini_3 создана/проверена с {len(column_names)} колонками")


def main():
    """
    Основная функция для переноса данных из MySQL в PostgreSQL.
    Получает максимальную дату из PostgreSQL таблицы calldetails_mini_3 и собирает
    только новые данные из MySQL таблицы ciscocdr.calldetails_mini после этой даты
    по колонке dateTimeOrigination (Unix timestamp). Если таблица пустая или не существует,
    берет данные с 01.01.2025.
    """
    import mysql.connector
    import logging
    from datetime import datetime, timedelta
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    # Настройка логирования для отслеживания процесса
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    try:
        # Подключение к MySQL
        mysql_connection = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            user=os.getenv("MYSQL_USER"),
            charset='utf8',
            password=os.getenv("MYSQL_PASSWORD")
        )
        
        # Подключение к PostgreSQL
        pg_connection_params = {
            "host": os.getenv("PG_HOST"),
            "port": os.getenv("PG_PORT"),
            "dbname": os.getenv("PG_DBNAME"),
            "user": os.getenv("PG_USER"),
            "password": os.getenv("PG_PASSWORD")
        }
        
        pg_connection = psycopg2.connect(**pg_connection_params)
        
        # Получаем максимальную дату из PostgreSQL и определяем фильтр
        max_timestamp = get_max_timestamp_from_postgresql(pg_connection, logger)
        timestamp_filter = get_timestamp_filter(max_timestamp, logger)
        
        # Получаем структуру таблицы из MySQL
        mysql_cursor = mysql_connection.cursor()
        mysql_cursor.execute("SELECT * FROM ciscocdr.calldetails_mini LIMIT 1")
        sample_data = mysql_cursor.fetchall()
        columns_info = mysql_cursor.description
        
        # Извлекаем названия колонок
        column_names = [col[0] for col in columns_info]
        logger.info(f"Найдено колонок: {len(column_names)}")
        
        # Используем колонку dateTimeOrigination для фильтрации по Unix timestamp
        date_column = 'dateTimeOrigination'
        
        # Проверяем, есть ли колонка dateTimeOrigination
        if date_column not in column_names:
            logger.error(f"Колонка {date_column} не найдена в таблице!")
            logger.info(f"Доступные колонки: {column_names}")
            raise ValueError(f"Колонка {date_column} не найдена в таблице")
        
        logger.info(f"Используем колонку для фильтрации: {date_column}")
        
        # Формируем SQL запрос с фильтрацией по Unix timestamp
        # Используем строгое сравнение > для получения только новых записей
        query = f"""
            SELECT * FROM ciscocdr.calldetails_mini 
            WHERE {date_column} > {timestamp_filter}
            ORDER BY {date_column} ASC
        """
        
        logger.info(f"Выполняем запрос: {query}")
        
        # Выполняем запрос
        mysql_cursor.execute(query)
        data = mysql_cursor.fetchall()
        
        logger.info(f"Найдено записей для переноса: {len(data)}")
        
        if data:
            # Подготавливаем данные для вставки в PostgreSQL
            pg_cursor = pg_connection.cursor()
            
            # Создаем таблицу в PostgreSQL, если она не существует
            create_postgresql_table_if_not_exists(pg_cursor, column_names, logger)
            
            # Не удаляем существующие записи, так как добавляем только новые
            logger.info("Добавляем только новые записи без удаления существующих")
            
            # Вставляем новые данные
            insert_query = f"""
                INSERT INTO calldetails_mini_3 ({', '.join([f'"{col}"' for col in column_names])})
                VALUES ({', '.join(['%s'] * len(column_names))})
            """
            
            pg_cursor.executemany(insert_query, data)
            pg_connection.commit()
            
            # Логируем статистику по загруженным данным
            log_data_statistics(data, column_names, date_column, logger)
            
            pg_cursor.close()
        else:
            logger.info("Нет данных для переноса")
        
        mysql_cursor.close()
        mysql_connection.close()
        pg_connection.close()
        
        logger.info("Перенос данных завершен успешно")
        
    except Exception as e:
        logger.error(f"Ошибка при переносе данных: {str(e)}")
        raise


task1 = PythonOperator(

    task_id='ciscon', python_callable=main, dag=dag)