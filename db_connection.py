import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def source_db_connection():
    conn = psycopg2.connect(
        host=os.getenv('SOURCE_DB_HOST'),
        port=os.getenv('SOURCE_DB_PORT', '5432'),
        database=os.getenv('SOURCE_DB_NAME'),
        user=os.getenv('SOURCE_DB_USER'),
        password=os.getenv('SOURCE_DB_PASSWORD')
    )
    return conn

def target_db_connection():
    conn = psycopg2.connect(
        host=os.getenv('TARGET_DB_HOST'),
        port=os.getenv('TARGET_DB_PORT', '5432'),
        database=os.getenv('TARGET_DB_NAME'),
        user=os.getenv('TARGET_DB_USER'),
        password=os.getenv('TARGET_DB_PASSWORD')
    )
    return conn

def test_connection():
    """
    Тестирует соединение с базой данных
    """
    try:
        # Получаем соединение
        conn = source_db_connection()
        # Создаем курсор
        cur = conn.cursor()
        # Выполняем простой запрос
        cur.execute('SELECT version();')
        # Получаем результат
        db_version = cur.fetchone()
        # Закрываем курсор и соединение
        cur.close()
        conn.close()
        
        print(f"Успешное подключение к PostgreSQL. Версия: {db_version[0]}")
        return True
    except Exception as e:
        print(f"Ошибка при подключении к PostgreSQL: {e}")
        return False

if __name__ == "__main__":
    # Если файл запущен напрямую, тестируем соединение
    test_connection() 