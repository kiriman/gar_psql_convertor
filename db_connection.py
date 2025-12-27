import os
import psycopg2
from dotenv import load_dotenv

# Загружаем .env с правильной кодировкой
load_dotenv(encoding='utf-8')

def source_db_connection():
    # Получаем параметры и очищаем их от невидимых символов
    host = os.getenv('SOURCE_DB_HOST', 'localhost').strip()
    port = os.getenv('SOURCE_DB_PORT', '5432').strip()
    database = os.getenv('SOURCE_DB_NAME', '').strip()
    user = os.getenv('SOURCE_DB_USER', '').strip()
    password = os.getenv('SOURCE_DB_PASSWORD', '').strip()
    
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )
    return conn

def target_db_connection():
    # Получаем параметры и очищаем их от невидимых символов
    host = os.getenv('TARGET_DB_HOST', 'localhost').strip()
    port = os.getenv('TARGET_DB_PORT', '5432').strip()
    database = os.getenv('TARGET_DB_NAME', '').strip()
    user = os.getenv('TARGET_DB_USER', '').strip()
    password = os.getenv('TARGET_DB_PASSWORD', '').strip()
    
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
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