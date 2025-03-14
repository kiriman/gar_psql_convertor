from db_connection import source_db_connection

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