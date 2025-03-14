#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Модуль для удаления таблиц из базы данных.
"""
from table_definitions import get_all_tables

def drop_table(cursor, table_name):
    try:
        # Проверяем, существует ли таблица
        cursor.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = '{table_name}'
            );
        """)
        
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            print(f"Таблица '{table_name}' не существует, нечего удалять.")
            return True
        
        # Удаляем таблицу
        cursor.execute(f'DROP TABLE "{table_name}" CASCADE;')
        
        # Фиксируем изменения
        cursor.connection.commit()
        
        print(f"Таблица '{table_name}' успешно удалена.")
        return True
    except Exception as e:
        cursor.connection.rollback()
        print(f"Ошибка при удалении таблицы '{table_name}': {e}")
        return False

def drop_tables(cursor):
    try:
        # Получаем список таблиц в обратном порядке, чтобы избежать проблем с зависимостями
        tables = get_all_tables()
        tables.reverse()
        
        for table_name in tables:
            if not drop_table(cursor, table_name):
                return False
        return True
    except Exception as e:
        cursor.connection.rollback()
        print(f"Ошибка удаления таблиц: {e}")
        return False 