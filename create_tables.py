#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Модуль для создания таблиц в базе данных.
"""
from table_definitions import get_table_definition, get_all_tables

def create_table(cursor, table_name):
    try:        
        # Получаем SQL-запрос для создания таблицы
        sql_query = get_table_definition(table_name)
        
        if not sql_query:
            print(f"Неизвестная таблица: {table_name}")
            return False
        
        # Создаем таблицу
        cursor.execute(sql_query)
        
        # Фиксируем изменения
        cursor.connection.commit()
        
        print(f"Таблица '{table_name}' успешно создана.")
        return True
    except Exception as e:
        # Откатываем изменения в случае ошибки
        cursor.connection.rollback()
        print(f"Ошибка при создании таблицы '{table_name}': {e}")
        return False

def create_tables(cursor):
    try:
        tables = get_all_tables()
        for table_name in tables:
            if not create_table(cursor, table_name):
                return False
        return True
    except Exception as e:
        # Откатываем изменения в случае ошибки
        cursor.connection.rollback()
        print(f"Ошибка создания таблиц: {e}")
        return False 