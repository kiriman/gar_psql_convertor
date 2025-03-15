#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Модуль с определениями структур таблиц базы данных.
"""

# Словарь с SQL-запросами для создания таблиц
TABLE_DEFINITIONS = {    
    'localities': """
        CREATE TABLE localities (
            id INTEGER NOT NULL PRIMARY KEY,
            name VARCHAR(256) NOT NULL,
            typename VARCHAR(50) NOT NULL
        );
    """,
    
    'streets': """
        CREATE TABLE streets (
            id INTEGER NOT NULL PRIMARY KEY,
            locality_id INTEGER NOT NULL,
            name VARCHAR(256) NOT NULL,
            typename VARCHAR(50)
        );
    """,
    
    'buildings': """
        CREATE TABLE buildings (
            id INTEGER NOT NULL PRIMARY KEY,
            street_id INTEGER NOT NULL,
            number VARCHAR(50) NOT NULL,
            add_num1 VARCHAR(50),
            add_num2 VARCHAR(50),
            type VARCHAR(50),
            add_type1 VARCHAR(50),
            add_type2 VARCHAR(50)
        );
    """
}

# Список всех таблиц в порядке их создания
ALL_TABLES = ['localities', 'streets', 'buildings']

def get_table_definition(table_name):
    return TABLE_DEFINITIONS.get(table_name)

def get_all_tables():
    return ALL_TABLES 

# def get_all_tables(cursor):
#     """
#     Получает список всех таблиц в базе данных.
    
#     Args:
#         cursor: Курсор базы данных
        
#     Returns:
#         list: Список кортежей (schema, table)
#     """
#     try:
#         cursor.execute("""
#             SELECT table_schema, table_name 
#             FROM information_schema.tables 
#             WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
#             ORDER BY table_schema, table_name;
#         """)
#         return cursor.fetchall()
#     except Exception as e:
#         print(f"Ошибка при получении списка таблиц: {e}")
#         return []