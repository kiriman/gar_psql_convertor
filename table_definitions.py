#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Модуль с определениями структур таблиц базы данных.
"""

# Словарь с SQL-запросами для создания таблиц
TABLE_DEFINITIONS = {    
    'localities': """
        CREATE TABLE localities (
            id SERIAL PRIMARY KEY,
            objectid INTEGER NOT NULL,
            name VARCHAR(256) NOT NULL,
            typename VARCHAR(50) NOT NULL
        );
    """,
    
    'streets': """
        CREATE TABLE streets (
            id SERIAL PRIMARY KEY,
            locality_id INTEGER NOT NULL,
            objectid INTEGER NOT NULL,
            name VARCHAR(256) NOT NULL,
            typename VARCHAR(50)
        );
    """,
    
    'buildings': """
        CREATE TABLE buildings (
            id SERIAL PRIMARY KEY,
            street_id INTEGER NOT NULL,
            objectid INTEGER NOT NULL,
            housenum VARCHAR(50) NOT NULL,
            addnum1 VARCHAR(50),
            addnum2 VARCHAR(50),
            housetype VARCHAR(50),
            addtype1 VARCHAR(50),
            addtype2 VARCHAR(50)
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