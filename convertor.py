#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Файл с функциями для конвертации данных.
Содержит функции для чтения данных из таблиц базы данных.
"""

from db_connection import source_db_connection, target_db_connection
from dictionaries import dictionaries, update_buildings_dictionary, update_localities_dictionary, update_streets_dictionary, additional_dictionaries
from insert_data import insert_buildings_data, insert_localities_data, insert_streets_data
# from utils import display_table_data
from create_tables import create_tables
from drop_tables import drop_tables

def convertor():
    # Получаем соединение
    source_connection = source_db_connection()
    target_connection = target_db_connection()

    try:
        # Создаем курсор
        source_cursor = source_connection.cursor()
        target_cursor = target_connection.cursor()

        # Удаляем существующие таблицы
        print("\nУдаление существующих таблиц...")
        drop_tables(target_cursor)    

        # Создаем таблицы
        print("\nСоздание таблиц...")
        create_tables(target_cursor)

        dictionary_locality, dictionary_street, dictionary_building = dictionaries(source_cursor)
        dictionary_house_types, dictionary_add_house_types = additional_dictionaries(source_cursor)

        dictionary_locality = update_localities_dictionary(source_cursor, dictionary_locality)
        insert_localities_data(target_cursor, dictionary_locality)

        dictionary_street = update_streets_dictionary(source_cursor, dictionary_street)
        insert_streets_data(target_cursor, dictionary_street)

        dictionary_building = update_buildings_dictionary(source_cursor, dictionary_building, dictionary_house_types, dictionary_add_house_types)
        insert_buildings_data(target_cursor, dictionary_building)
        
        # display_table_data(column_names, rows)           
    except Exception as e:
        print(f"Ошибка при выполнении конвертации: {e}")
    finally:
        # Закрываем соединения
        if 'source_cursor' in locals():
            source_cursor.close()
        if 'target_cursor' in locals():
            target_cursor.close()
        source_connection.close()
        target_connection.close()