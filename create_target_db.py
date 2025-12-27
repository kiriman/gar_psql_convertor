#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Скрипт для создания целевой базы данных
"""

import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv

load_dotenv(encoding='utf-8')

def create_target_database():
    """
    Создает целевую базу данных, если она не существует
    """
    try:
        # Подключаемся к postgres (системной БД)
        conn = psycopg2.connect(
            host=os.getenv('TARGET_DB_HOST', 'localhost').strip(),
            port=os.getenv('TARGET_DB_PORT', '5432').strip(),
            database='postgres',  # Подключаемся к системной БД
            user=os.getenv('TARGET_DB_USER', '').strip(),
            password=os.getenv('TARGET_DB_PASSWORD', '').strip()
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        target_db_name = os.getenv('TARGET_DB_NAME', '').strip()
        
        # Проверяем, существует ли база данных
        cursor.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (target_db_name,)
        )
        exists = cursor.fetchone()
        
        if exists:
            print(f"База данных '{target_db_name}' уже существует.")
        else:
            # Создаем базу данных с явным указанием кодировки
            cursor.execute(f'''
                CREATE DATABASE "{target_db_name}" 
                WITH ENCODING='UTF8' 
                LC_COLLATE='C' 
                LC_CTYPE='C' 
                TEMPLATE=template0;
            ''')
            print(f"База данных '{target_db_name}' успешно создана.")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Ошибка при создании базы данных: {e}")
        return False

if __name__ == "__main__":
    create_target_database()

