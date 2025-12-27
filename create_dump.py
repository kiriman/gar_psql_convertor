#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Скрипт для создания дампа целевой базы данных
"""

import os
import sys
import io
import subprocess
from datetime import datetime
from dotenv import load_dotenv

# Устанавливаем кодировку для вывода в консоль Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

load_dotenv(encoding='utf-8')

def create_database_dump():
    """
    Создает дамп целевой базы данных
    """
    try:
        # Получаем параметры из .env
        db_host = os.getenv('TARGET_DB_HOST', 'localhost').strip()
        db_port = os.getenv('TARGET_DB_PORT', '5432').strip()
        db_name = os.getenv('TARGET_DB_NAME', '').strip()
        db_user = os.getenv('TARGET_DB_USER', '').strip()
        db_password = os.getenv('TARGET_DB_PASSWORD', '').strip()
        
        if not db_name:
            print("Ошибка: TARGET_DB_NAME не указан в файле .env")
            return False
        
        # Извлекаем суффикс из имени базы данных
        # Например: gar_simple_db_arsenev -> arsenev
        #           gar_simple_db_spassk -> spassk
        db_parts = db_name.split('_')
        suffix = db_parts[-1] if len(db_parts) > 1 else db_name
        
        # Создаем имя файла дампа в формате: psql_gar_simple_db_dump_{suffix}.sql
        dump_filename = f"psql_gar_simple_db_dump_{suffix}.sql"
        
        # Получаем директорию для дампов (по умолчанию текущая)
        dump_dir = os.getenv('DUMP_DIR', '').strip()
        if dump_dir and os.path.exists(dump_dir):
            dump_filepath = os.path.join(dump_dir, dump_filename)
        else:
            dump_filepath = dump_filename
            if dump_dir and not os.path.exists(dump_dir):
                print(f"Предупреждение: Директория {dump_dir} не существует, сохраняем в текущую директорию")
                print()
        
        print("=" * 80)
        print("Создание дампа базы данных (только данные)")
        print("=" * 80)
        print(f"База данных: {db_name}")
        print(f"Хост: {db_host}:{db_port}")
        print(f"Пользователь: {db_user}")
        print(f"Файл дампа: {dump_filepath}")
        print("=" * 80)
        print()
        
        # Формируем команду pg_dump (только данные, без структуры)
        pg_dump_cmd = [
            'pg_dump',
            '-h', db_host,
            '-p', db_port,
            '-U', db_user,
            '-d', db_name,
            '-f', dump_filepath,
            '--data-only',  # Только данные
            '--inserts',  # Использовать INSERT вместо COPY
            '--verbose',
            '--format=plain',
            '--encoding=UTF8'
        ]
        
        # Устанавливаем переменную окружения для пароля
        env = os.environ.copy()
        env['PGPASSWORD'] = db_password
        
        # Выполняем команду
        print("Выполняется команда pg_dump...")
        print()
        
        result = subprocess.run(
            pg_dump_cmd,
            env=env,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace'  # Заменяем неправильные символы
        )
        
        if result.returncode == 0:
            # Получаем размер файла
            file_size = os.path.getsize(dump_filepath)
            file_size_mb = file_size / (1024 * 1024)
            
            print()
            print("=" * 80)
            print("✓ Дамп успешно создан!")
            print("=" * 80)
            print(f"Файл: {dump_filepath}")
            print(f"Размер: {file_size_mb:.2f} МБ ({file_size:,} байт)")
            print("=" * 80)
            return True
        else:
            print()
            print("=" * 80)
            print("✗ Ошибка при создании дампа!")
            print("=" * 80)
            print("STDERR:")
            print(result.stderr)
            print("=" * 80)
            return False
            
    except FileNotFoundError:
        print()
        print("=" * 80)
        print("✗ Ошибка: команда pg_dump не найдена!")
        print("=" * 80)
        print("Убедитесь, что PostgreSQL установлен и pg_dump доступен в PATH")
        print("Или добавьте путь к PostgreSQL в переменную PATH:")
        print("Например: C:\\Program Files\\PostgreSQL\\17\\bin")
        print("=" * 80)
        return False
    except Exception as e:
        print()
        print("=" * 80)
        print(f"✗ Ошибка: {e}")
        print("=" * 80)
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = create_database_dump()
    sys.exit(0 if success else 1)

