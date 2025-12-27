#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Скрипт для вывода всех городских округов в Приморском крае
"""

import sys
import io

# Устанавливаем кодировку для вывода в консоль Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

from db_connection import source_db_connection

def list_all_go():
    """
    Выводит все городские округа (LEVEL=3) в Приморском крае (381755)
    """
    try:
        conn = source_db_connection()
        cursor = conn.cursor()
        
        # Ищем все объекты с PATH начинающимся на 381755 и LEVEL=3
        query = """
            SELECT a."OBJECTID", a."NAME", a."TYPENAME", h."PATH"
            FROM "ADDR_OBJ" a
            JOIN "MUN_HIERARCHY" h ON a."OBJECTID" = h."OBJECTID"
            WHERE h."PATH" LIKE '381755.%'
            AND a."LEVEL" = '3'
            AND a."ISACTUAL" = 1 
            AND a."ISACTIVE" = 1
            AND h."ISACTIVE" = 1
            ORDER BY a."NAME";
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        print("=" * 100)
        print(f"Все городские округа в Приморском крае (LEVEL=3):")
        print("=" * 100)
        
        for row in results:
            objectid = row[0]
            name = row[1]
            typename = row[2]
            path = row[3]
            
            path_parts = path.split('.')
            base_path = '.'.join(path_parts[:2]) if len(path_parts) >= 2 else path
            
            print(f"{typename} {name:<40} | OBJECTID: {objectid:<10} | BASE_PATH: {base_path}")
        
        print("=" * 100)
        print(f"Всего найдено: {len(results)} городских округов")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    list_all_go()

