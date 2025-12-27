#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Скрипт для поиска кода городского округа по названию
"""

import sys
from db_connection import source_db_connection

def find_city_code(search_name):
    """
    Ищет городской округ по названию и возвращает его код
    """
    try:
        conn = source_db_connection()
        cursor = conn.cursor()
        
        # Ищем городской округ (LEVEL=3) по названию
        query = """
            SELECT a."OBJECTID", a."OBJECTGUID", a."NAME", a."TYPENAME", a."LEVEL",
                   h."PATH", h."PARENTOBJID"
            FROM "ADDR_OBJ" a
            JOIN "MUN_HIERARCHY" h ON a."OBJECTID" = h."OBJECTID"
            WHERE a."NAME" ILIKE %s
            AND a."ISACTUAL" = 1 
            AND a."ISACTIVE" = 1
            AND h."ISACTIVE" = 1
            ORDER BY a."LEVEL", a."NAME";
        """
        
        cursor.execute(query, (f'%{search_name}%',))
        results = cursor.fetchall()
        
        if not results:
            print(f"Населенный пункт '{search_name}' не найден.")
            cursor.close()
            conn.close()
            return
        
        print("=" * 100)
        print(f"Найдены объекты со словом '{search_name}':")
        print("=" * 100)
        
        for row in results:
            objectid = row[0]
            objectguid = row[1]
            name = row[2]
            typename = row[3]
            level = row[4]
            path = row[5]
            parentobjid = row[6]
            
            print(f"\nOBJECTID: {objectid}")
            print(f"GUID: {objectguid}")
            print(f"Название: {typename} {name}")
            print(f"LEVEL: {level}")
            print(f"PATH: {path}")
            print(f"PARENT: {parentobjid}")
            
            # Разбираем путь
            path_parts = path.split('.')
            if len(path_parts) >= 2:
                base_path = '.'.join(path_parts[:2])
                print(f"BASE_PATH (2 октета): {base_path}")
            
            print("-" * 100)
        
        # Ищем конкретно городские округа (LEVEL=3)
        print("\n" + "=" * 100)
        print("Городские округа (LEVEL=3):")
        print("=" * 100)
        
        found_go = False
        for row in results:
            level = row[4]
            if level == 3:  # Уровень городского округа
                found_go = True
                objectid = row[0]
                objectguid = row[1]
                name = row[2]
                typename = row[3]
                path = row[5]
                
                path_parts = path.split('.')
                base_path = '.'.join(path_parts[:2]) if len(path_parts) >= 2 else path
                
                print(f"\n✓ Найден городской округ:")
                print(f"  OBJECTID: {objectid}")
                print(f"  GUID: {objectguid}")
                print(f"  Название: {typename} {name}")
                print(f"  PATH: {path}")
                print(f"\n  >>> BASE_PATH = \"{base_path}\"")
                print("=" * 100)
        
        if not found_go:
            print("\nГородские округа не найдены. Показаны все найденные объекты.")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Ошибка при поиске: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Можно передать название как аргумент командной строки
    if len(sys.argv) > 1:
        search_name = sys.argv[1]
    else:
        search_name = "Арсеньев"  # По умолчанию
    
    print(f"Поиск городского округа: {search_name}")
    print("=" * 100)
    find_city_code(search_name)

