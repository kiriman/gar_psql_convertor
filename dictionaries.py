#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from tqdm import tqdm

query_dict = {
    'MUN_HIERARCHY': 'SELECT "OBJECTID", "PATH" FROM "MUN_HIERARCHY" WHERE "PATH" LIKE \'{base_path}.%\' AND "ISACTIVE" = 1;'
}

def dictionaries(cursor):
    base_path = os.getenv('BASE_PATH')

    cursor.execute(query_dict['MUN_HIERARCHY'].format(base_path=base_path))

    rows = cursor.fetchall()
    print(f"Количество записей MUN_HIERARCHY: {len(rows)}")

    try:
        dictionary_locality = {}
        dictionary_street = {}
        dictionary_building = {}

        for objectid, path in tqdm(rows, ncols=120, desc='dictionaries'):
            path_parts = path.split('.')
            locality_id = path_parts[2] if len(path_parts) > 2 else None
            street_id = path_parts[3] if len(path_parts) > 3 else None
            building_id = path_parts[4] if len(path_parts) > 4 else None

            if locality_id:
                dictionary_locality.setdefault(locality_id, { 'objectid': locality_id })
            if street_id:
                dictionary_street.setdefault(street_id, { 'locality_id': locality_id, 'objectid': street_id })
            if building_id:
                dictionary_building.setdefault(building_id, { 'street_id': street_id, 'objectid': building_id })

        print(f"")

        return dictionary_locality, dictionary_street, dictionary_building
    except Exception as e:
        print(f"Ошибка при формировании словаря: {e}")
        return False
    
def update_localities_dictionary(source_cursor, dictionary_locality):
    try:
        for key, value in tqdm(dictionary_locality.items(), ncols=120, desc='update_localities_dictionary'):
            query = f'SELECT "NAME", "TYPENAME" FROM "ADDR_OBJ" WHERE "OBJECTID" = {key};'
            source_cursor.execute(query)
            # column_names = [desc[0] for desc in source_cursor.description]
            rows = source_cursor.fetchall()
            dictionary_locality[key]['name'] = rows[0][0]
            dictionary_locality[key]['typename'] = rows[0][1]

        return dictionary_locality
    
    except Exception as e:
        print(f"Ошибка при обновлении словаря: {e} {dictionary_locality}")
        return False

def update_streets_dictionary(source_cursor, dictionary_street):
    try:
        keys_to_delete = []

        for key, value in tqdm(dictionary_street.items(), ncols=120, desc='update_streets_dictionary'):
            query = f'SELECT "NAME", "TYPENAME" FROM "ADDR_OBJ" WHERE "OBJECTID" = {key} AND "ISACTUAL" = 1 AND "ISACTIVE" = 1;'
            source_cursor.execute(query)
            # column_names = [desc[0] for desc in source_cursor.description]
            rows = source_cursor.fetchall()

            if len(rows) > 0:
                dictionary_street[key]['name'] = rows[0][0]
                dictionary_street[key]['typename'] = rows[0][1]
            else:
                keys_to_delete.append(key)

        print(f"keys_to_delete (dictionary_street): {len(keys_to_delete)}")
        for key in keys_to_delete:
            del dictionary_street[key]

        return dictionary_street
    except Exception as e:
        print(f"Ошибка при обновлении словаря: {e} {dictionary_street}")
        return False

def update_buildings_dictionary(source_cursor, dictionary_building):
    try:
        keys_to_delete = []
        normalized_houses = {}

        # ---------------------------------------------------------------------------------------------------------v.2 (faster)
        query = '''
            SELECT "OBJECTID", "HOUSENUM", "HOUSETYPE", "ADDNUM1", "ADDNUM2"
            FROM "HOUSES"
            WHERE "ISACTUAL" = 1 AND "ISACTIVE" = 1;
        '''
        source_cursor.execute(query)
        rows = source_cursor.fetchall()

        if not rows:
            print("Нет данных, удовлетворяющих условиям запроса.")
        else:
            for row in tqdm(rows, ncols=120, desc='Нормализуем данные из таблицы "HOUSES"'):
                normalized_houses.setdefault(row[0], {
                    'housenum': row[1] if row[1] is not None else '',
                    'housetype': row[2] if row[2] is not None else '',
                    'addnum1': row[3] if row[3] is not None else '',
                    'addnum2': row[4] if row[4] is not None else '',
                })

        for key, value in tqdm(dictionary_building.items(), ncols=120, desc='update_buildings_dictionary v.2'):
            key_int = int(key)
            if key_int in normalized_houses:
                dictionary_building[key]['housenum'] = normalized_houses[key_int]['housenum']
                dictionary_building[key]['housetype'] = normalized_houses[key_int]['housetype']
                dictionary_building[key]['addnum1'] = normalized_houses[key_int]['addnum1']
                dictionary_building[key]['addnum2'] = normalized_houses[key_int]['addnum2']
            else:
                keys_to_delete.append(key)

        # ---------------------------------------------------------------------------------------------------------v.1
        # dictionary_building: 27049 -> 22196
        # for key, value in tqdm(dictionary_building.items(), ncols=120, desc='update_buildings_dictionary v.1'):
        #     # limit -= 1
        #     # if limit < 0:
        #     #     break
        #     query = f'SELECT "HOUSENUM", "HOUSETYPE", "ADDNUM1", "ADDNUM2" FROM "HOUSES" WHERE "OBJECTID" = {key} AND "ISACTUAL" = 1 AND "ISACTIVE" = 1 LIMIT 1;'
        #     source_cursor.execute(query)
        #     # column_names = [desc[0] for desc in source_cursor.description]
        #     rows = source_cursor.fetchall()

        #     if len(rows) > 0:
        #         dictionary_building[key]['housenum'] = rows[0][0]
        #         dictionary_building[key]['housetype'] = rows[0][1]
        #         dictionary_building[key]['addnum1'] = rows[0][2]
        #         dictionary_building[key]['addnum2'] = rows[0][3]
        #     else:
        #         keys_to_delete.append(key)
        # ---------------------------------------------------------------------------------------------------------

        print(f"keys_to_delete (dictionary_building): {len(keys_to_delete)}")
        for key in keys_to_delete:
            del dictionary_building[key]

        return dictionary_building
    except Exception as e:
        print(f"Ошибка при обновлении словаря: {e}")
        return False


