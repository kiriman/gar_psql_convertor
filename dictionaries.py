#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from tqdm import tqdm

query_dict = {
    'MUN_HIERARCHY': 'SELECT "PATH" FROM "MUN_HIERARCHY" WHERE "PATH" LIKE \'{base_path}.%\' AND "ISACTIVE" = 1;'
}

def dictionaries(cursor):
    base_path = os.getenv('BASE_PATH')
    cursor.execute(query_dict['MUN_HIERARCHY'].format(base_path=base_path))
    rows = cursor.fetchall()

    try:
        dictionary_locality = {}
        dictionary_street = {}
        dictionary_building = {}

        for [path] in tqdm(rows, ncols=120, desc='dictionaries (MUN_HIERARCHY)'):
            path_parts = path.split('.')
            locality_id = path_parts[2] if len(path_parts) > 2 else None
            street_id = path_parts[3] if len(path_parts) > 3 else None
            building_id = path_parts[4] if len(path_parts) > 4 else None

            if locality_id:
                dictionary_locality.setdefault(locality_id, { 'id': locality_id })
            if street_id:
                dictionary_street.setdefault(street_id, { 'locality_id': locality_id, 'id': street_id })
            if building_id:
                dictionary_building.setdefault(building_id, { 'street_id': street_id, 'id': building_id })

        print(f"")

        return dictionary_locality, dictionary_street, dictionary_building
    except Exception as e:
        print(f"Ошибка при формировании основных словарей: {e}")
        return False
    
def additional_dictionaries(cursor):

    query = f'SELECT "ID", "SHORTNAME" FROM "HOUSE_TYPES";'
    cursor.execute(query)
    house_types_rows = cursor.fetchall()

    query = f'SELECT "ID", "SHORTNAME" FROM "ADDHOUSE_TYPES";'
    cursor.execute(query)
    add_house_types_rows = cursor.fetchall()

    try:
        dictionary_house_types = {}
        dictionary_add_house_types = {}

        for [id, shortname] in tqdm(house_types_rows, ncols=120, desc='additional_dictionaries (HOUSE_TYPES)'):
            if id:
                dictionary_house_types.setdefault(id, { 'shortname': shortname })

        for [id, shortname] in tqdm(add_house_types_rows, ncols=120, desc='additional_dictionaries (ADDRHOUSE_TYPES)'):
            if id:
                dictionary_add_house_types.setdefault(id, { 'shortname': shortname })

        print(f"")

        return dictionary_house_types, dictionary_add_house_types
    except Exception as e:
        print(f"Ошибка при формировании основных словарей: {e}")
        return False
    
def update_localities_dictionary(source_cursor, dictionary_locality):
    try:
        for key, value in tqdm(dictionary_locality.items(), ncols=120, desc='update_localities_dictionary'):
            query = f'SELECT "NAME", "TYPENAME" FROM "ADDR_OBJ" WHERE "OBJECTID" = {key};'
            source_cursor.execute(query)
            # column_names = [desc[0] for desc in source_cursor.description]
            rows = source_cursor.fetchall()
            dictionary_locality[key]['name'] = rows[0][0]
            dictionary_locality[key]['type'] = rows[0][1]

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
                dictionary_street[key]['type'] = rows[0][1]
            else:
                keys_to_delete.append(key)

        print(f"keys_to_delete (dictionary_street): {len(keys_to_delete)}")
        for key in keys_to_delete:
            del dictionary_street[key]

        return dictionary_street
    except Exception as e:
        print(f"Ошибка при обновлении словаря: {e} {dictionary_street}")
        return False

def update_buildings_dictionary(source_cursor, dictionary_building, dictionary_house_types, dictionary_add_house_types):
    try:
        keys_to_delete = []
        normalized_houses = {}

        # ---------------------------------------------------------------------------------------------------------v.2 (faster)
        query = '''
            SELECT "OBJECTID", "HOUSENUM", "HOUSETYPE", "ADDNUM1", "ADDNUM2", "ADDTYPE1", "ADDTYPE2"
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
                    'number': row[1] if row[1] is not None else '',
                    'type': row[2] if row[2] is not None else '',
                    'add_num1': row[3] if row[3] is not None else '',
                    'add_num2': row[4] if row[4] is not None else '',
                    'add_type1': row[5] if row[5] is not None else '',
                    'add_type2': row[6] if row[6] is not None else '',
                })

        for key, value in tqdm(dictionary_building.items(), ncols=120, desc='update_buildings_dictionary v.2'):
            key_int = int(key)
            if key_int in normalized_houses:
                dictionary_building[key]['number'] = normalized_houses[key_int]['number']
                
                key_type = normalized_houses[key_int]['type']
                dictionary_building[key]['type'] = dictionary_house_types[key_type]['shortname']

                key_add_type1 = normalized_houses[key_int]['add_type1']
                dictionary_building[key]['add_type1'] = dictionary_add_house_types[key_add_type1]['shortname'] if key_add_type1 else None

                key_add_type2 = normalized_houses[key_int]['add_type2']
                dictionary_building[key]['add_type2'] = dictionary_add_house_types[key_add_type2]['shortname'] if key_add_type2 else None

                dictionary_building[key]['add_num1'] = normalized_houses[key_int]['add_num1']
                dictionary_building[key]['add_num2'] = normalized_houses[key_int]['add_num2']
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
        #         dictionary_building[key]['number'] = rows[0][0]
        #         dictionary_building[key]['type'] = rows[0][1]
        #         dictionary_building[key]['add_num1'] = rows[0][2]
        #         dictionary_building[key]['add_num2'] = rows[0][3]
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


