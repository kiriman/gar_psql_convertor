#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tqdm import tqdm

def insert_localities_data(cursor, dictionary_locality):
    try:
        for key, value in tqdm(dictionary_locality.items(), ncols=120, desc='insert_localities_data'):
            cursor.execute('INSERT INTO "localities" (id, name, typename) VALUES (%s, %s, %s);', (key, value['name'], value['typename']))

        cursor.connection.commit()
        
        print(f"В таблицу 'localities' добавлено {len(dictionary_locality)} записей.\n")
        return True
    except Exception as e:
        cursor.connection.rollback()
        print(f"Ошибка при вставке тестовых данных в таблицу 'localities': {e}")
        return False
    
def insert_streets_data(cursor, dictionary_street):
    current = ''
    try:
        for key, value in tqdm(dictionary_street.items(), ncols=120, desc='insert_streets_data'):
            current = value
            cursor.execute('INSERT INTO "streets" (id, locality_id, name, typename) VALUES (%s, %s, %s, %s);', (key, value['locality_id'], value['name'], value['typename']))
        
        # Фиксируем изменения
        cursor.connection.commit()
        
        print(f"В таблицу 'streets' добавлено {len(dictionary_street)} записей.\n")
        return True
    except Exception as e:
        cursor.connection.rollback()
        print(f"Ошибка при вставке тестовых данных в таблицу 'streets': {e}, {current}")
        return False
    
def insert_buildings_data(cursor, dictionary_building):
    try:
        for key, value in tqdm(dictionary_building.items(), ncols=120, desc='insert_buildings_data'):
            cursor.execute('INSERT INTO "buildings" (id, street_id, number, type, add_num1, add_num2) VALUES (%s, %s, %s, %s, %s, %s);', (key, value['street_id'], value['number'], value['type'], value['add_num1'], value['add_num1']))
            
        cursor.connection.commit()

        print(f"В таблицу 'buildings' добавлено {len(dictionary_building)} записей.\n")
        return True
    except Exception as e:
        cursor.connection.rollback()
        print(f"Ошибка при вставке тестовых данных в таблицу 'buildings': {e} {dictionary_building}")
        return False
