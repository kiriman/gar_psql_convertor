#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Утилиты для работы с данными.
Содержит вспомогательные функции для отображения и обработки данных.
"""

def display_table_data(column_names, rows):
    """
    Выводит данные таблицы в консоль в форматированном виде
    
    Args:
        column_names (list): Список имен колонок
        rows (list): Список строк таблицы
    """
    if not column_names or not rows:
        print("Нет данных для отображения")
        return
    
    # Определяем максимальную ширину для каждой колонки
    col_widths = []
    for i, name in enumerate(column_names):
        # Максимальная ширина - это максимум из ширины заголовка и ширины данных
        max_width = max(
            len(str(name)),
            max(len(str(row[i])) for row in rows)
        )
        col_widths.append(max_width + 2)  # Добавляем отступ
    
    # Создаем разделительную линию
    separator = "+" + "+".join("-" * width for width in col_widths) + "+"
    
    # Выводим заголовок
    print("\n" + separator)
    header = "|"
    for i, name in enumerate(column_names):
        header += f" {name.ljust(col_widths[i] - 1)}|"
    print(header)
    print(separator)
    
    # Выводим данные
    for row in rows:
        row_str = "|"
        for i, cell in enumerate(row):
            row_str += f" {str(cell).ljust(col_widths[i] - 1)}|"
        print(row_str)
    
    print(separator)
    print(f"Всего записей: {len(rows)}") 