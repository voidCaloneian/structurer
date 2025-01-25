import os
from typing import Tuple, Dict, Set

import ijson
from tqdm import tqdm


class ProgressFileWrapper:
    """
    Обёртка для файла, которая обновляет прогресс-бар по мере чтения данных.
    """
    def __init__(self, file_obj, progress_bar):
        self.file_obj = file_obj
        self.progress_bar = progress_bar

    def read(self, size=-1):
        data = self.file_obj.read(size)
        self.progress_bar.update(len(data.encode('utf-8')))
        return data

    def readline(self, size=-1):
        data = self.file_obj.readline(size)
        self.progress_bar.update(len(data.encode('utf-8')))
        return data

    def __iter__(self):
        for line in self.file_obj:
            self.progress_bar.update(len(line.encode('utf-8')))
            yield line

    def __next__(self):
        data = next(self.file_obj)
        self.progress_bar.update(len(data.encode('utf-8')))
        return data

    def __getattr__(self, attr):
        return getattr(self.file_obj, attr)


def process_file(file_path: str) -> Tuple[Dict[str, int], Dict[str, float]]:
    """
    Обрабатывает файл и возвращает словари с количеством предметов
    и общей суммой продаж по категориям.

    :param file_path: путь к файлу JSON
    :return: кортеж из двух словарей (category_counts, category_sales)
    """
    # Инициализируем словари для хранения количества и суммы продаж по категориям
    category_counts: Dict[str, int] = {}
    category_sales: Dict[str, float] = {}

    # Множество для отслеживания уникальных идентификаторов предметов
    unique_ids: Set[str] = set()

    # Получаем общий размер файла для отображения прогресса
    try:
        file_size = os.path.getsize(file_path)
    except OSError as e:
        print(f"Ошибка при получении размера файла: {e}")
        return category_counts, category_sales

    # Открываем файл для чтения
    with open(file_path, 'r', encoding='utf-8') as f:
        # Инициализируем прогресс-бар
        with tqdm(total=file_size, unit='B', unit_scale=True, unit_divisor=1024, desc='Обработка файла') as pbar:
            # Оборачиваем файловый объект для обновления прогресс-бара при чтении
            wrapped_file = ProgressFileWrapper(f, pbar)
            try:
                # Используем ijson для последовательного чтения файла
                # Предполагаем, что в файле содержится массив объектов JSON
                objects = ijson.items(wrapped_file, 'item')
                for item in objects:
                    item_id = item.get('id')
                    if not item_id:
                        # Пропускаем элементы без идентификатора
                        continue

                    if item_id in unique_ids:
                        # Пропускаем дубликат
                        continue
                    else:
                        unique_ids.add(item_id)

                    category = item.get('category', 'Неизвестно')
                    price = item.get('price', 0.0)

                    # Увеличиваем количество предметов в категории
                    category_counts[category] = category_counts.get(category, 0) + 1

                    # Увеличиваем общую сумму продаж в категории
                    category_sales[category] = category_sales.get(category, 0.0) + price
            except Exception as e:
                print(f"\nОшибка при обработке файла: {e}")

    return category_counts, category_sales


def main():
    """
    Основная функция программы.
    """
    file_path = 'f.json'
    category_counts, category_sales = process_file(file_path)

    # Выводим результаты
    print("\nКоличество предметов по категориям:")
    for category, count in category_counts.items():
        print(f"{category}: {count}")

    print("\nОбщая сумма продаж по категориям:")
    for category, total in category_sales.items():
        print(f"{category}: {total:.2f}")


if __name__ == '__main__':
    main()
