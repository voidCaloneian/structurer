from src.structurer import process_file


data_file_path = 'src/data.json'

def main():
    """
    Основная функция программы.
    """
    category_counts, category_sales = process_file(data_file_path)

    # Выводим результаты
    print("\nКоличество предметов по категориям:")
    for category, count in category_counts.items():
        print(f"{category}: {count}")

    print("\nОбщая сумма продаж по категориям:")
    for category, total in category_sales.items():
        print(f"{category}: {total:.2f}")

if __name__ == '__main__':
    main()