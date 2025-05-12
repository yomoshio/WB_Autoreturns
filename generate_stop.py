import csv
import os

def generate_sku_file(input_file_path, output_file_path):
    """
    Генерирует файл с двумя столбцами: WB item No. и Seller item No.
    :param input_file_path: Путь к исходному файлу.
    :param output_file_path: Путь для сохранения нового файла.
    """
    try:
        # Проверяем, существует ли исходный файл
        if not os.path.exists(input_file_path):
            print(f"Файл {input_file_path} не найден.")
            return

        # Открываем исходный файл для чтения
        with open(input_file_path, mode='r', newline='', encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile, delimiter=',')  # Используем запятую как разделитель
            rows = list(reader)

            # Проверяем, есть ли нужные столбцы
            fieldnames = [field.strip() for field in reader.fieldnames]  # Убираем лишние пробелы
            if 'WB item No.' not in fieldnames or 'Seller item No.' not in fieldnames:
                print("В файле отсутствуют необходимые столбцы: 'WB item No.' или 'Seller item No.'.")
                print(f"Найденные столбцы: {fieldnames}")
                return

            # Создаем новый файл для записи
            with open(output_file_path, mode='w', newline='', encoding='utf-8') as outfile:
                writer = csv.writer(outfile, delimiter=',')  # Используем запятую как разделитель
                # Записываем заголовки
                writer.writerow(['WB item No.', 'Seller item No.'])
                # Записываем данные
                for row in rows:
                    wb_item = row['WB item No.'].strip()
                    seller_item = row['Seller item No.'].strip()
                    writer.writerow([wb_item, seller_item])

        print(f"Файл успешно создан: {output_file_path}")

    except Exception as e:
        print(f"Ошибка при обработке файла: {e}")

def main(cab_pref):
    # Пути к файлам
    input_file_path = f"/root/scripts/WB_Autoreturns/{cab_pref}csv_files/update_prices.csv"
    output_file_path = f"/root/telegram/YG_Telegram_Stop_List/app/skus/{cab_pref}_output.csv"  # Имя файла можно изменить

    # Убедимся, что директория для выходного файла существует
    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

    # Генерируем файл
    generate_sku_file(input_file_path, output_file_path)

if __name__ == "__main__":
    main("T")
    #main("B")