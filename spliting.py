import os
import pandas as pd

def split_large_csv_files(input_folder, output_folder, chunk_size=100000):
    """
    Разделяет большие CSV-файлы на части по chunk_size строк и сохраняет их в формате .xlsx.
    :param input_folder: Папка с исходными файлами.
    :param output_folder: Папка для сохранения разделённых файлов.
    :param chunk_size: Количество строк в каждом файле (по умолчанию 100 000).
    """
    # Проверяем, существует ли выходная папка, и создаем ее, если нет
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Перебираем все файлы в входной папке
    for filename in os.listdir(input_folder):
        if filename.endswith('.csv'):
            file_path = os.path.join(input_folder, filename)
            
            # Создаем папку для текущего файла
            file_output_folder = os.path.join(output_folder, os.path.splitext(filename)[0])
            if not os.path.exists(file_output_folder):
                os.makedirs(file_output_folder)

            # Читаем CSV файл
            df = pd.read_csv(file_path)
            
            # Разделяем DataFrame на части по chunk_size строк
            for i, start in enumerate(range(0, len(df), chunk_size)):
                end = start + chunk_size
                chunk_df = df.iloc[start:end]
                
                # Формируем новое имя файла
                new_filename = f"{os.path.splitext(filename)[0]}_part_{i+1}.xlsx"
                new_file_path = os.path.join(file_output_folder, new_filename)
                
                # Сохраняем часть DataFrame в новый файл .xlsx
                chunk_df.to_excel(new_file_path, index=False)
                print(f"Создан файл: {new_file_path}")

# Пример использования функции
def main(cab_pref):
    input_folder = f'/root/scripts/WB_Autoreturns/{cab_pref}processed_files'
    output_folder = f'/root/scripts/WB_Autoreturns/{cab_pref}processed_files_splited'
    split_large_csv_files(input_folder, output_folder)