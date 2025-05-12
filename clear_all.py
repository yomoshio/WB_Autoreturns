import os
import shutil



def clean_folder(folder_path):
    # Проверяем, существует ли папка
    if os.path.exists(folder_path):
        # Перебираем все файлы и папки внутри указанной директории
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            try:
                # Если это файл, удаляем его
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                # Если это папка, удаляем её и всё её содержимое
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f"Ошибка при удалении {file_path}. Причина: {e}")
    else:
        print(f"Папка {folder_path} не существует.")




def main(cab_pref):
    # Список папок, которые нужно очистить
    folders_to_clean = [
        f"/root/scripts/WB_Autoreturns/{cab_pref}csv_files",
        f"/root/scripts/WB_Autoreturns/{cab_pref}files",
        f"/root/scripts/WB_Autoreturns/{cab_pref}processed_files",
        f"/root/scripts/WB_Autoreturns/{cab_pref}processed_files_splited"
    ]
    # Очищаем каждую папку из списка
    for folder in folders_to_clean:
        clean_folder(folder)

    print("Очистка завершена.")

if __name__ == "__main__":
    main()