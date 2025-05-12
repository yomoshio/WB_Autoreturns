from playwright.sync_api import sync_playwright
import os
import time



def download_file(browser, downloads_dir, stop_file, li_selector, file_description, custom_filename):
    """
    Функция для скачивания файла с повторной попыткой при неудаче.
    :param browser: Объект браузера Playwright.
    :param downloads_dir: Директория для сохранения файлов.
    :param li_selector: Селектор для элемента <li>, который нужно нажать.
    :param file_description: Описание файла (для логов).
    :param custom_filename: Кастомное имя файла.
    """
    download_stop = "/root/telegram/YG_Telegram_Stop_List/app/skus"
    for attempt in range(3):  # Максимум 3 попытки
        try:
            page = browser.new_page()
            page.goto("https://seller.wildberries.ru/discount-and-prices", wait_until="domcontentloaded")
            page.wait_for_timeout(5000)

            print(f"Title: {page.title()}, URL: {page.url}")

            btn_all = page.query_selector("button[data-testid='xlsx-action-open-test-id-button-interface']")
            if btn_all:
                btn_all.click()
                print(f"Кнопка 'Все' нажата для {file_description}")
            else:
                print("Кнопка 'Все' не найдена")
                return

            page.wait_for_timeout(1000)

            li_element = page.query_selector(li_selector)
            if li_element:
                li_element.click()
                print(f"Кнопка '{file_description}' нажата")
            else:
                print(f"Кнопка '{file_description}' не найдена")
                return

            page.wait_for_timeout(1000)

            create_xlxs = page.query_selector("button[data-testid='xlsx-action-create-test-id-button-interface']")
            if create_xlxs:
                create_xlxs.click()
                print("Кнопка 'Создать XLSX' нажата")
            else:
                print("Кнопка 'Создать XLSX' не найдена")
                return

            page.wait_for_timeout(50000)

            download_xlsx = page.query_selector("button[data-testid='xlsx-action-download-test-id-button-interface']")
            if download_xlsx:
                with page.expect_download(timeout=120000) as download_info:
                    download_xlsx.click()

                download = download_info.value
                download.save_as(os.path.join(downloads_dir, custom_filename))
                if file_description == "Обновить цены":
                    download.save_as(os.path.join(download_stop, stop_file))
                print(f"Файл {custom_filename} успешно скачан в {downloads_dir}")
                return  # Успешно скачали, выходим из цикла

            else:
                print("Кнопка 'Скачать XLSX' не найдена")

        except Exception as e:
            print(f"Попытка {attempt + 1}: Ошибка при скачивании {file_description}: {e}")
            time.sleep(5)  # Ждем перед повторной попыткой
        
        finally:
            page.close()

    print(f"Файл {file_description} не удалось скачать после 3 попыток.")



def main(cab_pref):
    # Пути для сохранения данных
    
    if cab_pref == "T":
        user_data_dir = "SESSION_PATH"
    else: 
        user_data_dir = "SESSION_PATH"

    downloads_dir = f"/root/scripts/WB_Autoreturns/{cab_pref}files"  # Директория для скачивания файлов
    stop_file = f"{cab_pref}_delete.xlsx"
    # Создаем директорию для скачивания, если она не существует
    os.makedirs(downloads_dir, exist_ok=True)

    with sync_playwright() as p:
        # Запуск браузера с user_data_dir и указанием директории для скачивания
        browser = p.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            headless=True,  # Уберите headless=False, если не нужно видеть браузер
            args=["--no-sandbox", "--disable-dev-shm-usage", "--lang=en-US"],
            downloads_path=downloads_dir  # Указываем директорию для скачивания
        )

        try:
            # Скачиваем файл "Обновить цены"
            download_file(
                browser,
                downloads_dir,
                stop_file,
                "li[data-testid='xlsx-action-option-excel-update-test-id']",
                "Обновить цены",
                "update_prices.xlsx"
                
            )
            time.sleep(10)
            # Скачиваем файл "Обновить скидки WB Клуба"
            download_file(
                browser,
                downloads_dir,
                stop_file,
                "li[data-testid='xlsx-action-option-club-discount-update-test-id']",
                "Обновить скидки для WB Клуба",
                "update_club.xlsx"
            )
            time.sleep(10)

        finally:
            browser.close()

if __name__ == "__main__":
    main("T")
    main("B")