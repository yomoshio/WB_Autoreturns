from playwright.sync_api import sync_playwright
import os
import time



def download_file(browser):
    """
    Функция для скачивания файла с повторной попыткой при неудаче.
    :param browser: Объект браузера Playwright.
    :param downloads_dir: Директория для сохранения файлов.
    :param li_selector: Селектор для элемента <li>, который нужно нажать.
    :param file_description: Описание файла (для логов).
    :param custom_filename: Кастомное имя файла.
    """

    for attempt in range(3):  # Максимум 3 попытки
        try:
            page = browser.new_page()
            page.goto("https://seller.wildberries.ru/dynamic-product-categories/commission", wait_until="domcontentloaded")
            page.wait_for_timeout(5000)

            print(f"Title: {page.title()}, URL: {page.url}")

            test = '/root/scripts/WB_Autoreturns'
            path = 'comission.xlsx'
            download_xlsx = page.query_selector("button[class='Button-link__r5zr-XkUd9 Button-link--button__Blqgm0LxDF Button-link--interface__y3oLQL4dOh Button-link--button-big__alN6h4wIz2 Button-link--icon-text-big__w-pY-mrmSu']")
            if download_xlsx:
                with page.expect_download(timeout=120000) as download_info:
                    download_xlsx.click()

                download = download_info.value
                download.save_as(os.path.join(test, path))
                print(f"Файл {path} успешно скачан в {test}")
                return  # Успешно скачали, выходим из цикла

            else:
                print("Кнопка 'Скачать XLSX' не найдена")

        except Exception as e:
            print(f"Попытка {attempt + 1}: Ошибка при скачивании файла: {e}")
            time.sleep(5)  # Ждем перед повторной попыткой
        
        finally:
            page.close()

    print(f"Файл не удалось скачать после 3 попыток.")



def main():
    user_data_dir = "/root/scripts/WB_Autoreturns/bonateca_files/session_linux"


    with sync_playwright() as p:

        browser = p.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            headless=True,  
            args=["--no-sandbox", "--disable-dev-shm-usage"] 
        )

        try:
            # Скачиваем файл "Обновить цены"
            download_file(
                browser,               
            )
        finally:
            # Закрываем браузер (данные автоматически сохраняются в user_data_dir)
            browser.close()

if __name__ == "__main__":
    main()