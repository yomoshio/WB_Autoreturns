from playwright.sync_api import sync_playwright
import os
import sys 


def upload_file(browser, file_path, li_selector, button_selector, file_description):
    """
    Функция для загрузки файла.
    :param browser: Объект браузера Playwright.
    :param file_path: Путь к файлу для загрузки.
    :param li_selector: Селектор для элемента <li>, который нужно нажать.
    :param button_selector: Селектор для кнопки подтверждения загрузки.
    :param file_description: Описание файла (для логов).
    """
    try:
        # Создаем новую страницу
        page = browser.new_page()

        # Переход на страницу
        page.goto("https://seller.wildberries.ru/discount-and-prices", wait_until="domcontentloaded")
        page.wait_for_timeout(5000)  # Ожидание 5 секунд

        # Выводим заголовок страницы
        print(f"Title: {page.title()}")

        # Выводим URL текущей страницы
        print(f"URL: {page.url}")

        # Нажимаем кнопку "Все"
        btn_all = page.query_selector("button[data-testid='xlsx-action-open-test-id-button-interface']")
        if btn_all:
            btn_all.click()
            print(f"Кнопка 'Все' нажата для {file_description}")
        else:
            print("Кнопка 'Все' не найдена")
            return
        page.wait_for_timeout(1000)

        # Нажимаем на нужный <li>
        li_element = page.query_selector(li_selector)
        if li_element:
            li_element.click()
            print(f"Кнопка '{file_description}' нажата")
        else:
            print(f"Кнопка '{file_description}' не найдена")
            return
        page.wait_for_timeout(1000)

        # Загружаем файл
        file_input = page.query_selector("input[type='file']")
        if file_input:
            file_input.set_input_files(file_path)
            print(f"Файл {file_path} загружен")
        else:
            print("Элемент для загрузки файла не найден")
            return
        
        page.wait_for_timeout(1000)

        

        # Нажимаем кнопку подтверждения загрузки
        confirm_button = page.query_selector("button[data-testid='xlsx-action-action-test-id-button-primary']")
        if confirm_button:
            confirm_button.click()
            print("Кнопка подтверждения загрузки нажата")
        else:
            print("Кнопка подтверждения загрузки не найдена")
            return
        
        page.wait_for_timeout(40000)
        
        try:
            confirm_input = page.wait_for_selector("input[data-testid='check-changes-warning-checkbox-test-id-checkbox-simple-input']", state="visible")
            if confirm_input:
                confirm_input.click()
                print("Нажата галочка подтверждения")
                confirm_button = page.query_selector("button[data-testid='xlsx-action-action-test-id-button-primary']")
                if confirm_button:
                    confirm_button.click()
                    print("Кнопка подтверждения загрузки нажата")
                else:
                    print("Кнопка подтверждения загрузки не найдена")
                    return
            else:
                print("галочки не найдено")
        except Exception as e:
            print(f"Error - {e}")


        error_text = page.query_selector("div[data-testid='xlsx-action-error-test-id-error-message-children'] span")

        if error_text:
            print(error_text.inner_text())
        else:
            print("Элемент не найден")

        page.wait_for_timeout(90000)
    except Exception as e:
        print(f"Ошибка при загрузке файла {file_path}: {e}")
    finally:
        # Закрываем страницу
        page.close()

def main(cab_pref, user_data_dir):
    price_dir = f"/root/scripts/WB_Autoreturns/{cab_pref}processed_files_splited/processed_update_prices"
    minprice_dir = f"/root/scripts/WB_Autoreturns/{cab_pref}processed_files_splited/processed_minprice_block"
    club_dir = f"/root/scripts/WB_Autoreturns/{cab_pref}processed_files_splited/processed_update_club"

    selectors = {
        "price": (
            "li[data-testid='xlsx-action-option-excel-update-test-id']",  # Селектор для <li>
            "button.xlsx-action-action-test-id-button-primary",  # Селектор для кнопки подтверждения
            "Обновить цены" 
        ),
        "minprice": (
            "li[data-testid='xlsx-action-option-auto-actions-min-price-update-test-id']",
            "button.xlsx-action-action-test-id-button-primary",
            "Обновить минимальную цену и заблокировать для автоакции"
        ),
        "club": (
            "li[data-testid='xlsx-action-option-club-discount-update-test-id']",
            "button.xlsx-action-action-test-id-button-primary",
            "Обновить скидки для WB Клуба"
        ),
    }

    with sync_playwright() as p:
        # Запуск браузера
        browser = p.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            headless=False, 
            args=["--no-sandbox", "--disable-dev-shm-usage", "--lang=en-US"],
        )

        
        try:

                        
            for filename in os.listdir(price_dir):
                if filename.endswith(".xlsx"):
                    file_path = os.path.join(price_dir, filename)
                    upload_file(browser, file_path, *selectors["price"])

            for filename in os.listdir(club_dir):
                if filename.endswith(".xlsx"):
                    file_path = os.path.join(club_dir, filename)
                    upload_file(browser, file_path, *selectors["club"])
                    

        finally:
            # Закрываем браузер
            browser.close()

if __name__ == "__main__":
    cab_pref = sys.argv[1]

    user_data_dir = sys.argv[2]
    main(cab_pref, user_data_dir)