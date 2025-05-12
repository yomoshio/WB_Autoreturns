from playwright.sync_api import sync_playwright
import os
import datetime


def save_debug_info(page, prefix="error"):
    """Сохраняет скриншот и HTML текущей страницы"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    screenshot_path = f"{prefix}_screenshot_{timestamp}.png"
    html_path = f"{prefix}_page_source_{timestamp}.html"

    try:
        page.screenshot(path=screenshot_path)
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(page.content())
        print(f"Сохранён скриншот: {screenshot_path}")
        print(f"Сохранён HTML: {html_path}")
    except Exception as save_err:
        print(f"Ошибка при сохранении отладочной информации: {save_err}")


def main():
    # Пути для сохранения данных

    user_data_dir = "DIR"  

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            headless=False,
        )

        page = context.new_page()

        try:
            page.goto("https://seller.wildberries.ru/discount-and-prices", wait_until="domcontentloaded")
            page.wait_for_timeout(5000)

            print(f"Title: {page.title()}")
            print(f"URL: {page.url}")

            try:
                logo = page.query_selector("input[class='SimpleInput-JIIQvb037j SimpleInput--color-RichGreyNew-gIQvvmR+FY']")
                if logo:
                    logo.click()
                    logo.fill('NUMBER WITHOUT +7')
                    page.wait_for_timeout(60000)

                    btn = page.query_selector("button[data-testid='submit-phone-button']")
                    if btn:
                        btn.click()
                        print("Кнопка нажата")
                    else:
                        print("Кнопка не найдена")

                    page.wait_for_timeout(5000)
                    zxc = page.query_selector("form[class='CodeInputContentView__form-mgPTHveibl']")
                    if zxc:
                        print("Форма найдена")
                    else:
                        print("Форма не найдена")

                    input_fields = page.query_selector_all("input[data-testid='sms-code-input']")

                    if len(input_fields) == 6:
                        print("Найдено 6 полей для ввода кода")
                        code = input("Введите SMS-код (6 цифр): ")
                        if len(code) != 6 or not code.isdigit():
                            print("Ошибка: код должен состоять из 6 цифр.")
                        else:
                            for i, field in enumerate(input_fields):
                                field.click()
                                field.fill(code[i])
                                print(f"Введён символ: {code[i]}")
                                page.wait_for_timeout(500)
                    else:
                        print(f"Найдено {len(input_fields)} полей, ожидалось 6")
                        save_debug_info(page, prefix="form_error")
                else:
                    print("Элемент 'logo' не найден")
            except Exception as e:
                print(f"Ошибка при работе с формой: {e}")
                save_debug_info(page, prefix="form_error")

        except Exception as main_err:
            print(f"Произошла ошибка: {main_err}")
            save_debug_info(page, prefix="main_error")

        finally:
            context.close()

if __name__ == "__main__":
    main()