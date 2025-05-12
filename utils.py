import asyncio
import io

import functools
import gspread
import openpyxl
import pandas as pd

from consts import settings, bot


def bytes_excel_to_dataframe(decoded_data: bytes) -> pd.DataFrame:
    # decoded_data = base64.b64decode(data)
    # Создать объект BytesIO из декодированных байтов
    bytes_io = io.BytesIO(decoded_data)
    workbook = openpyxl.load_workbook(filename=bytes_io)
    sheet = workbook.active
    # Преобразовать данные из файла Excel в DataFrame Pandas
    data = sheet.values
    columns = next(data)  # Заголовки столбцов
    df = pd.DataFrame(data, columns=columns)
    return df


async def send_message(text: str, message_thread_id: None | int, chat_id: str = settings.CHAT_ID):
    try_count = 0
    while try_count < 100:
        try:
            await bot.send_message(
                chat_id=chat_id, 
                message_thread_id=None if message_thread_id=='None' else message_thread_id, 
                text=text, request_timeout=3)
            return
        except Exception as ex:
            try_count += 1
            print(ex)
            await asyncio.sleep(10)
            continue


def retry_on_exception_async(retries, delay=1, exceptions=(Exception,)):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            attempt = 0
            error_reason = None
            while attempt < retries:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    # print(f"Attempt {attempt + 1} failed: {e}")
                    error_reason = e
                    attempt += 1
                    if attempt < retries:
                        await asyncio.sleep(delay)
            raise Exception(f"Function failed after {retries} retries. Error: {error_reason}")
        return wrapper
    return decorator


def get_cookie():
    gc = gspread.service_account(filename='service.json')
    sheet = gc.open_by_key('18K2JcrzTolUMU2ZQD5rU8QknZprCnuVmltSdIJSBi-s')
    worksheet = sheet.worksheet("WB")
    content = worksheet.get_all_records()
    df = pd.DataFrame(content)
    df.set_index('Кабинет', inplace=True)
    result = {}
    for cabinet in df.index:
        cookies = df.loc[cabinet].to_dict()
        cookies_str = '; '.join(f'{key}={val}' for key, val in cookies.items())
        result[cabinet] = cookies_str
    return result
