import pandas as pd
import os
from joblib import Parallel, delayed
from WildberriesApi import Wildberries
import asyncio
import aiohttp
import math
from YG import YGApi
from typing import List

yg_api = YGApi()


def convert_xlsx_to_csv(input_path, output_path):
    """
    Конвертирует файл .xlsx в .csv.
    :param input_path: Путь к исходному файлу .xlsx.
    :param output_path: Путь для сохранения .csv.
    """
    try:
        # Читаем .xlsx
        df = pd.read_excel(input_path)
        
        # Сохраняем в .csv
        df.to_csv(output_path, index=False, encoding="utf-8")
        print(f"Файл {input_path} успешно конвертирован в {output_path}")

    except Exception as e:
        print(f"Ошибка при конвертации файла {input_path}: {e}")


async def stocks_wb(key: str, warehouse_id: int, stocks: List[dict]):
    wb = Wildberries(key=key)

    # Размер батча
    batch_size = 1000

    async with aiohttp.ClientSession() as session:
        for i in range(0, len(stocks), batch_size):
            # Получаем подсписок — батч из 1000 элементов (или меньше в конце)
            batch = stocks[i:i + batch_size]

            # Отправляем батч
            await wb.update_stocks(
                warehouse_id=warehouse_id,
                session=session,
                stocks=batch
            )

            # Задержка 1 секунда между батчами
            if i + batch_size < len(stocks):
                await asyncio.sleep(3)


def save_unique_to_csv(df, file_path):
    """
    Добавляет данные в CSV, удаляет дубликаты по первой колонке и сохраняет обратно.
    
    :param df: DataFrame, который нужно сохранить.
    :param file_path: Путь к CSV-файлу.
    """
    # Добавляем данные в CSV
    df.to_csv(file_path, index=False, header=not os.path.exists(file_path), mode="a")

    # Читаем файл, удаляем дубликаты по первой колонке и пересохраняем
    full_df = pd.read_csv(file_path)
    full_df.drop_duplicates(subset=full_df.columns[0], inplace=True)
    full_df.to_csv(file_path, index=False, header=True, mode="w")


def get_comissions() -> dict:
    comissions = pd.read_excel("/root/scripts/WB_Autoreturns/comission.xlsx")
    comissions = comissions.to_dict('records')
    comissions = {i["Subject"]: i["Seller's warehouse: distribution to WB warehouse, %"] for i in comissions}
    return comissions


def calculate_new_price(row, erp_skus, comissions):
    sku = row["Seller item No."]
    category = row["Category"]

    # SKU без первых 3 символов
    base_sku = sku
    base_price = erp_skus.get(base_sku, None)


    # Проверяем, если цена отсутствует или NaN, то пропускаем запись
    if base_price is None or base_price == "N/A" or (isinstance(base_price, float) and math.isnan(base_price)):
        return None  # Возвращаем None, чтобы Pandas не записывал новую цену

    # Получаем комиссию для категории
    service_percent = comissions.get(category, "0").replace(',', '.')  # Дефолт 0% если нет категории
    try:
        service = float(service_percent) / 100
        final = (0.9 - service) 
        final_price = (base_price / final)
        #print(final_price)
    except Exception as e:
        print(f"Не удалось получить комиссию для категории {category} sku - {sku} - {e}")
        service = 1.20
        final = (0.9 - service)   # Если вдруг комиссия не конвертируется, ставим 100%
        final_price = (base_price / final)

    # Вычисляем итоговую цену
    return int(round(final_price))


async def fetch_prices(skus):
    """
    Запрос цен у поставщиков. Возвращает словарь {SKU: base_price}.
    """
    pref_to_supplier = {
        "P": "pleer",
        "C": "citilink",
        "S": "simaland",
        "V": "vse_instrumenti",
        "A": "akusherstvo",
        "O": "online_trade",
        "G": "golden_apple",
        "D": "detskiy_mir",
        "L": "leonardo",
        "R": "riv_gosh",
        "M": "lemana",
        "K": "komus",
        "T": "letuile"
    }

    sku_prices = {}

    async with aiohttp.ClientSession() as session:
        for sku in skus:
            try:
                supplier = pref_to_supplier.get(sku[1])  # Определяем поставщика по первой букве SKU
                if not supplier:
                    print(f"Предупреждение: Поставщик для {sku} не найден.")
                    continue
                
                trimmed_sku = sku[3:]  # Убираем первые 3 символа перед отправкой запроса
                
                response = await yg_api.get_products_list(session=session, supplier=supplier, skus=[trimmed_sku])
                
                if int(response.get('total', 0)) != 0:
                    data = response['data'][0]
                    buy = data.get('price_purchase')

                    if buy is not None:
                        # Вычисляем объем для кросс-доставки
                        width = data.get('width', 0)  
                        height = data.get('height', 0)
                        length = data.get('length', 0)
                        volume = width * height * length / 1000
                        
                        cross = 35 + (volume - 1) * 8.5
                        fix = 100  

                        # Рассчитываем base_price без учета комиссии
                        base_price = calculate_base_price(buy, cross, fix)
                        sku_prices[sku] = base_price  # Записываем цену для полного SKU

            except Exception as e:
                print(f"Ошибка при получении buy для {sku}: {e}")

    return sku_prices



def calculate_base_price(buy: float, cross: float, fix: float) -> float:
    """Рассчитывает цену без учета сервисного коэффициента."""
    return round(2 * (0.7*buy + cross + fix))


def calculate_new_price_non_erp(row, sku_prices, comissions):
    """
    Вычисляет новую цену с учетом комиссии.
    """
    sku = row["Seller item No."]  # SKU без первых 3 символов
    category = row["Category"]

    base_price = sku_prices.get(sku, None)  # Получаем base_price
    
    if base_price is None:
        return None

    # Получаем комиссию
    service_percent = float(comissions.get(category, "0").replace(',', '.'))
    final = (0.9 - (service_percent / 100))

    try:
        final_price = (base_price / final)
    except Exception as e:
        print(f"Ошибка комиссии для {category} SKU {sku}: {e}")
        final = 0.9 - 1.20  # Фолбэк
        final_price = (base_price / final)



    return int(round(final_price))


async def fetch_excuded(skus):
    """
    Запрос информации о SKU у поставщиков. Возвращает множество SKU, у которых excluded=True.
    """
    pref_to_supplier = {
        "P": "pleer",
        "C": "citilink",
        "S": "simaland",
        "V": "vse_instrumenti",
        "A": "akusherstvo",
        "O": "online_trade",
        "G": "golden_apple",
        "D": "detskiy_mir",
        "L": "leonardo",
        "R": "riv_gosh",
        "M": "lemana",
        "K": "komus",
        "T": "letuile"
    }

    excluded_skus = set()

    async with aiohttp.ClientSession() as session:
        for sku in skus:
            try:
                supplier = pref_to_supplier.get(sku[1])  # Определяем поставщика по второй букве SKU
                if not supplier:
                    print(f"Предупреждение: Поставщик для {sku} не найден.")
                    continue
                
                trimmed_sku = sku[3:]  # Убираем первые 3 символа перед отправкой запроса
                response = await yg_api.get_products_list(session=session, supplier=supplier, skus=[trimmed_sku])
                
                if int(response.get('total', 0)) != 0:
                    data = response['data'][0]
                    excluded = data.get('excluded', False)  # Предполагаем, что есть поле excluded
                    if excluded:
                        excluded_skus.add(sku)  # Добавляем SKU в множество, если excluded=True
            except Exception as e:
                print(f"Ошибка при получении данных для {sku}: {e}")

    return excluded_skus

def process_csv(input_path, output_path, artikuls_with_stock_path, key, war_id, artikuls_no_stock_path, erp_sku_path, file_type="default"):
    """
    Обрабатывает .csv файл, добавляет условия по SKU и сохраняет результаты.
    """
    try:
        comissions = get_comissions()
        df = pd.read_csv(input_path)

        # Загружаем ERP-SKU
        erp_df = pd.read_csv(erp_sku_path)
        erp_skus = erp_df.set_index("SKU")["Price"].to_dict()
        erp_skus = {str(k): v for k, v in erp_skus.items()}
        print(list(erp_skus.keys()))
        # Преобразуем нужные столбцы
        df["Seller item No."] = df["Seller item No."].fillna("").astype(str)
        if "New Smart Promo discount blocking" in df.columns:
            df["New Smart Promo discount blocking"] = df["New Smart Promo discount blocking"].fillna("").astype(str)

        # Фильтр ERP-SKU
        erp_mask = df["Seller item No."].isin(list(erp_skus.keys()))
        non_erp_mask = ~erp_mask
        # print(erp_mask)
        # print(non_erp_mask)
        erp_barcodes = list(map(str, df.loc[erp_mask, "Barcode"].unique()))
        non_erp_barcodes = list(map(str, df.loc[non_erp_mask, "Barcode"].unique()))
        stocks = [{'sku': item, 'amount': 0} for item in non_erp_barcodes]
        #print(stocks)
        asyncio.run(stocks_wb(key=key, warehouse_id=war_id, stocks=stocks))
        if erp_barcodes:
            stocks = [{'sku': item, 'amount': 1} for item in erp_barcodes]
            #print(stocks)
            asyncio.run(stocks_wb(key=key, warehouse_id=war_id, stocks=stocks))

        # Применяем скидки только для не-ERP товаров
        if file_type == "club":
            # Получаем SKU только для строк с условием
            relevant_skus = df.loc[~erp_mask & (df["WB inventory"] > 0), "Seller item No."].dropna().astype(str).unique().tolist()
            excluded_skus = asyncio.run(fetch_excuded(relevant_skus))  # Проверяем только эти SKU
            excluded_mask = df["Seller item No."].isin(excluded_skus)

            df.loc[~erp_mask & ~excluded_mask & (df["WB inventory"] > 0), "New WB Club discount"] = 5
            df.loc[~erp_mask & ~excluded_mask & (df["WB inventory"] == 0), "New WB Club discount"] = 0
        elif file_type == "minprice_block":
            # Получаем SKU только для строк с условием
            relevant_skus = df.loc[~erp_mask & (df["WB inventory"] > 0), "Seller item No."].dropna().astype(str).unique().tolist()
            excluded_skus = asyncio.run(fetch_excuded(relevant_skus))
            excluded_mask = df["Seller item No."].isin(excluded_skus)

            df.loc[~erp_mask & ~excluded_mask & (df["WB inventory"] > 0), "New Smart Promo discount blocking"] = "No"
            df.loc[~erp_mask & ~excluded_mask & (df["WB inventory"] == 0), "New Smart Promo discount blocking"] = "Yes"
        else:
            # Получаем SKU только для строк с условием
            relevant_skus = df.loc[~erp_mask & (df["WB inventory"] > 0), "Seller item No."].dropna().astype(str).unique().tolist()
            excluded_skus = asyncio.run(fetch_excuded(relevant_skus))
            excluded_mask = df["Seller item No."].isin(excluded_skus)

            df.loc[~erp_mask & ~excluded_mask & (df["WB inventory"] > 0), "New discount"] = 50
            df.loc[~erp_mask & ~excluded_mask & (df["WB inventory"] == 0), "New discount"] = 20

            non_erp_skus = df.loc[~erp_mask & ~excluded_mask & (df["WB inventory"] > 0), "Seller item No."].dropna().astype(str)
            cleaned_skus = non_erp_skus.unique().tolist()
            sku_prices = asyncio.run(fetch_prices(cleaned_skus))
            print(sku_prices)

            before_change = df.loc[~erp_mask & ~excluded_mask & (df["WB inventory"] > 0), ["Seller item No.", "New price"]].copy()
            df.loc[~erp_mask & ~excluded_mask & (df["WB inventory"] > 0), "New price"] = df.loc[~erp_mask & ~excluded_mask & (df["WB inventory"] > 0)].apply(
                lambda row: calculate_new_price_non_erp(row, sku_prices, comissions), axis=1
            )
            changed_rows = df.loc[~erp_mask & ~excluded_mask & (df["WB inventory"] > 0), ["Seller item No.", "New price"]]
            changed_rows = changed_rows[changed_rows["New price"] != before_change["New price"]]
            print("Измененные строки:")
            print(changed_rows)

        # Обработка ERP-SKU
        if file_type == "club":
            relevant_skus = df.loc[erp_mask, "Seller item No."].dropna().astype(str).unique().tolist()
            excluded_skus = asyncio.run(fetch_excuded(relevant_skus))
            excluded_mask = df["Seller item No."].isin(excluded_skus)

            df.loc[erp_mask & ~excluded_mask, "New WB Club discount"] = 5
        elif file_type == "minprice_block":
            relevant_skus = df.loc[erp_mask, "Seller item No."].dropna().astype(str).unique().tolist()
            excluded_skus = asyncio.run(fetch_excuded(relevant_skus))
            excluded_mask = df["Seller item No."].isin(excluded_skus)

            df.loc[erp_mask & ~excluded_mask, "New Smart Promo discount blocking"] = "No"
        else:
            relevant_skus = df.loc[erp_mask, "Seller item No."].dropna().astype(str).unique().tolist()
            excluded_skus = asyncio.run(fetch_excuded(relevant_skus))
            excluded_mask = df["Seller item No."].isin(excluded_skus)

            df.loc[erp_mask & ~excluded_mask, "New discount"] = 50
            df.loc[erp_mask & ~excluded_mask, "New price"] = df.loc[erp_mask & ~excluded_mask].apply(
                lambda row: calculate_new_price(row, erp_skus, comissions), axis=1
            )

        # Сохранение результатов
        artikuls = pd.concat([
            df.loc[df["WB inventory"] > 0, ["Seller item No."]],
            pd.DataFrame(df.loc[erp_mask, "Seller item No."].unique(), columns=["Seller item No."])
        ])
        barcodes = pd.concat([
            df.loc[df["WB inventory"] > 0, ["Barcode"]],
            pd.DataFrame(df.loc[erp_mask, "Barcode"], columns=["Barcode"])
        ])
        artikuls.to_csv(artikuls_with_stock_path, index=False, header=False)
        barcodes.to_csv(artikuls_no_stock_path, index=False, header=False)

        df = df.drop_duplicates(subset="Seller item No.")
        df.to_csv(output_path, index=False)
        print(f"Файл успешно обработан и сохранён в {output_path}")

    except FileNotFoundError as e:
        print(f"Ошибка: Файл не найден - {e}")
    except pd.errors.EmptyDataError:
        print("Ошибка: Входной CSV-файл пуст!")
    except pd.errors.ParserError:
        print("Ошибка: Проблема с разбором CSV-файла!")
    except Exception as e:
        print(f"Неизвестная ошибка: {e}")


def main(cab_pref):
    # Пути для файлов
    if cab_pref == "T":
        key = "KEY"
        war_id = 123456789
    else:
        key = "KEY"
        war_id = 123456789 

    input_dir = f"/root/scripts/WB_Autoreturns/{cab_pref}files"  # Директория с исходными файлами
    output_dir = f"/root/scripts/WB_Autoreturns/{cab_pref}processed_files"  # Директория для обработанных файлов
    artikuls_dir = f"/root/scripts/WB_Autoreturns/{cab_pref}artikuls"  # Директория для артикулов
    csv_dir = f"/root/scripts/WB_Autoreturns/{cab_pref}csv_files"  # Директория для .csv файлов
    erp_sku_path = f"/root/scripts/WB_Autoreturns/{cab_pref}_erp_files.csv"
    # Создаем директории, если они не существуют
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(artikuls_dir, exist_ok=True)
    os.makedirs(csv_dir, exist_ok=True)

    # Файлы для обработки
    files_to_process = [
        ("update_prices.xlsx", "default"),
        ("update_club.xlsx", "club"),
        #("minprice_block.xlsx", "minprice_block"),
    ]



    for file_name, file_type in files_to_process:
        input_path = os.path.join(input_dir, file_name)
        csv_path = os.path.join(csv_dir, file_name.replace(".xlsx", ".csv"))
        convert_xlsx_to_csv(input_path, csv_path)
        
        

    # Обрабатываем .csv файлы
    for file_name, file_type in files_to_process:
        csv_path = os.path.join(csv_dir, file_name.replace(".xlsx", ".csv"))
        output_path = os.path.join(output_dir, f"processed_{file_name.replace('.xlsx', '.csv')}")
        artikuls_with_stock_path = os.path.join(artikuls_dir, "artikuls_with_stock.csv")
        artikuls_no_stock_path = os.path.join(artikuls_dir, "artikuls_no_stock.csv")

        process_csv(csv_path, output_path, artikuls_with_stock_path, key, war_id, artikuls_no_stock_path, erp_sku_path=erp_sku_path, file_type=file_type)

if __name__ == "__main__":
    main("B")
    main("T")
