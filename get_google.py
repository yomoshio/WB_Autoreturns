import csv
import asyncio
from collections import defaultdict
from YG import YGApi
import aiohttp

async def process_skus():
    yg_api = YGApi()
    skus = []

    async with aiohttp.ClientSession() as session:
        try:
            # Получаем все данные из ERP
            erp_data = await yg_api.get_erp(session)

            # Собираем только нужные SKU
            for item in erp_data:
                sku = item.get('sku_marketplace')
                if sku:
                    skus.append(sku)

            # Получаем цены по этим SKU
            sku_prices = await fetch_prices(skus)

        except Exception as e:
            print(f"Ошибка при обработке SKU: {e}")
            sku_prices = {}

    return sku_prices



async def fetch_prices(skus):
    """
    Запрос цен у поставщиков. Возвращает словарь {SKU: base_price}.
    """
    yg_api = YGApi()
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
                    sku_prices[sku] = buy  

            except Exception as e:
                print(f"Ошибка при получении buy для {sku}: {e}")

    return sku_prices


def main(cabinet_name: str, cab_pref: str):
    # Получение цен через API с помощью asyncio.run()
    sku_prices = asyncio.run(process_skus())

    # Запись в CSV
    with open(f'{cab_pref}_erp_files.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['SKU', 'Price'])
        for sku, price in sku_prices.items():
            writer.writerow([sku, price])
    
    print(f"Обработано {len(sku_prices)} SKU. Данные сохранены в {cab_pref}_erp_files.csv")

if __name__ == "__main__":
    main('Texnoplace', 'T')
    main('Бонатека', 'B')