import pandas as pd
import os
from joblib import Parallel, delayed
from WildberriesApi import Wildberries
import asyncio
import aiohttp
from itertools import islice


async def batch(iterable, size):
    """Генератор для разбиения списка на батчи."""
    it = iter(iterable)
    while batch := list(islice(it, size)):
        yield batch


def read_skus_from_file(file_path: str) -> list:
    """
    Читает CSV-файл с SKU и возвращает список sku.
    Предполагается, что файл содержит одну колонку без заголовка.
    """
    # Читаем файл с помощью pandas. Параметр header=None указывает, что заголовка нет.
    df = pd.read_csv(file_path, header=None)
    
    skus = df.iloc[:, 0].dropna().astype(str).tolist()
    return skus


async def stocks_wb(key: str, warehouse_id: int, stocks: list):
    wb = Wildberries(key=key)
    async with aiohttp.ClientSession() as session:
        await wb.update_stocks(warehouse_id=warehouse_id, session=session, stocks=stocks)


async def main(cab_pref):
    if cab_pref == "T":
        key = "KEY"
        war_id_1 = 123456789
        war_id_2 = 123456789        
    else:
        key = "KEY"
        war_id_1 = 123456789 
        war_id_2 = 123456789

    file = f"/root/scripts/WB_Autoreturns/{cab_pref}artikuls/artikuls_no_stock.csv"

    skus = read_skus_from_file(file)
    stocks = [{"sku": str(item), "amount": 0} for item in skus]
    async for batch_stocks in batch(stocks, 1000):
        await stocks_wb(key, war_id_1, batch_stocks)
        await asyncio.sleep(1)
        await stocks_wb(key, war_id_2, batch_stocks)
        await asyncio.sleep(1)  


if __name__ == "__main__":
    asyncio.run(main("T"))
    asyncio.run(main("B"))
    