# main.py
import asyncio
from typing import Set, List, Dict, AsyncIterator
from YG import YGApi
import pandas as pd
import aiohttp

def read_skus_from_file(file_path: str) -> list:
    """
    Читает CSV-файл с SKU и возвращает список sku, обрезая первые два символа (начиная с третьего).
    Предполагается, что файл содержит одну колонку без заголовка.
    """
    df = pd.read_csv(file_path, header=None)

    # Извлекаем первую колонку, удаляем NaN, приводим к строкам и обрезаем с 3-го символа
    skus = df.iloc[:, 0].dropna().astype(str).apply(lambda x: x[3:] if len(x) > 3 else "").tolist()
    
    return skus


async def generate_batches(skus: Set[str], excluded: bool, batch_size: int = 1000) -> AsyncIterator[List[Dict[str, str | bool]]]:
    """
    Генерирует батчи данных для отправки.
    :param skus: Множество sku.
    :param excluded: Значение excluded (True или False).
    :param batch_size: Размер батча (по умолчанию 1000).
    :return: Асинхронный итератор батчей.
    """
    batch = []
    for sku in skus:
        batch.append({"sku": sku, "excluded": excluded})
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:  # Отправляем оставшиеся данные
        yield batch

async def process_files_and_send_requests(excluded_true_path: str, excluded_false_path: str, batch_size: int = 1000, max_concurrent_requests: int = 10):
    """
    Основная функция: читает файлы, формирует данные и отправляет запросы батчами.
    :param excluded_true_path: Путь к файлу с sku, где excluded=True.
    :param excluded_false_path: Путь к файлу с sku, где excluded=False.
    :param batch_size: Размер батча (по умолчанию 1000).
    :param max_concurrent_requests: Максимальное количество одновременных запросов (по умолчанию 10).
    """
    # Чтение файлов
    excluded_true_skus = read_skus_from_file(excluded_true_path)  # sku с excluded=True
    excluded_false_skus = read_skus_from_file(excluded_false_path)  # sku с excluded=False

    # Создаем клиент для отправки запросов
    client = YGApi()

    # Список задач для параллельной отправки
    tasks = []

    # Генерация и отправка батчей для excluded=True
    async for batch in generate_batches(excluded_true_skus, excluded=True, batch_size=batch_size):
        tasks.append(client.set_all_excluded(batch))
        if len(tasks) >= max_concurrent_requests:
            await asyncio.gather(*tasks)
            tasks = []

    # Генерация и отправка батчей для excluded=False
    async for batch in generate_batches(excluded_false_skus, excluded=False, batch_size=batch_size):
        tasks.append(client.set_all_excluded(batch))
        if len(tasks) >= max_concurrent_requests:
            await asyncio.gather(*tasks)
            tasks = []

    # Отправка оставшихся задач
    if tasks:
        await asyncio.gather(*tasks)

    print("Все запросы отправлены.")


async def send_update_stocks(excluded_true_path: str):
    excluded_true_skus = read_skus_from_file(excluded_true_path) 
    print(excluded_true_skus)
    client = YGApi()
    
    async with aiohttp.ClientSession() as session:
        await client.update_stocks(session=session, skus=excluded_true_skus)


async def main(cab_pref):
    excluded_false_path = f"/root/scripts/WB_Autoreturns/{cab_pref}artikuls/artikuls_no_stock.csv"
    excluded_true_path = f"/root/scripts/WB_Autoreturns/{cab_pref}artikuls/artikuls_with_stock.csv"
    await send_update_stocks(excluded_true_path)

# Запуск асинхронного кода
if __name__ == "__main__":
    asyncio.run(main("B"))