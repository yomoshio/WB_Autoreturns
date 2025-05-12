import aiohttp
from pathlib import Path
import gc 
import math 
import aiofiles
import csv
from typing import Set, List, Dict, AsyncIterator


class YGApi:
    def __init__(self) -> None:
        self.token = "TOKEN_TO_CUSTOM"
        self.yg_url = "https://resalecrm.com"
        self.headers = {
            'Authorization': 'Bearer ' + self.token
        }  
        self.output_dir = Path("/root/scripts/WB_Autoreturns/artikuls")  


    async def set_all_excluded(self, skus: List[Dict[str, str | bool]]):
        """
        Отправляет запрос на сервер для обновления excluded.
        :param skus: Список словарей вида [{"sku": "1000005", "excluded": True}]
        """
        url = f'{self.yg_url}/api/v1/suppliers/set_excluded_all'
        headers = {
            'Content-Type': 'application/json',
            # Добавьте другие заголовки, если они нужны (например, авторизация)
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(url, json=skus, headers=headers) as response:
                    if response.status == 200:
                        # Успешный запрос
                        response_data = await response.json()
                        print("Успешно:", response_data)
                        return response_data
                    else:
                        # Обработка ошибок
                        error_data = await response.text()
                        print(f"Ошибка: {response.status}, {error_data}")
                        raise Exception(f"Ошибка запроса: {response.status}, {error_data}")
            except aiohttp.ClientError as e:
                print(f"Ошибка при выполнении запроса: {e}")
                raise
    
    
    async def update_stocks(self, session: aiohttp.ClientSession, skus: list):
        url = f'{self.yg_url}/api/v1/suppliers/update_stocks'
        limit = 100
        portions = [skus[i:i + limit] for i in range(0, len(skus), limit)]
        for portion in portions:
            response = await session.post(url=url, headers=self.headers, json=portion)
            if response.status != 200:
                print(f'Не удалось обновить остатки товара : {await response.text()}')
                continue
        return "OK"
    

    
    

    async def get_skus(self, session: aiohttp.ClientSession, skus: list):
        url = f'{self.yg_url}/api/v1/suppliers/get_products_all'
        response = await session.post(url=url, headers=self.headers, json=skus)
        if response.status != 200:
            raise Exception(f'Не удалось получить данные по артикулам поставщика: {await response.text()}')

        response = await response.json()
        return response  
    

    async def search_product(self, session: aiohttp.ClientSession, supplier: str, sku: str):
        url = f"{self.yg_url}/api/v1/suppliers/{supplier}/search?limit=1&offset=0&sku={sku}&sort_order=asc&sort_field=sku"
        response = await session.get(url=url, headers=self.headers)
        if response.status != 200:
            raise Exception(f'Не удалось получить данные по артикулам поставщика: {await response.text()}')

        response = await response.json()
        return response 
    

    async def get_products_list(self, session: aiohttp.ClientSession, supplier: str, skus: list[str | int]):
        url = f"{self.yg_url}/api/v1/suppliers/{supplier}/search/list"
        response = await session.post(url=url, headers=self.headers, json=skus)
        if response.status != 200:
            raise Exception(f'Не удалось получить данные по артикулам поставщика: {await response.text()}')

        response = await response.json()
        return response 
    

    async def update_fields(self, session: aiohttp.ClientSession, supplier: str, data: list[dict]):
        url = f'{self.yg_url}/api/v1/suppliers/{supplier}/update'
        response = await session.post(
            url=url, headers=self.headers, json=data
        )
        if response.status != 200:
            raise Exception((
                f'Не удалось обновить поля по артикулам поставщика '
                f'{supplier}: {await response.text()}'
            ))
        response = await response.json()
        return response
    

    async def get_erp(self, session: aiohttp.ClientSession):
        url = f"{self.yg_url}/api/v1/erp/returns"
        limit = 1000  # количество записей на странице
        offset = 0  # начальное смещение
        all_data = []  # список для хранения всех данных
        
        while True:
            # Формируем параметры запроса
            params = {
                'limit': limit,
                'offset': offset,
                'substatus': 'В наличии'  # если нужен этот фильтр
            }
            
            try:
                # Выполняем GET запрос
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Проверяем успешность запроса
                        if not data.get('success', False):
                            break
                        
                        # Добавляем полученные данные в общий список
                        current_data = data.get('data', [])
                        all_data.extend(current_data)
                        
                        # Получаем общее количество записей
                        total = data.get('total', 0)
                        
                        # Если получили все записи или данных больше нет, выходим
                        if not current_data or len(all_data) >= total:
                            break
                            
                        # Увеличиваем offset для следующей страницы
                        offset += limit
                        
                    else:
                        print(f"Ошибка: статус {response.status}")
                        break
                        
            except Exception as e:
                print(f"Произошла ошибка: {str(e)}")
                break
        
        return all_data