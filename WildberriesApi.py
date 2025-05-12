import base64
import io
import zipfile
import aiohttp
from pathlib import Path
import asyncio
from utils import bytes_excel_to_dataframe
import pandas as pd
import os

class Wildberries:
    def __init__(self, key: str) -> None:
        self.key = key
        self.headers_key = {
            'Authorization': key
        }




    async def update_stocks(self, session: aiohttp.ClientSession, stocks: list[dict], warehouse_id: int):
        url = f'https://marketplace-api.wildberries.ru/api/v3/stocks/{warehouse_id}'
        data = {
            "stocks": stocks
        }
        response = await session.put(url=url, headers=self.headers_key, json=data)
        if response.status not in [200, 409, 204]:
            raise Exception(f'Не удалось обновить остатки: ({response.status}) {await response.text()}')
        try:
            response = await response.json()
        except:
            response = {}
        return response

