from aiogram import Bot
from pydantic import field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    TELEGRAM_TOKEN_API: str
    CHAT_ID: str
    THREAD_ID: int | None
    YG_TOKEN: str
    YG_BASE_URL: str

    class Config:
        env_file = '.env'


settings = Settings()

# default=DefaultBotProperties(parse_mode='HTML')
# bot = Bot(settings.TELEGRAM_TOKEN_API, default=default)
bot = Bot(settings.TELEGRAM_TOKEN_API)
