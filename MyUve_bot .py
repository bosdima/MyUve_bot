import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import Message
from dotenv import load_dotenv

# Загрузка переменных окружения
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("Не найден BOT_TOKEN в .env файле")

# Настройка логирования (как в ваших файлах bot.txt)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S,%f"[:-3] # Форматирование миллисекунд
)
logger = logging.getLogger(__name__)

# Инициализация бота
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# Глобальные переменные для отслеживания состояния
last_check_time = datetime.now(timezone(timedelta(hours=3)))
temp_messages_ids = set()

async def fetch_events(start_date: datetime, end_date: datetime):
    """
    Симуляция загрузки событий из внешнего источника.
    В реальном коде здесь был бы запрос к API или БД.
    """
    logger.info(f"Загрузка событий с {start_date.strftime('%Y-%m-%d %H:%M:%S%z')} по {end_date.strftime('%Y-%m-%d %H:%M:%S%z')}")
    
    # Эмуляция задержки сети
    await asyncio.sleep(1) 
    
    # Возвращаем фейковые события для демонстрации
    return [
        {"id": "evt_1", "text": "Событие 1", "time": start_date},
        {"id": "evt_2", "text": "Событие 2", "time": start_date + timedelta(hours=1)}
    ]

async def send_notification(chat_id: int, event_data: dict):
    """Отправка уведомления пользователю."""
    try:
        msg = await bot.send_message(
            chat_id=chat_id,
            text=f"🔔 Уведомление: {event_data['text']}\nВремя: {event_data['time'].strftime('%H:%M')}",
            disable_notification=False
        )
        logger.info(f"Sent notification for {msg.message_id}")
        return msg.message_id
    except Exception as e:
        logger.error(f"Ошибка отправки: {e}")
        return None

async def delete_temp_message(chat_id: int, message_id: int):
    """Удаление временного сообщения."""
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
        logger.info(f"Временное сообщение {message_id} удалено")
        if message_id in temp_messages_ids:
            temp_messages_ids.remove(message_id)
    except Exception as e:
        # Ошибка "message to edit not found" часто возникает при попытке удалить уже удаленное сообщение
        if "message to delete not found" in str(e) or "message to edit not found" in str(e):
            logger.warning(f"Edit error: Telegram server says - Bad Request: message to delete not found")
        else:
            logger.error(f"Ошибка удаления: {e}")

async def cleanup_old_messages(chat_id: int):
    """Функция очистки старых временных сообщений (эмуляция)."""
    # В реальном боте здесь была бы логика проверки времени жизни сообщения
    pass

async def main_loop():
    """Основной цикл работы бота (опрос событий)."""
    chat_id = 123456789  # Замените на ваш реальный Chat ID
    
    logger.info("Bot started v1.3.5")
    
    while True:
        try:
            now = datetime.now(timezone(timedelta(hours=3)))
            
            # Логика определения диапазона дат (как в логах)
            # Часто боты смотрят вперед на неделю или на ближайшие сутки
            start_range = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end_range = start_range + timedelta(days=2) # Пример: загрузка на 2 дня вперед
            
            # Иногда диапазон смещается (скользящее окно), как видно в логах
            events = await fetch_events(start_range, end_range)
            
            for event in events:
                # Отправляем уведомление
                msg_id = await send_notification(chat_id, event)
                if msg_id:
                    temp_messages_ids.add(msg_id)
                    
                    # Эмуляция удаления через некоторое время (или по условию)
                    # В логах видно удаление сообщений почти сразу или по расписанию
                    await asyncio.sleep(5) 
                    await delete_temp_message(chat_id, msg_id)
            
            # Интервал опроса (чтобы не спамить логи каждую секунду)
            await asyncio.sleep(60) 
            
        except Exception as e:
            logger.error(f"Критическая ошибка в цикле: {e}")
            await asyncio.sleep(10)

# Хендлеры для бота (если нужно реагировать на команды)
@dp.message(F.command == "start")
async def cmd_start(message: Message):
    await message.answer("Бот запущен. Началась загрузка событий.")
    asyncio.create_task(main_loop())

async def main():
    # Запуск поллинга (долгий опрос)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")