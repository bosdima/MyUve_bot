import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, List
from uuid import UUID

from pydantic import BaseModel, Field, ValidationError, AnyUrl
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message

# Конфигурация
API_TOKEN = "YOUR_BOT_TOKEN_HERE"  # Замените на ваш токен
LOG_FILE = "bot.log"
TIMEZONE = timezone(timedelta(hours=3))  # MSK

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Bot")

# --- Модели данных (Pydantic) ---

class EventFilter(BaseModel):
    """Модель для фильтрации событий по датам"""
    start_date: datetime = Field(..., description="Начало периода")
    end_date: datetime = Field(..., description="Конец периода")
    
    class Config:
        json_schema_extra = {
            "example": {
                "start_date": "2026-04-27T00:00:00+03:00",
                "end_date": "2026-04-29T00:00:00+03:00"
            }
        }

class NotificationData(BaseModel):
    """Модель данных для уведомления"""
    user_id: str = Field(..., min_length=1)
    event_type: str = "default"
    details: Optional[str] = None
    
    def is_valid_uuid(self) -> bool:
        try:
            UUID(self.user_id)
            return True
        except ValueError:
            # Допускаем и email-подобные строки, как в логах (7jibswan...)
            return "@" in self.user_id or len(self.user_id) > 5

# --- Имитация источника данных ---

async def fetch_events(start: datetime, end: datetime) -> List[dict]:
    """
    Имитация загрузки событий из внешнего API.
    В реальном проекте здесь будет запрос к базе данных или внешнему сервису.
    """
    logger.info(f"Загрузка событий с {start.isoformat()} по {end.isoformat()}")
    
    # Симуляция задержки сети
    await asyncio.sleep(0.1) 
    
    # Возвращаем пустой список или фейковые данные для демонстрации
    # В логах видно, что загрузка происходит постоянно, но реальных событий может не быть
    return []

# --- Логика бота ---

class MonitorBot:
    def __init__(self, token: str):
        self.bot = Bot(token=token)
        self.dp = Dispatcher()
        self.running = True
        self.last_check_time = datetime.now(TIMEZONE)
        
        # Регистрация хендлеров
        self.dp.message.register(self.cmd_start, Command("start"))
        self.dp.message.register(self.cmd_status, Command("status"))

    async def cmd_start(self, message: Message):
        await message.answer("Бот мониторинга событий запущен (v1.3.5). Ожидайте уведомлений.")

    async def cmd_status(self, message: Message):
        await message.answer(f"Статус: Работает\nПоследняя проверка: {self.last_check_time}")

    async def process_cycle(self):
        """Основной цикл мониторинга"""
        logger.info("Bot started v1.3.5")
        
        while self.running:
            try:
                now = datetime.now(TIMEZONE)
                
                # Логика определения диапазона дат из логов:
                # Бот часто проверяет диапазон [Сегодня 00:00, Послезавтра 00:00]
                # Или скользящее окно [Текущее время - смещение, Текущее время + 7 дней]
                
                # Вариант 1: Проверка ближайших 2 дней (как в начале логов)
                start_range = now.replace(hour=0, minute=0, second=0, microsecond=0)
                end_range = start_range + timedelta(days=2)
                
                events = await fetch_events(start_range, end_range)
                
                # Обработка найденных событий (если бы они были)
                for event in events:
                    await self.handle_event(event)

                # Вариант 2: Скользящее окно (появляется в логах позже)
                # start_slide = now - timedelta(hours=1) # Условное смещение
                # end_slide = now + timedelta(days=7)
                # ... загрузка ...

                # Симуляция удаления временных сообщений (из логов: "Временное сообщение 6716 удалено")
                # В реальном коде здесь была бы очистка кэша или старых сообщений в чате
                
                # Пауза между циклами (в логах интервал ~1 минута)
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"Ошибка в цикле мониторинга: {e}", exc_info=True)
                await asyncio.sleep(10)

    async def handle_event(self, event_data: dict):
        """Обработка конкретного события и отправка уведомления"""
        # Пример формирования ID пользователя из данных события
        user_identifier = event_data.get('user_id', 'unknown')
        
        try:
            notification = NotificationData(user_id=user_identifier)
            if not notification.is_valid_uuid():
                logger.warning(f"Невалидный ID пользователя: {user_identifier}")
                return

            msg_text = f"⚠️ Новое событие для {notification.user_id}"
            if notification.details:
                msg_text += f"\nДетали: {notification.details}"
            
            # Отправка уведомления (в логах: "Sent notification for...")
            logger.info(f"Sent notification for {notification.user_id}")
            
            # Здесь должен быть код отправки в Telegram конкретному пользователю
            # await self.bot.send_message(chat_id=chat_id, text=msg_text)
            
            # Симуляция удаления сообщения после обработки (из логов: "Deleted: ...")
            # В реальном сценарии это может быть удаление сообщения-триггера
            # logger.info(f"Deleted: {notification.user_id}")
            
        except ValidationError as e:
            logger.error(f"Ошибка валидации данных события: {e.errors()}")

    async def run(self):
        """Запуск поллинга и основного цикла"""
        # Запускаем цикл мониторинга в фоне
        monitor_task = asyncio.create_task(self.process_cycle())
        
        # Запускаем поллинг Telegram бота
        try:
            await self.dp.start_polling(self.bot)
        finally:
            self.running = False
            monitor_task.cancel()
            await self.bot.session.close()

# --- Точка входа ---

if __name__ == "__main__":
    # Проверка токена (заглушка)
    if API_TOKEN == "YOUR_BOT_TOKEN_HERE":
        logger.error("Необходимо установить API_TOKEN в начале файла!")
        exit(1)

    bot_instance = MonitorBot(API_TOKEN)
    
    try:
        asyncio.run(bot_instance.run())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")