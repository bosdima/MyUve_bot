import logging
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='bot_fixed.log' # Сохраняем логи в файл
)
logger = logging.getLogger(__name__)

class EventBot:
    def __init__(self, token: str):
        self.token = token
        # Инициализируем курсор времени. 
        # Важно: сохраняем это значение в БД или файл, чтобы при перезапуске не начинать сначала
        self.last_check_time = datetime.now(timezone.utc) - timedelta(hours=1) 
        
        # Словарь для отслеживания ID сообщений, которые можно редактировать (опционально)
        self.active_messages = {} 

    async def fetch_events(self, start_time: datetime, end_time: datetime):
        """
        Эмуляция запроса к внешнему API за событиями.
        В реальном коде здесь будет запрос к вашей базе данных или стороннему сервису.
        """
        logger.info(f"Загрузка событий с {start_time} по {end_time}")
        
        # СИМУЛЯЦИЯ: Возвращаем пустой список или тестовые данные
        # В реальности здесь должен быть код, который возвращает новые события
        return [] 

    async def send_or_update_notification(self, event_data: dict):
        """
        Отправка нового сообщения или обновление существующего.
        Обрабатывает ошибку 'message not found'.
        """
        chat_id = event_data.get('chat_id')
        message_text = event_data.get('text')
        msg_id = event_data.get('message_id') # Если это обновление

        try:
            if msg_id and msg_id in self.active_messages.get(chat_id, {}):
                # Попытка редактирования
                # await bot.edit_message_text(text=message_text, chat_id=chat_id, message_id=msg_id)
                logger.info(f"Сообщение {msg_id} обновлено")
            else:
                # Отправка нового сообщения
                # new_msg = await bot.send_message(chat_id=chat_id, text=message_text)
                # self.active_messages.setdefault(chat_id, {})[new_msg.message_id] = True
                logger.info(f"Отправлено новое сообщение для {chat_id}")
                
        except Exception as e:
            if "message to edit not found" in str(e):
                logger.error(f"Ошибка редактирования: сообщение не найдено (возможно, удалено). ID: {msg_id}")
                # Удаляем из словаря активных сообщений, чтобы не пытаться редактировать снова
                if chat_id in self.active_messages and msg_id in self.active_messages[chat_id]:
                    del self.active_messages[chat_id][msg_id]
                # Здесь можно отправить новое сообщение вместо редактирования
            else:
                logger.error(f"Неизвестная ошибка отправки: {e}")

    async def run_cycle(self):
        """
        Основной цикл работы бота.
        """
        while True:
            try:
                now = datetime.now(timezone.utc)
                
                # Определяем диапазон для проверки (например, последние 2 дня, но сдвигаемся)
                # Ключевой момент исправления: мы используем last_check_time как начало,
                # а не жестко заданную дату из прошлого.
                start_time = self.last_check_time
                end_time = now
                
                # Защита от слишком больших интервалов (если бот был выключен долго)
                if (end_time - start_time) > timedelta(days=7):
                    start_time = end_time - timedelta(days=1)
                    logger.warning("Интервал слишком большой, сокращен до 1 дня.")

                events = await self.fetch_events(start_time, end_time)

                if events:
                    for event in events:
                        await self.send_or_update_notification(event)
                    
                    # Обновляем курсор времени ТОЛЬКО после успешной обработки
                    self.last_check_time = end_time
                    logger.info(f"Курсор времени обновлен до: {self.last_check_time}")
                else:
                    # Если событий нет, все равно немного сдвигаем время, чтобы не проверять одно и то же
                    # Или оставляем как есть, если API возвращает события строго по границам
                    pass

                # Пауза между циклами (чтобы не спамить API)
                await asyncio.sleep(60) 

            except Exception as e:
                logger.error(f"Критическая ошибка в цикле: {e}", exc_info=True)
                await asyncio.sleep(10) # Пауза перед повторной попыткой

async def main():
    bot = EventBot(token="YOUR_TELEGRAM_TOKEN")
    logger.info("Bot started (Fixed Version)")
    await bot.run_cycle()

if __name__ == "__main__":
    asyncio.run(main())