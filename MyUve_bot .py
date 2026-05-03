import asyncio
import logging
from datetime import datetime, timedelta, timezone
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.methods import DeleteMessage

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Токен бота (замените на ваш)
BOT_TOKEN = "YOUR_BOT_TOKEN_HERE"

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- ГЛАВНОЕ ИСПРАВЛЕНИЕ: Явный хендлер для /start ---
@dp.message(Command("start"))
async def cmd_start(message: Message):
    logger.info(f"Получена команда /start от пользователя {message.from_user.id}")
    await message.answer(
        "Привет! Бот запущен и работает.\n"
        "Я могу загружать события и отправлять уведомления."
    )

# --- Фоновая задача загрузки событий (чтобы не блокировать бота) ---
async def background_event_loader(bot: Bot):
    """
    Эта функция имитирует вашу логику 'Загрузка событий'.
    Важно: она должна работать в отдельном таске и не блокировать цикл событий.
    """
    logger.info("Фоновая загрузка событий запущена")
    
    # Пример диапазона дат из ваших логов
    start_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = start_date + timedelta(days=2)
    
    while True:
        try:
            # Имитация тяжелой работы (загрузка из БД или API)
            # В вашем оригинальном коде это, вероятно, синхронный запрос, который блокирует всё.
            # Здесь мы делаем это асинхронно или в executor'е, если библиотека синхронная.
            
            logger.info(f"Загрузка событий с {start_date} по {end_date}")
            
            # ЭМУЛЯЦИЯ ЗАДЕРЖКИ (замените на реальный код запроса)
            await asyncio.sleep(60) # Ждем 60 секунд перед следующей проверкой
            
            # Сдвигаем даты для следующего цикла, если нужно
            # start_date = end_date
            # end_date = start_date + timedelta(days=2)
            
        except Exception as e:
            logger.error(f"Ошибка в фоновой загрузке: {e}")
            await asyncio.sleep(5)

# --- Запуск бота ---
async def main():
    logger.info("Bot started")
    
    # Запускаем фоновую задачу загрузки событий параллельно с ботом
    loader_task = asyncio.create_task(background_event_loader(bot))
    
    try:
        # Запускаем поллинг
        await dp.start_polling(bot)
    finally:
        # Корректная остановка при выходе
        loader_task.cancel()
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")