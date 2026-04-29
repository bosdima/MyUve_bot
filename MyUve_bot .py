import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.exceptions import TelegramBadRequest

# Конфигурация
BOT_TOKEN = "ВАШ_ТОКЕН_БОТА"
DB_NAME = "bot_notifications.db"
TIMEZONE = timezone(timedelta(hours=3))  # Москва +3

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- Работа с Базой Данных ---

def init_db():
    """Инициализация базы данных для хранения ID последних сообщений"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    # Таблица хранит: event_id (уникальный идентификатор события) и last_message_id
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sent_notifications (
            event_id TEXT PRIMARY KEY,
            last_message_id INTEGER,
            chat_id INTEGER,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def get_last_message(event_id: str) -> Optional[int]:
    """Получить ID последнего отправленного сообщения для события"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT last_message_id FROM sent_notifications WHERE event_id = ?", (event_id,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

def save_message(event_id: str, message_id: int, chat_id: int):
    """Сохранить или обновить ID сообщения для события"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO sent_notifications (event_id, last_message_id, chat_id, updated_at)
        VALUES (?, ?, ?, ?)
    ''', (event_id, message_id, chat_id, datetime.now()))
    conn.commit()
    conn.close()

def delete_notification_record(event_id: str):
    """Удалить запись о сообщении (если событие завершено или отменено)"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM sent_notifications WHERE event_id = ?", (event_id,))
    conn.commit()
    conn.close()

# --- Логика загрузки событий (симуляция ваших логов) ---

def load_events_from_logs(start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
    """
    Симуляция парсинга логов. 
    В реальном проекте здесь будет чтение файла bot.txt/bot1.txt и парсинг строк.
    Возвращает список событий со структурой:
    {
        'id': 'unique_event_uuid_or_title',
        'title': 'Купить Акции',
        'scheduled_time': datetime объект времени события,
        'is_completed': False
    }
    """
    events = []
    
    # Пример жестко заданного события из вашего запроса для демонстрации
    # В реальности вы будете парсить строки вида: "Загрузка событий с ... по ..." и искать конкретные задачи
    event_data = {
        'id': 'buy_stocks_28_04_10_00', # Уникальный ключ
        'title': 'Купить Акции',
        'scheduled_time': datetime(2026, 4, 28, 10, 0, tzinfo=TIMEZONE),
        'is_completed': False
    }
    
    # Проверяем, попадает ли событие в диапазон загрузки
    if start_date <= event_data['scheduled_time'] < end_date:
        events.append(event_data)
        
    # Здесь должен быть ваш реальный код парсинга файлов bot.txt и bot1.txt
    # logger.info(f"Загрузка событий с {start_date} по {end_date}")
    
    return events

# --- Основная логика уведомлений ---

async def process_notifications(chat_id: int):
    """Основной цикл проверки событий и отправки уведомлений"""
    now = datetime.now(TIMEZONE)
    
    # Определяем диапазон загрузки (например, сегодня и завтра, как в логах)
    start_range = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_range = start_range + timedelta(days=2)
    
    logger.info(f"Проверка событий с {start_range} по {end_range}")
    
    events = load_events_from_logs(start_range, end_range)
    
    for event in events:
        if event['is_completed']:
            continue
            
        event_id = event['id']
        event_time = event['scheduled_time']
        
        # Логика: если текущее время >= времени события и минуты совпадают (или прошло менее часа)
        # Для теста сделаем так: если сейчас время события или позже, но событие еще не отмечено выполненным
        # В вашем случае уведомления приходят каждый час. Значит, проверяем:
        # 1. Время события наступило.
        # 2. Прошло меньше 24 часов (или другой лимит), чтобы не спамить вечно.
        
        if now >= event_time:
            # Вычисляем, сколько часов прошло с момента события
            hours_passed = (now - event_time).seconds // 3600 + (now - event_time).days * 24
            
            # Ограничим повторения, например, до 24 часов или пока пользователь не отметит выполнение
            if hours_passed < 24: 
                await send_or_update_reminder(chat_id, event, event_id, hours_passed)

async def send_or_update_reminder(chat_id: int, event: Dict, event_id: str, hours_passed: int):
    last_msg_id = get_last_message(event_id)
    
    # Формируем текст
    prefix = ""
    if hours_passed > 0:
        prefix = "(Повторное уведомление) "
    
    text = (
        f"{prefix}Напоминание: {event['title']}\n"
        f"Время: {event['scheduled_time'].strftime('%H:%M')}\n"
        f"Прошло часов: {hours_passed}"
    )
    
    # 1. Если есть старое сообщение - удаляем его
    if last_msg_id:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=last_msg_id)
            logger.info(f"Удалено старое сообщение {last_msg_id} для события {event_id}")
        except TelegramBadRequest as e:
            logger.warning(f"Не удалось удалить сообщение {last_msg_id}: {e}")
            # Если сообщение уже удалено пользователем или ошибочно, продолжаем
    
    # 2. Отправляем новое сообщение
    try:
        new_msg = await bot.send_message(chat_id=chat_id, text=text)
        save_message(event_id, new_msg.message_id, chat_id)
        logger.info(f"Отправлено новое уведомление (ID: {new_msg.message_id}) для {event_id}")
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения: {e}")

# --- Запуск периодической задачи ---

async def scheduler():
    """Запускает проверку каждые 60 минут (или чаще для тестов)"""
    while True:
        try:
            # Замените chat_id на ваш реальный ID или получите из базы пользователей
            target_chat_id = 123456789  # ЗАМЕНИТЕ НА ВАШ CHAT_ID
            await process_notifications(target_chat_id)
        except Exception as e:
            logger.error(f"Ошибка в планировщике: {e}")
        
        # Ждем 1 час (3600 секунд). Для тестов можно поставить 60 секунд.
        await asyncio.sleep(3600) 

# --- Handlers (для запуска и управления) ---

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer("Бот запущен. Уведомления будут приходить согласно расписанию.")
    # Можно добавить пользователя в базу для рассылки

@dp.message(Command("test"))
async def cmd_test(message: Message):
    """Команда для мгновенного теста логики без ожидания часа"""
    await message.answer("Запуск тестовой проверки уведомлений...")
    await process_notifications(message.chat.id)
    await message.answer("Проверка завершена.")

async def main():
    init_db()
    logger.info("Bot started v1.3.6 (Fixed Repeating Notifications)")
    
    # Запускаем планировщик в фоне
    asyncio.create_task(scheduler())
    
    # Запускаем поллинг бота
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())