import os
import asyncio
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from dotenv import load_dotenv
import caldav
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

# --- НАСТРОЙКИ И ВЕРСИЯ БОТА ---
# ВЕРСИЯ ХРАНИТСЯ ЗДЕСЬ. МЕНЯЙТЕ ЕЕ ПРИ КАЖДОМ ОБНОВЛЕНИИ КОДА.
BOT_VERSION = "1.0.0" 

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
YANDEX_LOGIN = os.getenv("YANDEX_LOGIN")
YANDEX_PASSWORD = os.getenv("YANDEX_APP_PASSWORD")
CALDAV_URL = os.getenv("CALDAV_URL", "https://caldav.yandex.ru/")

if not all([BOT_TOKEN, ADMIN_ID, YANDEX_LOGIN, YANDEX_PASSWORD]):
    raise ValueError("Ошибка: Проверьте файл .env. Заполнены ли BOT_TOKEN, ADMIN_ID, YANDEX_LOGIN и пароль?")

# --- НАСТРОЙКА ЛОГИРОВАНИЯ (МАКС 300 КБ) ---
LOG_FILE = "bot.log"
MAX_BYTES = 300 * 1024  # 300 КБ
BACKUP_COUNT = 5        # Количество архивных файлов

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = RotatingFileHandler(
    LOG_FILE, 
    maxBytes=MAX_BYTES, 
    backupCount=BACKUP_COUNT, 
    encoding='utf-8'
)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Инициализация бота
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- РАБОТА С CALDAV (Яндекс Календарь) ---
def get_calendar():
    try:
        client = caldav.DAVClient(
            url=CALDAV_URL,
            username=YANDEX_LOGIN,
            password=YANDEX_PASSWORD
        )
        principal = client.principal()
        calendars = principal.calendars()
        if not calendars:
            logger.error("Календари не найдены.")
            return None
        return calendars[0]
    except Exception as e:
        logger.error(f"Ошибка подключения CalDAV: {e}")
        return None

def get_events_data():
    calendar = get_calendar()
    if not calendar:
        return [], []

    now = datetime.now()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = now.replace(hour=23, minute=59, second=59, microsecond=0)
    
    # Ищем события за последний год, чтобы найти просроченные
    start_search = now - timedelta(days=365)
    
    events = calendar.date_search(start=start_search, end=today_end, expand=True)
    
    today_events = []
    overdue_events = []

    for event in events:
        dt_start = event.instance.vevent.dtstart.value
        if hasattr(dt_start, 'date'):
            dt_start = datetime.combine(dt_start, datetime.min.time())
        
        summary = event.instance.vevent.summary.value if event.instance.vevent.summary else "Без названия"
        uid = event.instance.vevent.uid.value
        
        # Если событие сегодня
        if dt_start.date() == today_start.date():
            today_events.append({"summary": summary, "time": dt_start.strftime("%H:%M"), "uid": uid, "obj": event})
        # Если событие просрочено (дата прошла, но оно еще висит)
        elif dt_start < today_start:
            overdue_events.append({"summary": summary, "time": dt_start.strftime("%d.%m %H:%M"), "uid": uid, "obj": event})

    return today_events, overdue_events

def delete_event_from_calendar(uid):
    calendar = get_calendar()
    if not calendar:
        return False
    
    try:
        event_to_delete = calendar.event_by_uid(uid)
        if event_to_delete:
            event_to_delete.delete()
            logger.info(f"Событие удалено: {uid}")
            return True
        return False
    except Exception as e:
        logger.error(f"Ошибка удаления {uid}: {e}")
        return False

# --- КЛАВИАТУРЫ ---
def get_main_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Добавить новую заметку", callback_data="add_note")
    builder.button(text="✏️ Редактировать заметки", callback_data="edit_notes")
    return builder.as_markup()

def get_done_keyboard(uid):
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Выполнено (Удалить)", callback_data=f"done_{uid}")
    return builder.as_markup()

# --- ОБРАБОТЧИКИ ---

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("Доступ запрещен.")
        return
    
    text = (
        f" **Бот запущен успешно!**\n"
        f" **Версия:** `{BOT_VERSION}`\n"
        f"🔄 Обновление событий каждые 15 мин.\n"
        f"📅 Синхронизация с Яндекс Календарем активна."
    )
    await message.answer(text, parse_mode="Markdown", reply_markup=get_main_keyboard())
    logger.info(f"Бот запущен. Версия: {BOT_VERSION}")

@dp.callback_query(F.data == "add_note")
async def add_note_callback(callback: types.CallbackQuery):
    await callback.message.answer(
        "Напишите текст заметки и время (например: 'Сдать отчет завтра в 12:00').\n"
        "Я добавлю её в календарь.",
        reply_markup=None # Временно скрываем кнопки для ввода текста
    )
    await callback.answer()

@dp.callback_query(F.data == "edit_notes")
async def edit_notes_callback(callback: types.CallbackQuery):
    await callback.message.answer(
        "Режим редактирования:\n"
        "Чтобы изменить задачу, сначала отметьте её как выполненную (удалите), а затем добавьте новую с правильными данными.\n"
        "(Полный функционал редактирования будет добавлен в следующей версии).",
        reply_markup=get_main_keyboard()
    )
    await callback.answer()

@dp.message()
async def handle_new_note(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return
    
    # Простая логика добавления: создаем событие на сейчас + 1 час, если дата не распознана сложным парсером
    # Для полноценного парсинга дат из текста нужен отдельный модуль, здесь базовый пример
    calendar = get_calendar()
    if calendar:
        try:
            new_time = datetime.now() + timedelta(hours=1)
            event_data = f"""BEGIN:VEVENT
SUMMARY:{message.text}
DTSTART:{new_time.strftime('%Y%m%dT%H%M%SZ')}
DTEND:{(new_time + timedelta(hours=1)).strftime('%Y%m%dT%H%M%SZ')}
END:VEVENT"""
            calendar.save_event(event_data)
            await message.answer("✅ Заметка добавлена в календарь!", reply_markup=get_main_keyboard())
            logger.info(f"Добавлена заметка: {message.text}")
        except Exception as e:
            await message.answer(f"❌ Ошибка: {e}", reply_markup=get_main_keyboard())
            logger.error(f"Ошибка создания: {e}")
    else:
        await message.answer("❌ Нет связи с календарем.", reply_markup=get_main_keyboard())

@dp.callback_query(F.data.startswith("done_"))
async def mark_done_callback(callback: types.CallbackQuery):
    uid = callback.data.split("_")[1]
    
    if delete_event_from_calendar(uid):
        await callback.message.edit_text(
            f"✅ Задача выполнена и удалена из календаря.",
            reply_markup=get_main_keyboard()
        )
        await callback.answer("Задача удалена!")
    else:
        await callback.answer("Не удалось удалить задачу.", show_alert=True)

# --- ПЛАНИРОВЩИК (15 МИНУТ) ---
async def scheduled_notify():
    while True:
        try:
            await asyncio.sleep(900)  # 15 минут
            
            today_ev, overdue_ev = get_events_data()
            
            if not today_ev and not overdue_ev:
                logger.info("Нет новых или просроченных событий.")
                continue

            # Отправка просроченных (каждая отдельно с кнопкой удаления)
            if overdue_ev:
                for ev in overdue_ev:
                    text = f"️ **ПРОСРОЧЕНО:**\n{ev['summary']}\n🕒 Было: {ev['time']}"
                    await bot.send_message(ADMIN_ID, text, parse_mode="Markdown", reply_markup=get_done_keyboard(ev['uid']))
            
            # Отправка сегодняшних (одним списком)
            if today_ev:
                text = "📅 **События на сегодня:**\n"
                for ev in today_ev:
                    text += f"• {ev['summary']} в {ev['time']}\n"
                await bot.send_message(ADMIN_ID, text, parse_mode="Markdown", reply_markup=get_main_keyboard())
                
            logger.info("Плановая рассылка завершена.")
            
        except Exception as e:
            logger.error(f"Критическая ошибка в планировщике: {e}")

async def main():
    logger.info(f"Старт бота версии {BOT_VERSION}")
    asyncio.create_task(scheduled_notify())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())