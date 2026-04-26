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
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.enums import ParseMode

# --- НАСТРОЙКИ И ВЕРСИЯ ---
BOT_VERSION = "1.1.0"  # Измените версию здесь при обновлении

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
YANDEX_LOGIN = os.getenv("YANDEX_LOGIN")
YANDEX_PASSWORD = os.getenv("YANDEX_APP_PASSWORD")
CALDAV_URL = os.getenv("CALDAV_URL", "https://caldav.yandex.ru/")

if not all([BOT_TOKEN, ADMIN_ID, YANDEX_LOGIN, YANDEX_PASSWORD]):
    raise ValueError("Ошибка: Проверьте .env. Убедитесь, что заполнен YANDEX_LOGIN (ваш email)!")

# --- ЛОГИРОВАНИЕ (Макс 300 КБ) ---
LOG_FILE = "bot.log"
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = RotatingFileHandler(LOG_FILE, maxBytes=300*1024, backupCount=5, encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# --- МАШИНА СОСТОЯНИЙ ДЛЯ ДОБАВЛЕНИЯ ЗАМЕТКИ ---
class AddNoteState(StatesGroup):
    waiting_for_text = State()
    waiting_for_time = State()

# --- РАБОТА С CALDAV ---
def get_calendar():
    try:
        client = caldav.DAVClient(url=CALDAV_URL, username=YANDEX_LOGIN, password=YANDEX_PASSWORD)
        principal = client.principal()
        calendars = principal.calendars()
        return calendars[0] if calendars else None
    except Exception as e:
        logger.error(f"CalDAV Error: {e}")
        return None

def get_events_range(start_date, end_date):
    calendar = get_calendar()
    if not calendar:
        return []
    try:
        events = calendar.date_search(start=start_date, end=end_date, expand=True)
        result = []
        for event in events:
            dt_start = event.instance.vevent.dtstart.value
            if hasattr(dt_start, 'date'):
                dt_start = datetime.combine(dt_start, datetime.min.time())
            
            summary = event.instance.vevent.summary.value if event.instance.vevent.summary else "Без названия"
            uid = event.instance.vevent.uid.value
            result.append({"summary": summary, "time": dt_start, "uid": uid})
        return result
    except Exception as e:
        logger.error(f"Error fetching events: {e}")
        return []

def delete_event(uid):
    calendar = get_calendar()
    if not calendar: return False
    try:
        ev = calendar.event_by_uid(uid)
        if ev:
            ev.delete()
            return True
    except Exception as e:
        logger.error(f"Delete error: {e}")
    return False

def create_event_in_yandex(summary, start_dt, duration_hours=1):
    calendar = get_calendar()
    if not calendar: return False
    try:
        end_dt = start_dt + timedelta(hours=duration_hours)
        # Формат iCal для CalDAV
        dt_str_start = start_dt.strftime('%Y%m%dT%H%M%SZ')
        dt_str_end = end_dt.strftime('%Y%m%dT%H%M%SZ')
        
        event_data = f"""BEGIN:VEVENT
SUMMARY:{summary}
DTSTART:{dt_str_start}
DTEND:{dt_str_end}
END:VEVENT"""
        calendar.save_event(event_data)
        return True
    except Exception as e:
        logger.error(f"Create error: {e}")
        return False

# --- КЛАВИАТУРЫ ---
def get_main_kb():
    builder = InlineKeyboardBuilder()
    builder.button(text=" Добавить новую заметку", callback_data="add_note_start")
    builder.button(text="✏️ Редактировать / Удалить", callback_data="edit_notes")
    return builder.as_markup()

def get_time_options_kb():
    builder = InlineKeyboardBuilder()
    now = datetime.now()
    
    # Варианты
    t1 = now + timedelta(hours=1)
    t2 = (now + timedelta(days=1)).replace(hour=9, minute=0)
    t3 = (now + timedelta(days=1)).replace(hour=18, minute=0)
    
    builder.button(text=f"⏰ Через 1 час ({t1.strftime('%H:%M')})", callback_data=f"time_{int(t1.timestamp())}")
    builder.button(text=f"🌅 Завтра утром ({t2.strftime('%d.%m %H:%M')})", callback_data=f"time_{int(t2.timestamp())}")
    builder.button(text=f"🌆 Завтра вечером ({t3.strftime('%d.%m %H:%M')})", callback_data=f"time_{int(t3.timestamp())}")
    builder.button(text="📅 Выбрать дату вручную", callback_data="time_manual")
    builder.adjust(1)
    return builder.as_markup()

def get_done_kb(uid):
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Выполнено (Удалить)", callback_data=f"done_{uid}")
    return builder.as_markup()

# --- ОБРАБОТЧИКИ ---

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    await state.clear()
    now = datetime.now()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = now.replace(hour=23, minute=59, second=59, microsecond=0)
    tomorrow_start = today_end + timedelta(seconds=1)
    tomorrow_end = tomorrow_start + timedelta(days=1)
    past_end = today_start - timedelta(seconds=1)

    # Сбор данных
    overdue = get_events_range(datetime(2020, 1, 1), past_end)
    today = get_events_range(today_start, today_end)
    tomorrow = get_events_range(tomorrow_start, tomorrow_end)

    msg = f"🤖 **Бот запущен! Версия: {BOT_VERSION}**\n\n"
    
    # Просроченные
    if overdue:
        msg += "🔴 **ПРОСРОЧЕНО:**\n"
        for ev in overdue:
            msg += f"• {ev['summary']} ({ev['time'].strftime('%d.%m %H:%M')})\n"
            # Отправляем отдельное сообщение с кнопкой удаления для каждого просроченного
            await message.answer(f"🔴 **ПРОСРОЧЕНО:** {ev['summary']}\nБыло: {ev['time'].strftime('%d.%m %H:%M')}", 
                                 reply_markup=get_done_kb(ev['uid']), parse_mode=ParseMode.MARKDOWN)
        msg += "\n"
    else:
        msg += " Просроченных задач нет.\n\n"

    # Сегодня
    msg += " **СЕГОДНЯ:**\n"
    if today:
        for ev in today:
            msg += f"• {ev['summary']} в {ev['time'].strftime('%H:%M')}\n"
    else:
        msg += "На сегодня нет заметок.\n"
    
    msg += "\n📆 **ЗАВТРА:**\n"
    if tomorrow:
        for ev in tomorrow:
            msg += f"• {ev['summary']} в {ev['time'].strftime('%H:%M')}\n"
    else:
        msg += "На завтра нет заметок."

    await message.answer(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=get_main_kb())
    logger.info(f"Start command processed. Version: {BOT_VERSION}")

@dp.callback_query(F.data == "add_note_start")
async def start_add_note(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("✍️ **Введите текст вашей новой заметки:**", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(AddNoteState.waiting_for_text)
    await callback.answer()

@dp.message(AddNoteState.waiting_for_text)
async def process_note_text(message: types.Message, state: FSMContext):
    await state.update_data(note_text=message.text)
    await message.answer(
        f"📝 Текст сохранен: *{message.text}*\n\n⏰ **Когда нужно оповестить?** Выберите вариант:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_time_options_kb()
    )
    await state.set_state(AddNoteState.waiting_for_time)

@dp.callback_query(AddNoteState.waiting_for_time, F.data.startswith("time_"))
async def process_time_selection(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    text = data.get("note_text")
    
    if callback.data == "time_manual":
        await callback.message.answer("Напишите дату и время в формате: ДД.ММ ГГ:ММ (например: 25.12 15:30)")
        # Здесь можно добавить еще одно состояние для ручного ввода даты, 
        # но для простоты пока оставим предустановленные кнопки как основные.
        await callback.answer("Функция ручного ввода в разработке, выберите готовый вариант.", show_alert=True)
        return

    timestamp = int(callback.data.split("_")[1])
    event_time = datetime.fromtimestamp(timestamp)
    
    if create_event_in_yandex(text, event_time):
        await callback.message.edit_text(
            f"✅ **Заметка добавлена!**\n📝 {text}\n⏰ На {event_time.strftime('%d.%m в %H:%M')}",
            reply_markup=get_main_kb(),
            parse_mode=ParseMode.MARKDOWN
        )
        logger.info(f"New event created: {text} at {event_time}")
    else:
        await callback.message.answer("❌ Ошибка при добавлении в календарь.")
    
    await state.clear()

@dp.callback_query(F.data == "edit_notes")
async def edit_notes(callback: types.CallbackQuery):
    await callback.message.answer("Для редактирования используйте кнопку 'Выполнено' у старых задач или создайте новую с правильным временем.", reply_markup=get_main_kb())
    await callback.answer()

@dp.callback_query(F.data.startswith("done_"))
async def mark_done(callback: types.CallbackQuery):
    uid = callback.data.split("_")[1]
    if delete_event(uid):
        await callback.message.edit_text("✅ Задача выполнена и удалена из календаря.", reply_markup=get_main_kb())
        await callback.answer("Удалено!")
    else:
        await callback.answer("Не удалось удалить.", show_alert=True)

# --- ПЛАНИРОВЩИК (ФОНОВЫЙ) ---
async def background_scheduler():
    while True:
        await asyncio.sleep(900) # 15 минут
        logger.info("Running scheduled check...")
        # Можно добавить логику отправки уведомлений, если бот был закрыт, 
        # но основной функционал теперь доступен по команде /start
        pass

async def main():
    logger.info(f"Bot started v{BOT_VERSION}")
    asyncio.create_task(background_scheduler())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())