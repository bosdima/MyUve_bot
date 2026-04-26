import os
import asyncio
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from dotenv import load_dotenv
import caldav
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.enums import ParseMode

# --- НАСТРОЙКИ И ВЕРСИЯ ---
BOT_VERSION = "1.1.1"  # Обновленная версия

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
YANDEX_LOGIN = os.getenv("YANDEX_LOGIN")
YANDEX_PASSWORD = os.getenv("YANDEX_APP_PASSWORD")
CALDAV_URL = os.getenv("CALDAV_URL", "https://caldav.yandex.ru/")

# Чтение интервала из .env (по умолчанию 15)
try:
    CHECK_INTERVAL_MINUTES = int(os.getenv("CHECK_INTERVAL_MINUTES", 15))
except ValueError:
    CHECK_INTERVAL_MINUTES = 15

if not all([BOT_TOKEN, ADMIN_ID, YANDEX_LOGIN, YANDEX_PASSWORD]):
    raise ValueError("Ошибка: Проверьте .env. Убедитесь, что заполнен YANDEX_LOGIN!")

# --- ЛОГИРОВАНИЕ ---
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

# --- МАШИНА СОСТОЯНИЙ ---
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
            # Корректная обработка даты и времени
            if hasattr(dt_start, 'date'):
                # Если это дата без времени, ставим 00:00
                dt_start = datetime.combine(dt_start, datetime.min.time())
            
            summary = event.instance.vevent.summary.value if event.instance.vevent.summary else "Без названия"
            uid = event.instance.vevent.uid.value
            result.append({"summary": summary, "time": dt_start, "uid": uid})
        # Сортировка по времени
        result.sort(key=lambda x: x['time'])
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

# Reply-клавиатура (всегда внизу)
def get_reply_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="➕ Добавить заметку"), KeyboardButton(text="️ Настройки"))
    return builder.as_markup(resize_keyboard=True)

# Inline для выбора времени
def get_time_options_kb():
    builder = InlineKeyboardBuilder()
    now = datetime.now()
    
    t1 = now + timedelta(hours=1)
    t2 = (now + timedelta(days=1)).replace(hour=9, minute=0)
    t3 = (now + timedelta(days=1)).replace(hour=18, minute=0)
    
    builder.button(text=f"⏰ Через 1 час ({t1.strftime('%H:%M')})", callback_data=f"time_{int(t1.timestamp())}")
    builder.button(text=f"🌅 Завтра утром ({t2.strftime('%d.%m %H:%M')})", callback_data=f"time_{int(t2.timestamp())}")
    builder.button(text=f"🌆 Завтра вечером ({t3.strftime('%d.%m %H:%M')})", callback_data=f"time_{int(t3.timestamp())}")
    builder.button(text="📅 Выбрать дату вручную", callback_data="time_manual")
    builder.adjust(1)
    return builder.as_markup()

# Inline для удаления
def get_done_kb(uid):
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Выполнено (Удалить)", callback_data=f"done_{uid}")
    return builder.as_markup()

# Inline для настроек
def get_settings_kb(current_interval):
    builder = InlineKeyboardBuilder()
    intervals = [5, 15, 30, 60]
    for mins in intervals:
        text = f"{mins} мин" + (" ✅" if mins == current_interval else "")
        builder.button(text=text, callback_data=f"set_interval_{mins}")
    builder.adjust(2)
    return builder.as_markup()

# --- ОБРАБОТЧИКИ ---

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    await state.clear()
    now = datetime.now()
    
    # Границы времени
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
    
    # 1. Просроченные
    if overdue:
        msg += " **ПРОСРОЧЕНО:**\n"
        for ev in overdue:
            # Формат: День недели, Дата, Время
            date_str = ev['time'].strftime("%A, %d.%m в %H:%M").replace("Monday", "Понедельник").replace("Tuesday", "Вторник").replace("Wednesday", "Среда").replace("Thursday", "Четверг").replace("Friday", "Пятница").replace("Saturday", "Суббота").replace("Sunday", "Воскресенье")
            msg += f"• {ev['summary']} ({date_str})\n"
            
            # Отправляем отдельное сообщение с кнопкой удаления
            await message.answer(f"🔴 **ПРОСРОЧЕНО:** {ev['summary']}\n⏰ Было: {date_str}", 
                                 reply_markup=get_done_kb(ev['uid']), parse_mode=ParseMode.MARKDOWN)
        msg += "\n"
    else:
        msg += " Просроченных задач нет.\n\n"

    # 2. Сегодня
    day_name_today = today_start.strftime("%A").replace("Monday", "Понедельник").replace("Tuesday", "Вторник").replace("Wednesday", "Среда").replace("Thursday", "Четверг").replace("Friday", "Пятница").replace("Saturday", "Суббота").replace("Sunday", "Воскресенье")
    msg += f" **СЕГОДНЯ ({day_name_today}, {today_start.strftime('%d.%m')}):**\n"
    if today:
        for ev in today:
            time_str = ev['time'].strftime("%H:%M")
            msg += f"• {ev['summary']} в {time_str}\n"
    else:
        msg += "На сегодня нет заметок.\n"
    
    msg += "\n"

    # 3. Завтра
    day_name_tomorrow = tomorrow_start.strftime("%A").replace("Monday", "Понедельник").replace("Tuesday", "Вторник").replace("Wednesday", "Среда").replace("Thursday", "Четверг").replace("Friday", "Пятница").replace("Saturday", "Суббота").replace("Sunday", "Воскресенье")
    msg += f" **ЗАВТРА ({day_name_tomorrow}, {tomorrow_start.strftime('%d.%m')}):**\n"
    if tomorrow:
        for ev in tomorrow:
            time_str = ev['time'].strftime("%H:%M")
            msg += f"• {ev['summary']} в {time_str}\n"
    else:
        msg += "На завтра нет заметок."

    await message.answer(msg, parse_mode=ParseMode.MARKDOWN, reply_markup=get_reply_keyboard())
    logger.info(f"Start command processed. Version: {BOT_VERSION}")

@dp.message(F.text == "➕ Добавить заметку")
async def start_add_note(message: types.Message, state: FSMContext):
    await message.answer("✍️ **Введите текст вашей новой заметки:**", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(AddNoteState.waiting_for_text)

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
        await callback.message.answer("Функция ручного ввода даты пока в разработке. Пожалуйста, выберите один из предложенных вариантов выше.", show_alert=True)
        return

    timestamp = int(callback.data.split("_")[1])
    event_time = datetime.fromtimestamp(timestamp)
    
    if create_event_in_yandex(text, event_time):
        date_str = event_time.strftime("%A, %d.%m в %H:%M").replace("Monday", "Понедельник").replace("Tuesday", "Вторник").replace("Wednesday", "Среда").replace("Thursday", "Четверг").replace("Friday", "Пятница").replace("Saturday", "Суббота").replace("Sunday", "Воскресенье")
        
        await callback.message.edit_text(
            f"✅ **Заметка добавлена!**\n {text}\n⏰ На: {date_str}",
            reply_markup=get_reply_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
        logger.info(f"New event created: {text} at {event_time}")
    else:
        await callback.message.answer("❌ Ошибка при добавлении в календарь.", reply_markup=get_reply_keyboard())
    
    await state.clear()

@dp.message(F.text == "⚙️ Настройки")
async def open_settings(message: types.Message):
    global CHECK_INTERVAL_MINUTES
    text = f"⚙️ **Настройки бота**\n\nТекущий интервал обновления: **{CHECK_INTERVAL_MINUTES} мин**\nВыберите новый интервал:"
    await message.answer(text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_settings_kb(CHECK_INTERVAL_MINUTES))

@dp.callback_query(F.data.startswith("set_interval_"))
async def change_interval(callback: types.CallbackQuery):
    global CHECK_INTERVAL_MINUTES
    new_interval = int(callback.data.split("_")[2])
    CHECK_INTERVAL_MINUTES = new_interval
    
    # Сохраняем в .env файл (опционально, чтобы перезапуск сохранял настройку)
    # Для простоты меняем только в памяти, но логируем
    logger.info(f"Интервал изменен на {new_interval} мин")
    
    await callback.message.edit_text(
        f"✅ Интервал обновлен: **{new_interval} мин**\nБот будет проверять календарь чаще.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_reply_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "edit_notes")
async def edit_notes_placeholder(callback: types.CallbackQuery):
    await callback.message.answer("Для редактирования удалите старую задачу (кнопка 'Выполнено') и создайте новую.", reply_markup=get_reply_keyboard())
    await callback.answer()

@dp.callback_query(F.data.startswith("done_"))
async def mark_done(callback: types.CallbackQuery):
    uid = callback.data.split("_")[1]
    if delete_event(uid):
        await callback.message.edit_text("✅ Задача выполнена и удалена из календаря.", reply_markup=get_reply_keyboard())
        await callback.answer("Удалено!")
    else:
        await callback.answer("Не удалось удалить.", show_alert=True)

# --- ПЛАНИРОВЩИК (ДИНАМИЧЕСКИЙ ИНТЕРВАЛ) ---
async def background_scheduler():
    while True:
        interval_seconds = CHECK_INTERVAL_MINUTES * 60
        logger.info(f"Sleeping for {CHECK_INTERVAL_MINUTES} minutes...")
        await asyncio.sleep(interval_seconds)
        logger.info("Running scheduled check...")
        # Здесь можно добавить логику автоматической отправки уведомлений без команды /start
        # Но основной функционал сейчас работает по запросу /start

async def main():
    logger.info(f"Bot started v{BOT_VERSION}")
    asyncio.create_task(background_scheduler())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())