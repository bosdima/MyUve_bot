import os
import asyncio
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import caldav
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.enums import ParseMode

# --- НАСТРОЙКИ И ВЕРСИЯ ---
BOT_VERSION = "1.3.1"

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
YANDEX_LOGIN = os.getenv("YANDEX_LOGIN")
YANDEX_PASSWORD = os.getenv("YANDEX_APP_PASSWORD")
CALDAV_URL = os.getenv("CALDAV_URL", "https://caldav.yandex.ru/")

try:
    CHECK_INTERVAL_MINUTES = int(os.getenv("CHECK_INTERVAL_MINUTES", 15))
except ValueError:
    CHECK_INTERVAL_MINUTES = 15

if not all([BOT_TOKEN, ADMIN_ID, YANDEX_LOGIN, YANDEX_PASSWORD]):
    raise ValueError("Ошибка: Проверьте .env! Убедитесь, что заполнены BOT_TOKEN, ADMIN_ID, YANDEX_LOGIN и пароль.")

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

# Глобальные переменные состояния
MAIN_MESSAGE_ID = None
CURRENT_WEEK_START = None

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

def get_local_time():
    return datetime.now().astimezone()

def get_week_start(date_obj):
    return date_obj - timedelta(days=date_obj.weekday())

def format_date_full(dt_obj):
    if dt_obj is None: return ""
    local_dt = dt_obj.astimezone() if hasattr(dt_obj, 'tzinfo') else dt_obj
    day_name = local_dt.strftime("%A").replace("Monday", "Пн").replace("Tuesday", "Вт").replace("Wednesday", "Ср").replace("Thursday", "Чт").replace("Friday", "Пт").replace("Saturday", "Сб").replace("Sunday", "Вс")
    return f"{day_name}, {local_dt.strftime('%d.%m')}"

def format_time_only(dt_obj):
    if dt_obj is None: return "--:--"
    local_dt = dt_obj.astimezone() if hasattr(dt_obj, 'tzinfo') else dt_obj
    return local_dt.strftime("%H:%M")

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

def get_events_for_week(start_date, end_date):
    calendar = get_calendar()
    if not calendar:
        return []
    
    logger.info(f"Загрузка событий с {start_date} по {end_date}")
    try:
        events = calendar.date_search(start=start_date, end=end_date, expand=True)
        result = []
        
        for event in events:
            try:
                dt_start = event.instance.vevent.dtstart.value
                summary = event.instance.vevent.summary.value if event.instance.vevent.summary else "Без названия"
                uid = event.instance.vevent.uid.value
                
                # Нормализация времени
                if hasattr(dt_start, 'date') and not hasattr(dt_start, 'hour'):
                    dt_start = datetime.combine(dt_start, datetime.min.time()).replace(tzinfo=timezone.utc)
                elif hasattr(dt_start, 'tzinfo') and dt_start.tzinfo is None:
                    dt_start = dt_start.replace(tzinfo=timezone.utc)
                
                local_dt = dt_start.astimezone()
                
                result.append({
                    "summary": summary,
                    "time": local_dt,
                    "uid": uid,
                    "is_overdue": local_dt < get_local_time()
                })
            except Exception as e:
                logger.warning(f"Ошибка парсинга: {e}")
                continue
        
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
            logger.info(f"Deleted: {uid}")
            return True
    except Exception as e:
        logger.error(f"Delete error: {e}")
    return False

def create_event_in_yandex(summary, start_dt, duration_hours=1):
    calendar = get_calendar()
    if not calendar: return False
    try:
        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=timezone.utc)
        else:
            start_dt = start_dt.astimezone(timezone.utc)
            
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

def get_reply_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="➕ Добавить заметку"), KeyboardButton(text="⚙️ Настройки"))
    return builder.as_markup(resize_keyboard=True)

def get_main_nav_keyboard(week_start):
    week_end = week_start + timedelta(days=6)
    builder = InlineKeyboardBuilder()
    
    prev_week = week_start - timedelta(days=7)
    next_week = week_start + timedelta(days=7)
    
    builder.button(text="️ Назад", callback_data=f"nav_prev_{int(prev_week.timestamp())}")
    builder.button(text=f"📅 {week_start.strftime('%d.%m')} - {week_end.strftime('%d.%m')}", callback_data="current_week")
    builder.button(text="Вперед ➡️", callback_data=f"nav_next_{int(next_week.timestamp())}")
    
    # Убраны кнопки "Добавить" и "Настройки", так как они есть в Reply-клавиатуре
    builder.row(InlineKeyboardButton(text="️ Управление (Удалить)", callback_data="manage_list"))
    builder.row(InlineKeyboardButton(text=" Обновить", callback_data="force_refresh"))
    
    return builder.as_markup()

def get_manage_list_keyboard(events):
    if not events:
        builder = InlineKeyboardBuilder()
        builder.button(text="🔙 Закрыть", callback_data="close_manage")
        return builder.as_markup()

    builder = InlineKeyboardBuilder()
    for ev in events:
        date_str = format_date_full(ev['time'])
        time_str = format_time_only(ev['time'])
        btn_text = f"❌ {ev['summary']} ({date_str} {time_str})"
        builder.button(text=btn_text, callback_data=f"del_{ev['uid']}")
    
    builder.adjust(1)
    builder.row(InlineKeyboardButton(text="🔙 Закрыть список", callback_data="close_manage"))
    return builder.as_markup()

def get_notification_keyboard(uid):
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Выполнено (Удалить)", callback_data=f"done_notify_{uid}")
    builder.button(text="Напомнить позже", callback_data=f"snooze_{uid}")
    return builder.as_markup()

def get_time_options_kb():
    builder = InlineKeyboardBuilder()
    now = get_local_time()
    t1 = now + timedelta(hours=1)
    t2 = (now + timedelta(days=1)).replace(hour=9, minute=0)
    t3 = (now + timedelta(days=1)).replace(hour=18, minute=0)
    
    builder.button(text=f"Через 1 час ({t1.strftime('%H:%M')})", callback_data=f"time_{int(t1.timestamp())}")
    builder.button(text=f"Завтра утром ({t2.strftime('%d.%m %H:%M')})", callback_data=f"time_{int(t2.timestamp())}")
    builder.button(text=f"Завтра вечером ({t3.strftime('%d.%m %H:%M')})", callback_data=f"time_{int(t3.timestamp())}")
    builder.button(text="Отмена", callback_data="cancel_add")
    builder.adjust(1)
    return builder.as_markup()

def get_settings_kb(current_interval):
    builder = InlineKeyboardBuilder()
    for mins in [5, 15, 30, 60]:
        text = f"{mins} мин" + (" ✅" if mins == current_interval else "")
        builder.button(text=text, callback_data=f"set_interval_{mins}")
    builder.button(text="🔙 Назад", callback_data="close_settings")
    builder.adjust(2)
    return builder.as_markup()

# --- ОСНОВНАЯ ЛОГИКА ОТОБРАЖЕНИЯ ---

async def build_week_report(week_start):
    global CURRENT_WEEK_START
    CURRENT_WEEK_START = week_start
    
    week_end = week_start + timedelta(days=6, hours=23, minutes=59, seconds=59)
    events = get_events_for_week(week_start, week_end)
    
    now = get_local_time()
    # Исправлено время синхронизации на локальное
    sync_time = now.strftime("%d.%m.%Y %H:%M:%S")
    
    text = f"🤖 **Бот запущен! Версия: {BOT_VERSION}**\n"
    text += f"_Период: {format_date_full(week_start)} — {format_date_full(week_end)}_\n\n"
    
    if not events:
        text += "Нет событий на эту неделю."
    else:
        for ev in events:
            date_str = format_date_full(ev['time'])
            time_str = format_time_only(ev['time'])
            
            status_icon = ""
            color_mark = ""
            
            if ev['time'] < now:
                status_icon = " ⚠️"
                color_mark = "" 
            elif ev['time'].date() == now.date():
                status_icon = " 🔁"
                color_mark = "" 
            else:
                status_icon = ""
                color_mark = "" 
            
            text += f"{color_mark} {time_str} — {ev['summary']} ({date_str}){status_icon}\n"
    
    text += f"\n_Последняя синхронизация: {sync_time}_"
    return text, get_main_nav_keyboard(week_start)

async def send_or_edit_main_message(message=None):
    global MAIN_MESSAGE_ID, CURRENT_WEEK_START
    
    if CURRENT_WEEK_START is None:
        CURRENT_WEEK_START = get_week_start(get_local_time())
    
    text, keyboard = await build_week_report(CURRENT_WEEK_START)
    
    try:
        if MAIN_MESSAGE_ID is None:
            if message:
                sent_msg = await message.answer(text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard, reply_markup=get_reply_keyboard())
                MAIN_MESSAGE_ID = sent_msg.message_id
            else:
                sent_msg = await bot.send_message(ADMIN_ID, text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard, reply_markup=get_reply_keyboard())
                MAIN_MESSAGE_ID = sent_msg.message_id
        else:
            await bot.edit_message_text(
                chat_id=ADMIN_ID,
                message_id=MAIN_MESSAGE_ID,
                text=text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=keyboard
            )
            # Обновляем Reply-клавиатуру отдельно, если нужно
            await bot.edit_message_reply_markup(
                chat_id=ADMIN_ID,
                message_id=MAIN_MESSAGE_ID,
                reply_markup=get_reply_keyboard()
            )
    except Exception as e:
        logger.error(f"Edit error: {e}")
        if "message to edit not found" in str(e):
            MAIN_MESSAGE_ID = None

# --- ОБРАБОТЧИКИ ---

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    await state.clear()
    await send_or_edit_main_message(message)

@dp.callback_query(F.data.startswith("nav_prev_"))
async def nav_prev(callback: types.CallbackQuery):
    ts = int(callback.data.split("_")[2])
    new_start = datetime.fromtimestamp(ts).replace(tzinfo=timezone.utc).astimezone()
    await callback.answer()
    global CURRENT_WEEK_START
    CURRENT_WEEK_START = new_start
    await send_or_edit_main_message()

@dp.callback_query(F.data.startswith("nav_next_"))
async def nav_next(callback: types.CallbackQuery):
    ts = int(callback.data.split("_")[2])
    new_start = datetime.fromtimestamp(ts).replace(tzinfo=timezone.utc).astimezone()
    await callback.answer()
    global CURRENT_WEEK_START
    CURRENT_WEEK_START = new_start
    await send_or_edit_main_message()

@dp.callback_query(F.data == "force_refresh")
async def force_refresh(callback: types.CallbackQuery):
    await callback.answer("Обновление...")
    await send_or_edit_main_message()

@dp.callback_query(F.data == "manage_list")
async def show_manage_list(callback: types.CallbackQuery):
    events = get_events_for_week(CURRENT_WEEK_START, CURRENT_WEEK_START + timedelta(days=6))
    kb = get_manage_list_keyboard(events)
    if events:
        await callback.message.answer("Выберите задачу для удаления:", reply_markup=kb)
    else:
        await callback.message.answer("Нет задач для удаления в этой неделе.", reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data.startswith("del_"))
async def delete_from_list(callback: types.CallbackQuery):
    uid = callback.data.split("_")[1]
    if delete_event(uid):
        await callback.message.edit_text("✅ Задача удалена.", reply_markup=None)
        await send_or_edit_main_message()
    else:
        await callback.answer("Ошибка удаления", show_alert=True)

@dp.callback_query(F.data == "close_manage")
async def close_manage(callback: types.CallbackQuery):
    await callback.message.delete()
    await callback.answer()

@dp.message(F.text == "➕ Добавить заметку")
async def start_add_note(message: types.Message, state: FSMContext):
    await message.answer("✍️ Введите текст новой заметки:", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(AddNoteState.waiting_for_text)

class AddNoteState(StatesGroup):
    waiting_for_text = State()
    waiting_for_time = State()

@dp.message(AddNoteState.waiting_for_text)
async def process_note_text(message: types.Message, state: FSMContext):
    await state.update_data(note_text=message.text)
    await message.answer(f" Текст: {message.text}\nКогда добавить?", reply_markup=get_time_options_kb())
    await state.set_state(AddNoteState.waiting_for_time)

@dp.callback_query(AddNoteState.waiting_for_time, F.data.startswith("time_"))
async def process_time_selection(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    text = data.get("note_text")
    ts = int(callback.data.split("_")[1])
    event_time = datetime.fromtimestamp(ts, tz=timezone.utc)
    
    if create_event_in_yandex(text, event_time):
        await callback.message.answer("✅ Добавлено!", reply_markup=None)
        await send_or_edit_main_message()
    else:
        await callback.message.answer("❌ Ошибка", reply_markup=None)
    await state.clear()

@dp.callback_query(F.data == "cancel_add")
async def cancel_add(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()

@dp.message(F.text == "⚙️ Настройки")
async def open_settings(message: types.Message):
    await message.answer(f"Интервал проверки: {CHECK_INTERVAL_MINUTES} мин", reply_markup=get_settings_kb(CHECK_INTERVAL_MINUTES))

@dp.callback_query(F.data.startswith("set_interval_"))
async def set_interval(callback: types.CallbackQuery):
    global CHECK_INTERVAL_MINUTES
    CHECK_INTERVAL_MINUTES = int(callback.data.split("_")[2])
    await callback.message.edit_text(f"✅ Интервал установлен: {CHECK_INTERVAL_MINUTES} мин", reply_markup=None)
    await send_or_edit_main_message()

@dp.callback_query(F.data == "close_settings")
async def close_settings(callback: types.CallbackQuery):
    await callback.message.delete()

# --- СИСТЕМА УВЕДОМЛЕНИЙ ---
active_notifications = {}

async def notification_scheduler():
    while True:
        await asyncio.sleep(60)
        now = get_local_time()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow_end = (today_start + timedelta(days=2))
        
        events = get_events_for_week(today_start, tomorrow_end)
        
        for ev in events:
            uid = ev['uid']
            event_time = ev['time']
            
            if event_time <= now:
                last_notify = active_notifications.get(uid)
                
                should_notify = False
                if last_notify is None:
                    if (now - event_time).total_seconds() < 3600: 
                        should_notify = True
                else:
                    if (now - last_notify).total_seconds() >= 3600:
                        should_notify = True
                
                if should_notify:
                    try:
                        kb = get_notification_keyboard(uid)
                        text = f" **Напоминание:** {ev['summary']}\nВремя: {format_time_only(event_time)}"
                        await bot.send_message(ADMIN_ID, text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
                        active_notifications[uid] = now
                        logger.info(f"Sent notification for {uid}")
                    except Exception as e:
                        logger.error(f"Notify error: {e}")

@dp.callback_query(F.data.startswith("done_notify_"))
async def done_notify(callback: types.CallbackQuery):
    uid = callback.data.split("_")[2]
    if delete_event(uid):
        await callback.message.edit_text("✅ Задача выполнена и удалена.")
        if uid in active_notifications:
            del active_notifications[uid]
        await send_or_edit_main_message()
    else:
        await callback.answer("Не удалось удалить", show_alert=True)

@dp.callback_query(F.data.startswith("snooze_"))
async def snooze_notify(callback: types.CallbackQuery):
    await callback.answer("Напомню через час.")

# --- ЗАПУСК ---
async def main():
    logger.info(f"Bot started v{BOT_VERSION}")
    await asyncio.sleep(2)
    
    asyncio.create_task(notification_scheduler())
    
    async def refresh_loop():
        while True:
            await asyncio.sleep(CHECK_INTERVAL_MINUTES * 60)
            await send_or_edit_main_message()
    
    asyncio.create_task(refresh_loop())
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())