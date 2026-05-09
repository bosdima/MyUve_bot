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
import pytz
import calendar as cal_module

# ==========================================
# НАСТРОЙКИ И ВЕРСИЯ
# ==========================================
BOT_VERSION = "1.6.7"
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
    raise ValueError("Ошибка: Проверьте .env! Убедитесь, что заполнены BOT_TOKEN, ADMIN_ID, YANDEX_LOGIN и YANDEX_APP_PASSWORD.")

# ==========================================
# ЛОГИРОВАНИЕ
# ==========================================
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
TEMP_MESSAGES = []
VIEW_MODE = 'today_tomorrow' 
# Хранит {uid: {'msg_id': message_id, 'time': datetime}} для управления напоминаниями
active_notifications = {} 

# ==========================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ==========================================
def get_local_time():
    moscow_tz = pytz.timezone('Europe/Moscow')
    return datetime.now(moscow_tz)

def get_week_start(date_obj):
    return date_obj - timedelta(days=date_obj.weekday())

def format_date_full(dt_obj):
    if dt_obj is None: return ""
    if dt_obj.tzinfo is None:
        moscow_tz = pytz.timezone('Europe/Moscow')
        dt_obj = moscow_tz.localize(dt_obj)
    else:
        dt_obj = dt_obj.astimezone(pytz.timezone('Europe/Moscow'))
    
    day_name = dt_obj.strftime("%A").replace("Monday", "Пн").replace("Tuesday", "Вт").replace("Wednesday", "Ср").replace("Thursday", "Чт").replace("Friday", "Пт").replace("Saturday", "Сб").replace("Sunday", "Вс")
    return f"{day_name}, {dt_obj.strftime('%d.%m')}"

def format_time_only(dt_obj):
    if dt_obj is None: return "--:--"
    if dt_obj.tzinfo is None:
        moscow_tz = pytz.timezone('Europe/Moscow')
        dt_obj = moscow_tz.localize(dt_obj)
    else:
        dt_obj = dt_obj.astimezone(pytz.timezone('Europe/Moscow'))
    return dt_obj.strftime("%H:%M")

async def delete_temp_messages():
    """Автоматическое удаление временных сообщений через 15 минут"""
    while True:
        await asyncio.sleep(900)  # 15 минут
        for msg_id in TEMP_MESSAGES[:]:
            try:
                await bot.delete_message(ADMIN_ID, msg_id)
                if msg_id in TEMP_MESSAGES:
                    TEMP_MESSAGES.remove(msg_id)
            except Exception as e:
                if msg_id in TEMP_MESSAGES:
                    TEMP_MESSAGES.remove(msg_id)

def add_to_delete_list(message_obj):
    """Добавляет ID сообщения в список на удаление"""
    if message_obj and hasattr(message_obj, 'message_id'):
        if message_obj.message_id not in TEMP_MESSAGES:
            TEMP_MESSAGES.append(message_obj.message_id)

# ==========================================
# РАБОТА С CALDAV
# ==========================================
def get_calendar():
    try:
        client = caldav.DAVClient(url=CALDAV_URL, username=YANDEX_LOGIN, password=YANDEX_PASSWORD)
        principal = client.principal()
        calendars = principal.calendars()
        if not calendars:
            logger.error("CalDAV: Календари не найдены")
            return None
        return calendars[0]
    except Exception as e:
        logger.error(f"CalDAV Error connection: {e}")
        return None

def get_events_for_range(start_date, end_date):
    calendar = get_calendar()
    if not calendar: return []
    
    moscow_tz = pytz.timezone('Europe/Moscow')
    if start_date.tzinfo is None: start_date = moscow_tz.localize(start_date)
    if end_date.tzinfo is None: end_date = moscow_tz.localize(end_date)
        
    start_utc = start_date.astimezone(pytz.utc)
    end_utc = end_date.astimezone(pytz.utc)

    try:
        events = calendar.date_search(start=start_utc, end=end_utc, expand=True)
        result = []
        for event in events:
            try:
                ical_event = event.icalendar_instance
                if not ical_event: continue
                vevent = None
                for component in ical_event.walk():
                    if component.name == "VEVENT":
                        vevent = component
                        break
                if not vevent: continue

                uid = str(vevent.get('UID', ''))
                summary_obj = vevent.get('SUMMARY')
                summary = str(summary_obj) if summary_obj else "Без названия"
                
                dt_start_prop = vevent.get('DTSTART')
                if not dt_start_prop: continue
                dt_start_val = dt_start_prop.dt
                
                if isinstance(dt_start_val, datetime):
                    dt_start_dt = dt_start_val if dt_start_val.tzinfo else dt_start_val.replace(tzinfo=timezone.utc)
                else:
                    dt_start_dt = datetime.combine(dt_start_val, datetime.min.time()).replace(tzinfo=timezone.utc)

                local_dt = dt_start_dt.astimezone(moscow_tz)
                result.append({"summary": summary, "time": local_dt, "uid": uid, "is_overdue": local_dt < get_local_time()})
            except Exception as e:
                logger.warning(f"Ошибка парсинга события: {e}")
                continue
        result.sort(key=lambda x: x['time'])
        return result
    except Exception as e:
        logger.error(f"Error fetching events from CalDAV: {e}")
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
        moscow_tz = pytz.timezone('Europe/Moscow')
        if start_dt.tzinfo is None: start_dt = moscow_tz.localize(start_dt)
        else: start_dt = start_dt.astimezone(moscow_tz)
            
        utc_dt = start_dt.astimezone(timezone.utc)
        end_dt = utc_dt + timedelta(hours=duration_hours)
        
        event_data = f"""BEGIN:VEVENT
SUMMARY:{summary}
DTSTART:{utc_dt.strftime('%Y%m%dT%H%M%SZ')}
DTEND:{end_dt.strftime('%Y%m%dT%H%M%SZ')}
END:VEVENT"""
        calendar.save_event(event_data)
        return True
    except Exception as e:
        logger.error(f"Create error: {e}")
        return False

# ==========================================
# КЛАВИАТУРЫ
# ==========================================
def get_reply_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="➕ Добавить заметку"), KeyboardButton(text="⚙️ Настройки"))
    return builder.as_markup(resize_keyboard=True, one_time_keyboard=False)

def get_main_nav_keyboard():
    global VIEW_MODE
    builder = InlineKeyboardBuilder()
    
    today = get_local_time().replace(hour=0, minute=0, second=0, microsecond=0)
    
    if VIEW_MODE == 'today_tomorrow':
        builder.row(InlineKeyboardButton(text="📅 Показать всю неделю", callback_data="switch_to_week"))
    else:
        builder.row(InlineKeyboardButton(text="🔥 Сегодня и Завтра", callback_data="switch_to_today_tomorrow"))
        week_start = CURRENT_WEEK_START or get_week_start(today)
        week_end = week_start + timedelta(days=6)
        builder.row(
            InlineKeyboardButton(text="⬅️ Назад", callback_data=f"nav_prev_{int(week_start.timestamp())}"),
            InlineKeyboardButton(text=f"📅 {week_start.strftime('%d.%m')} - {week_end.strftime('%d.%m')}", callback_data="current_week"),
            InlineKeyboardButton(text="Вперед ➡️", callback_data=f"nav_next_{int((week_start + timedelta(days=7)).timestamp())}")
        )

    builder.row(
        InlineKeyboardButton(text="🔄 Обновить", callback_data="force_refresh"),
        InlineKeyboardButton(text="✏️ Управление", callback_data="manage_list")
    )
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
        builder.button(text=f"📋 {ev['summary']} ({date_str} {time_str})", callback_data=f"manage_{ev['uid']}")
    builder.adjust(1)
    builder.row(InlineKeyboardButton(text="🔙 Закрыть список", callback_data="close_manage"))
    return builder.as_markup()

def get_manage_action_keyboard(uid):
    builder = InlineKeyboardBuilder()
    builder.button(text="📅 Изменить дату/время", callback_data=f"edit_date_{uid}")
    builder.button(text="✅ Выполнено (Удалить)", callback_data=f"done_{uid}")
    builder.button(text="🔙 Назад к списку", callback_data="manage_list")
    builder.adjust(1)
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
    builder.button(text="📅 Выбрать дату и время", callback_data="datetime_wizard")
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

# ==========================================
# ОСНОВНАЯ ЛОГИКА
# ==========================================
async def build_report():
    global CURRENT_WEEK_START, VIEW_MODE
    now = get_local_time()
    
    if VIEW_MODE == 'today_tomorrow':
        start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(days=2) # Сегодня и завтра
        header = f"**🔥 Ближайшие дела (Сегодня и Завтра)**\n"
        header += f"_Период: {format_date_full(start_date)} — {format_date_full(end_date - timedelta(seconds=1))}_\n\n"
    else:
        if CURRENT_WEEK_START is None:
            CURRENT_WEEK_START = get_week_start(now)
        start_date = CURRENT_WEEK_START
        end_date = start_date + timedelta(days=7)
        header = f"**📅 Календарь на неделю**\n"
        header += f"_Период: {format_date_full(start_date)} — {format_date_full(start_date + timedelta(days=6))}_\n\n"

    events = get_events_for_range(start_date, end_date)
    sync_time = now.strftime("%d.%m.%Y %H:%M:%S")

    text = f"**Бот запущен! Версия: {BOT_VERSION}**\n"
    text += header

    if not events:
        text += "✨ Нет событий на этот период."
    else:
        for ev in events:
            date_str = format_date_full(ev['time'])
            time_str = format_time_only(ev['time'])
            status_icon = "⚠️ " if ev['time'] < now else ("📍 " if ev['time'].date() == now.date() else "")
            text += f"{status_icon}{time_str} — {ev['summary']} ({date_str})\n"

    text += f"\n_Последняя синхронизация: {sync_time}_"
    return text, get_main_nav_keyboard()

async def send_or_edit_main_message(message=None, force_refresh=False):
    global MAIN_MESSAGE_ID
    text, keyboard = await build_report()
    try:
        if MAIN_MESSAGE_ID is None:
            if message:
                sent_msg = await message.answer(text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)
                MAIN_MESSAGE_ID = sent_msg.message_id
                await message.answer("📋", reply_markup=get_reply_keyboard())
            else:
                sent_msg = await bot.send_message(ADMIN_ID, text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)
                MAIN_MESSAGE_ID = sent_msg.message_id
                await bot.send_message(ADMIN_ID, "📋", reply_markup=get_reply_keyboard())
        else:
            await bot.edit_message_text(chat_id=ADMIN_ID, message_id=MAIN_MESSAGE_ID, text=text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Edit error: {e}")
        if "message to edit not found" in str(e): MAIN_MESSAGE_ID = None

async def send_temp_message(text, reply_markup=None):
    msg = await bot.send_message(ADMIN_ID, text, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
    add_to_delete_list(msg)
    return msg

# ==========================================
# FSM СОСТОЯНИЯ
# ==========================================
class AddNoteState(StatesGroup):
    waiting_for_text = State()
    waiting_for_time = State()
    waiting_for_datetime = State()
    selected_year = State()
    selected_month = State()
    selected_day = State()
    selected_hour = State()

class EditNoteState(StatesGroup):
    waiting_for_datetime = State()
    selected_year = State()
    selected_month = State()
    selected_day = State()
    selected_hour = State()
    original_uid = State()
    original_summary = State()

# ==========================================
# КАЛЕНДАРЬ И ВЫБОР ВРЕМЕНИ
# ==========================================
def get_calendar_keyboard(year=None, month=None):
    now = get_local_time()
    if year is None: year = now.year
    if month is None: month = now.month
    
    builder = InlineKeyboardBuilder()
    month_name = ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"][month - 1]
    builder.row(InlineKeyboardButton(text=f"{month_name} {year}", callback_data="calendar_ignore"))
    
    prev_month = month - 1 if month > 1 else 12
    prev_year = year if month > 1 else year - 1
    next_month = month + 1 if month < 12 else 1
    next_year = year if month < 12 else year + 1
    
    builder.row(
        InlineKeyboardButton(text="◀️", callback_data=f"cal_prev_{prev_year}_{prev_month}"),
        InlineKeyboardButton(text="▶️", callback_data=f"cal_next_{next_year}_{next_month}")
    )
    
    days_short = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    builder.row(*[InlineKeyboardButton(text=d, callback_data="day_ignore") for d in days_short])
    
    cal = cal_module.Calendar(firstweekday=0)
    month_days = cal.monthdayscalendar(year, month)
    
    for week in month_days:
        row = []
        for day in week:
            if day == 0:
                row.append(InlineKeyboardButton(text=" ", callback_data="day_ignore"))
            else:
                try:
                    day_date = datetime(year, month, day)
                    is_today = (day == now.day and month == now.month and year == now.year)
                    if day_date.date() < now.date():
                        row.append(InlineKeyboardButton(text=str(day), callback_data="day_ignore"))
                    else:
                        btn_text = f"{day} 🟢" if is_today else str(day)
                        row.append(InlineKeyboardButton(text=btn_text, callback_data=f"cal_day_{year}_{month}_{day}"))
                except:
                    row.append(InlineKeyboardButton(text=str(day), callback_data="day_ignore"))
        builder.row(*row)
    
    builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_datetime"))
    return builder.as_markup()

def get_hours_keyboard(year, month, day):
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text=f"Выберите час: {day}.{month}.{year}", callback_data="hours_ignore"))
    buttons = [InlineKeyboardButton(text=f"{hour:02d}", callback_data=f"hour_{year}_{month}_{day}_{hour}") for hour in range(24)]
    for i in range(0, len(buttons), 4):
        builder.row(*buttons[i:i+4])
    builder.row(InlineKeyboardButton(text="◀️ Назад к календарю", callback_data=f"back_calendar_{year}_{month}"))
    builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_datetime"))
    return builder.as_markup()

def get_minutes_keyboard(year, month, day, hour):
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text=f"Выберите минуты: {hour}:__", callback_data="min_ignore"))
    minutes = [0, 10, 20, 30, 40, 50]
    buttons = [InlineKeyboardButton(text=f"{m:02d}", callback_data=f"min_{year}_{month}_{day}_{hour}_{m}") for m in minutes]
    builder.row(*buttons)
    builder.row(InlineKeyboardButton(text="◀️ Назад к часам", callback_data=f"back_hours_{year}_{month}_{day}"))
    builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_datetime"))
    return builder.as_markup()

# ==========================================
# ОБРАБОТЧИКИ
# ==========================================
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    global VIEW_MODE
    VIEW_MODE = 'today_tomorrow' # По умолчанию показываем сегодня и завтра
    await state.clear()
    
    add_to_delete_list(message) # Удаляем команду /start через 15 мин
    
    status_text = f"✅ Бот запущен!\n**Версия: {BOT_VERSION}**\n"
    status_text += "🟢 Календарь: OK"
    
    status_msg = await message.answer(status_text, parse_mode=ParseMode.MARKDOWN)
    add_to_delete_list(status_msg)
    
    await send_or_edit_main_message(message)

@dp.callback_query(F.data == "switch_to_week")
async def switch_to_week(callback: types.CallbackQuery):
    global VIEW_MODE, CURRENT_WEEK_START
    VIEW_MODE = 'week'
    if CURRENT_WEEK_START is None: CURRENT_WEEK_START = get_week_start(get_local_time())
    await callback.answer("Режим: Неделя")
    await send_or_edit_main_message()

@dp.callback_query(F.data == "switch_to_today_tomorrow")
async def switch_to_today_tomorrow(callback: types.CallbackQuery):
    global VIEW_MODE
    VIEW_MODE = 'today_tomorrow'
    await callback.answer("Режим: Сегодня и Завтра")
    await send_or_edit_main_message()

@dp.callback_query(F.data.startswith("nav_prev_"))
async def nav_prev(callback: types.CallbackQuery):
    ts = int(callback.data.split("_")[2])
    new_start = datetime.fromtimestamp(ts).replace(tzinfo=timezone.utc).astimezone(pytz.timezone('Europe/Moscow'))
    global CURRENT_WEEK_START, VIEW_MODE
    VIEW_MODE = 'week'
    CURRENT_WEEK_START = new_start
    await callback.answer()
    await send_or_edit_main_message()

@dp.callback_query(F.data.startswith("nav_next_"))
async def nav_next(callback: types.CallbackQuery):
    ts = int(callback.data.split("_")[2])
    new_start = datetime.fromtimestamp(ts).replace(tzinfo=timezone.utc).astimezone(pytz.timezone('Europe/Moscow'))
    global CURRENT_WEEK_START, VIEW_MODE
    VIEW_MODE = 'week'
    CURRENT_WEEK_START = new_start
    await callback.answer()
    await send_or_edit_main_message()

@dp.callback_query(F.data == "force_refresh")
async def force_refresh(callback: types.CallbackQuery):
    await callback.answer("Обновление...")
    await send_or_edit_main_message()

@dp.callback_query(F.data == "manage_list")
async def show_manage_list(callback: types.CallbackQuery):
    global CURRENT_WEEK_START
    now = get_local_time()
    if VIEW_MODE == 'today_tomorrow':
        start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(days=2)
    else:
        if CURRENT_WEEK_START is None: CURRENT_WEEK_START = get_week_start(now)
        start_date = CURRENT_WEEK_START
        end_date = start_date + timedelta(days=6, hours=23, minutes=59, seconds=59)
        
    events = get_events_for_range(start_date, end_date)
    kb = get_manage_list_keyboard(events)
    if events:
        await send_temp_message("Выберите задачу для управления:", reply_markup=kb)
    else:
        await send_temp_message("Нет задач для управления.", reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data.startswith("manage_"))
async def show_manage_actions(callback: types.CallbackQuery):
    uid = callback.data.split("_")[1]
    kb = get_manage_action_keyboard(uid)
    await callback.message.edit_text("Выберите действие:", reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data.startswith("done_"))
async def mark_as_done(callback: types.CallbackQuery):
    uid = callback.data.split("_")[1]
    if delete_event(uid):
        await callback.message.edit_text("✅ Задача выполнена и удалена.", reply_markup=None)
        await send_or_edit_main_message()
    else:
        await callback.answer("Ошибка удаления", show_alert=True)

@dp.callback_query(F.data.startswith("edit_date_"))
async def start_edit_date_from_manage(callback: types.CallbackQuery, state: FSMContext):
    logger.debug(f"Start editing date for UID: {callback.data.split('_')[2]}")
    uid = callback.data.split("_")[2]
    events = get_events_for_range(get_local_time() - timedelta(days=14), get_local_time() + timedelta(days=20))
    target_event = next((ev for ev in events if ev['uid'] == uid), None)
    if not target_event:
        await callback.answer("Ошибка: Событие не найдено", show_alert=True)
        return
    await state.update_data(original_uid=uid, original_summary=target_event['summary'])
    await state.set_state(EditNoteState.waiting_for_datetime)
    await callback.message.edit_text("📅 Выберите новую дату:", reply_markup=get_calendar_keyboard())
    await callback.answer()

@dp.callback_query(F.data.startswith("cal_prev_"))
async def calendar_prev_month(callback: types.CallbackQuery, state: FSMContext):
    try:
        parts = callback.data.split("_")
        year, month = int(parts[2]), int(parts[3])
        await callback.message.edit_text("📅 Выберите дату:", reply_markup=get_calendar_keyboard(year, month))
        await callback.answer()
    except Exception as e:
        logger.error(f"Error in calendar_prev_month: {e}")

@dp.callback_query(F.data.startswith("cal_next_"))
async def calendar_next_month(callback: types.CallbackQuery, state: FSMContext):
    try:
        parts = callback.data.split("_")
        year, month = int(parts[2]), int(parts[3])
        await callback.message.edit_text("📅 Выберите дату:", reply_markup=get_calendar_keyboard(year, month))
        await callback.answer()
    except Exception as e:
        logger.error(f"Error in calendar_next_month: {e}")

@dp.callback_query(F.data.startswith("cal_day_"))
async def calendar_day_selected(callback: types.CallbackQuery, state: FSMContext):
    try:
        logger.debug(f"Calendar day selected callback: {callback.data}")
        parts = callback.data.split("_")
        year, month, day = int(parts[2]), int(parts[3]), int(parts[4])
        logger.debug(f"Parsed date: {year}-{month}-{day}")
        await state.update_data(selected_year=year, selected_month=month, selected_day=day)
        await callback.message.edit_text(f"🕐 Выберите час для {day}.{month}.{year}:", reply_markup=get_hours_keyboard(year, month, day))
        await callback.answer()
    except Exception as e:
        logger.error(f"Error in calendar_day_selected: {e}")

@dp.callback_query(F.data.startswith("back_calendar_"))
async def back_to_calendar(callback: types.CallbackQuery, state: FSMContext):
    try:
        parts = callback.data.split("_")
        year, month = int(parts[2]), int(parts[3])
        await callback.message.edit_text("📅 Выберите дату:", reply_markup=get_calendar_keyboard(year, month))
        await callback.answer()
    except Exception as e:
        logger.error(f"Error in back_to_calendar: {e}")

@dp.callback_query(F.data.startswith("back_hours_"))
async def back_to_hours(callback: types.CallbackQuery, state: FSMContext):
    try:
        parts = callback.data.split("_")
        year, month, day = int(parts[2]), int(parts[3]), int(parts[4])
        await callback.message.edit_text(f"🕐 Выберите час для {day}.{month}.{year}:", reply_markup=get_hours_keyboard(year, month, day))
        await callback.answer()
    except Exception as e:
        logger.error(f"Error in back_to_hours: {e}")

@dp.callback_query(F.data.startswith("hour_"))
async def hour_selected(callback: types.CallbackQuery, state: FSMContext):
    try:
        parts = callback.data.split("_")
        year, month, day, hour = int(parts[1]), int(parts[2]), int(parts[3]), int(parts[4])
        await state.update_data(selected_hour=hour)
        await callback.message.edit_text(f"⏱️ Выберите минуты для {hour}:__:", reply_markup=get_minutes_keyboard(year, month, day, hour))
        await callback.answer()
    except Exception as e:
        logger.error(f"Error in hour_selected: {e}")

@dp.callback_query(F.data.startswith("min_"))
async def minute_selected(callback: types.CallbackQuery, state: FSMContext):
    try:
        logger.debug(f"Minute selected callback: {callback.data}")
        parts = list(map(int, callback.data.split("_")[1:]))
        year, month, day, hour, minute = parts
        logger.debug(f"Final datetime selection: {year}-{month}-{day} {hour}:{minute}")
        
        moscow_tz = pytz.timezone('Europe/Moscow')
        selected_datetime = moscow_tz.localize(datetime(year, month, day, hour, minute))
        current_state = await state.get_state()
        logger.debug(f"Current FSM state: {current_state}")
        
        if current_state == AddNoteState.waiting_for_datetime:
            data = await state.get_data()
            note_text = data.get("note_text")
            if note_text and create_event_in_yandex(note_text, selected_datetime):
                await callback.message.edit_text(f"✅ Добавлено!\n📅 {day}.{month}.{year} {hour}:{minute:02d}", reply_markup=None)
                await send_or_edit_main_message()
            else:
                await callback.message.edit_text("❌ Ошибка при создании.", reply_markup=None)
                
        elif current_state == EditNoteState.waiting_for_datetime:
            data = await state.get_data()
            uid = data.get('original_uid')
            summary = data.get('original_summary')
            if uid and summary:
                delete_event(uid)
                if create_event_in_yandex(summary, selected_datetime):
                    await callback.message.edit_text(f"✅ Дата изменена!\n📅 {day}.{month}.{year} {hour}:{minute:02d}", reply_markup=None)
                    await send_or_edit_main_message()
                else:
                    await callback.message.edit_text("❌ Ошибка при создании.", reply_markup=None)
            else:
                await callback.message.edit_text("❌ Ошибка данных.", reply_markup=None)
        
        await state.clear()
        await callback.answer()
    except Exception as e:
        logger.error(f"Error in minute_selected: {e}")

@dp.callback_query(F.data == "cancel_datetime")
async def cancel_datetime(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
    await callback.answer()

@dp.callback_query(F.data == "close_manage")
async def close_manage(callback: types.CallbackQuery):
    await callback.message.delete()
    await callback.answer()

@dp.message(F.text == "➕ Добавить заметку")
async def start_add_note(message: types.Message, state: FSMContext):
    add_to_delete_list(message) # Удаляем сообщение кнопки через 15 мин
    await state.clear()
    prompt = await message.answer("✍️ Введите текст новой заметки:", parse_mode=ParseMode.MARKDOWN)
    add_to_delete_list(prompt)
    await state.set_state(AddNoteState.waiting_for_text)

@dp.message(AddNoteState.waiting_for_text)
async def process_note_text(message: types.Message, state: FSMContext):
    add_to_delete_list(message) # Удаляем текст заметки через 15 мин
    await state.update_data(note_text=message.text)
    prompt = await message.answer(f"Текст: {message.text}\nКогда добавить?", reply_markup=get_time_options_kb(), parse_mode=ParseMode.MARKDOWN)
    add_to_delete_list(prompt)
    await state.set_state(AddNoteState.waiting_for_time)

@dp.callback_query(AddNoteState.waiting_for_time, F.data.startswith("time_"))
async def process_time_selection(callback: types.CallbackQuery, state: FSMContext):
    try:
        data = await state.get_data()
        text = data.get("note_text")
        ts = int(callback.data.split("_")[1])
        event_time = datetime.fromtimestamp(ts, tz=pytz.timezone('Europe/Moscow'))
        if create_event_in_yandex(text, event_time):
            confirm_msg = await callback.message.answer("✅ Добавлено!", reply_markup=None, parse_mode=ParseMode.MARKDOWN)
            add_to_delete_list(confirm_msg)
            await send_or_edit_main_message()
        else:
            err_msg = await callback.message.answer("❌ Ошибка", reply_markup=None, parse_mode=ParseMode.MARKDOWN)
            add_to_delete_list(err_msg)
        await state.clear()
    except Exception as e:
        logger.error(f"Error in process_time_selection: {e}")

@dp.callback_query(AddNoteState.waiting_for_time, F.data == "datetime_wizard")
async def start_datetime_wizard(callback: types.CallbackQuery, state: FSMContext):
    logger.debug("Starting datetime wizard for adding note")
    await state.set_state(AddNoteState.waiting_for_datetime)
    await callback.message.edit_text("📅 Выберите дату:", reply_markup=get_calendar_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "cancel_add")
async def cancel_add(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
    await callback.answer()

@dp.message(F.text == "⚙️ Настройки")
async def open_settings(message: types.Message):
    add_to_delete_list(message) # Удаляем команду настроек через 15 мин
    settings_msg = await message.answer(f"Интервал проверки: {CHECK_INTERVAL_MINUTES} мин", reply_markup=get_settings_kb(CHECK_INTERVAL_MINUTES))
    add_to_delete_list(settings_msg)

@dp.callback_query(F.data.startswith("set_interval_"))
async def set_interval(callback: types.CallbackQuery):
    global CHECK_INTERVAL_MINUTES
    CHECK_INTERVAL_MINUTES = int(callback.data.split("_")[2])
    await callback.message.edit_text(f"✅ Интервал установлен: {CHECK_INTERVAL_MINUTES} мин", reply_markup=None, parse_mode=ParseMode.MARKDOWN)
    await send_or_edit_main_message()

@dp.callback_query(F.data == "close_settings")
async def close_settings(callback: types.CallbackQuery):
    await callback.message.delete()

# ==========================================
# УВЕДОМЛЕНИЯ
# ==========================================
async def notification_scheduler():
    while True:
        await asyncio.sleep(60)
        now = get_local_time()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow_end = (today_start + timedelta(days=2))
        
        events = get_events_for_range(today_start, tomorrow_end)
        
        for ev in events:
            uid = ev['uid']
            event_time = ev['time']
            
            if event_time <= now:
                last_data = active_notifications.get(uid)
                should_notify = False
                
                if last_data is None:
                    # First notification if event is overdue but less than 1 hour ago
                    if (now - event_time).total_seconds() < 3600: 
                        should_notify = True
                else:
                    # Repeat notification if 1 hour passed since last notify
                    if (now - last_data['time']).total_seconds() >= 3600:
                        should_notify = True
                
                if should_notify:
                    try:
                        # Delete old reminder if exists
                        if last_data and 'msg_id' in last_data:
                            try:
                                await bot.delete_message(ADMIN_ID, last_data['msg_id'])
                            except Exception as e:
                                logger.debug(f"Could not delete old reminder {last_data['msg_id']}: {e}")
                        
                        kb = get_notification_keyboard(uid)
                        text = f"**Напоминание:** {ev['summary']}\nВремя: {format_time_only(event_time)}"
                        notify_msg = await bot.send_message(ADMIN_ID, text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
                        
                        # Update notification record with new message ID
                        active_notifications[uid] = {'msg_id': notify_msg.message_id, 'time': now}
                        logger.info(f"Sent notification for {uid}")
                    except Exception as e:
                        logger.error(f"Notify error: {e}")

@dp.callback_query(F.data.startswith("done_notify_"))
async def done_notify(callback: types.CallbackQuery):
    uid = callback.data.split("_")[2]
    if delete_event(uid):
        await callback.message.edit_text("✅ Задача выполнена и удалена.", parse_mode=ParseMode.MARKDOWN)
        add_to_delete_list(callback.message)
        active_notifications.pop(uid, None) # Clean up notification record
        await send_or_edit_main_message()
    else:
        await callback.answer("Не удалось удалить", show_alert=True)

@dp.callback_query(F.data.startswith("snooze_"))
async def snooze_notify(callback: types.CallbackQuery):
    await callback.answer("Напомню через час.")

# ==========================================
# ЗАПУСК
# ==========================================
async def main():
    logger.info(f"Bot started v{BOT_VERSION}")
    await asyncio.sleep(2)
    asyncio.create_task(notification_scheduler())
    asyncio.create_task(delete_temp_messages())
    async def refresh_loop():
        while True:
            await asyncio.sleep(CHECK_INTERVAL_MINUTES * 60)
            await send_or_edit_main_message()
    asyncio.create_task(refresh_loop())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())