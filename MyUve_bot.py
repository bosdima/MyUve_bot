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
import re

# ==========================================
# НАСТРОЙКИ И ЛОГИРОВАНИЕ
# ==========================================
BOT_VERSION = "1.10.2"
load_dotenv()

def get_env(key, default=None):
    val = os.getenv(key, default)
    return val.strip() if val else val

BOT_TOKEN = get_env("BOT_TOKEN")
ADMIN_ID = int(get_env("ADMIN_ID"))
YANDEX_LOGIN = get_env("YANDEX_LOGIN")
YANDEX_PASSWORD = get_env("YANDEX_APP_PASSWORD")
CALDAV_URL = get_env("CALDAV_URL", "https://caldav.yandex.ru/")
try:
    CHECK_INTERVAL_MINUTES = int(get_env("CHECK_INTERVAL_MINUTES", "15"))
except ValueError:
    CHECK_INTERVAL_MINUTES = 15

if not all([BOT_TOKEN, ADMIN_ID, YANDEX_LOGIN, YANDEX_PASSWORD]):
    raise ValueError("ОШИБКА: Проверьте .env! Убедитесь, что BOT_TOKEN, ADMIN_ID, YANDEX_LOGIN и YANDEX_APP_PASSWORD заполнены корректно.")

LOG_FILE = "bot.log"
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8')
formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(funcName)-15s | %(message)s')
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

MAIN_MESSAGE_ID = None
TEMP_MESSAGES = []
active_notifications = {}
caldav_connected = False

# Глобальные настройки просмотра
VIEW_MODE = "short"  # "short" (сегодня/завтра) или "week" (неделя)
VIEW_OFFSET_DAYS = 0

# ==========================================
# FSM СОСТОЯНИЯ
# ==========================================
class AddNoteState(StatesGroup):
    waiting_for_text = State()
    waiting_for_time = State()
    waiting_for_datetime = State()

class EditNoteState(StatesGroup):
    waiting_for_datetime = State()
    waiting_for_new_text = State()
    original_uid = State()
    original_summary = State()
    original_time = State()

# ==========================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ==========================================
def get_local_time():
    return datetime.now(pytz.timezone('Europe/Moscow'))

def format_date_full(dt_obj):
    if dt_obj is None: return ""
    if dt_obj.tzinfo is None: dt_obj = pytz.timezone('Europe/Moscow').localize(dt_obj)
    else: dt_obj = dt_obj.astimezone(pytz.timezone('Europe/Moscow'))
    day_map = {"Monday": "Пн", "Tuesday": "Вт", "Wednesday": "Ср", "Thursday": "Чт", "Friday": "Пт", "Saturday": "Сб", "Sunday": "Вс"}
    day_name = day_map.get(dt_obj.strftime("%A"), dt_obj.strftime("%A"))
    return f"{day_name}, {dt_obj.strftime('%d.%m')}"

def format_time_only(dt_obj):
    if dt_obj is None: return "--:--"
    if dt_obj.tzinfo is None: dt_obj = pytz.timezone('Europe/Moscow').localize(dt_obj)
    else: dt_obj = dt_obj.astimezone(pytz.timezone('Europe/Moscow'))
    return dt_obj.strftime("%H:%M")

def escape_html(text):
    return re.sub(r'[<>]', '', str(text))

async def delete_temp_messages():
    while True:
        await asyncio.sleep(900)
        for msg_id in TEMP_MESSAGES[:]:
            try: await bot.delete_message(ADMIN_ID, msg_id)
            except: pass
            if msg_id in TEMP_MESSAGES: TEMP_MESSAGES.remove(msg_id)

def add_to_delete_list(message_obj):
    if message_obj and hasattr(message_obj, 'message_id'):
        if message_obj.message_id not in TEMP_MESSAGES: TEMP_MESSAGES.append(message_obj.message_id)

# ==========================================
# КАЛЕНДАРЬ И ВРЕМЯ
# ==========================================
def get_calendar_keyboard(year=None, month=None):
    now = get_local_time()
    year = year or now.year
    month = month or now.month
    builder = InlineKeyboardBuilder()
    months = ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]
    builder.row(InlineKeyboardButton(text=f"{months[month-1]} {year}", callback_data="ignore"))
    prev_m, prev_y = (month-1 if month > 1 else 12), (year if month > 1 else year-1)
    next_m, next_y = (month+1 if month < 12 else 1), (year if month < 12 else year+1)
    builder.row(
        InlineKeyboardButton(text="◀️", callback_data=f"cal_prev_{prev_y}_{prev_m}"),
        InlineKeyboardButton(text="▶️", callback_data=f"cal_next_{next_y}_{next_m}")
    )
    builder.row(*[InlineKeyboardButton(text=d, callback_data="ignore") for d in ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]])
    cal = cal_module.Calendar(firstweekday=0)
    for week in cal.monthdayscalendar(year, month):
        row = []
        for day in week:
            if day == 0: row.append(InlineKeyboardButton(text="  ", callback_data="ignore"))
            else:
                is_today = (day == now.day and month == now.month and year == now.year)
                row.append(InlineKeyboardButton(text=f"{day} 🟢" if is_today else str(day), callback_data=f"cal_day_{year}_{month}_{day}"))
        builder.row(*row)
    builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_datetime"))
    return builder.as_markup()

def get_hours_keyboard(year, month, day):
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text=f"Час для {day}.{month}.{year}", callback_data="ignore"))
    buttons = [InlineKeyboardButton(text=f"{h:02d}", callback_data=f"hour_{year}_{month}_{day}_{h}") for h in range(24)]
    for i in range(0, len(buttons), 4): builder.row(*buttons[i:i+4])
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data=f"back_calendar_{year}_{month}"))
    builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_datetime"))
    return builder.as_markup()

def get_minutes_keyboard(year, month, day, hour):
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text=f"Минуты для {hour}:__", callback_data="ignore"))
    buttons = [InlineKeyboardButton(text=f"{m:02d}", callback_data=f"min_{year}_{month}_{day}_{hour}_{m}") for m in [0, 10, 20, 30, 40, 50]]
    builder.row(*buttons)
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data=f"back_hours_{year}_{month}_{day}"))
    builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_datetime"))
    return builder.as_markup()

# ==========================================
# CALDAV
# ==========================================
async def check_caldav_connection():
    global caldav_connected
    try:
        logger.info("Проверка подключения к CalDAV...")
        client = caldav.DAVClient(url=CALDAV_URL, username=YANDEX_LOGIN, password=YANDEX_PASSWORD)
        calendars = client.principal().calendars()
        if calendars:
            caldav_connected = True
            logger.info("✅ CalDAV подключен успешно")
            return True, calendars[0]
        else:
            logger.warning("⚠️ CalDAV: календари не найдены")
            caldav_connected = False
            return False, None
    except Exception as e:
        logger.error(f"❌ Ошибка CalDAV: {e}")
        caldav_connected = False
        return False, None

def get_events_for_range(start_date, end_date):
    if not caldav_connected: return []
    moscow_tz = pytz.timezone('Europe/Moscow')
    start_utc = start_date.astimezone(pytz.utc) if start_date.tzinfo else moscow_tz.localize(start_date).astimezone(pytz.utc)
    end_utc = end_date.astimezone(pytz.utc) if end_date.tzinfo else moscow_tz.localize(end_date).astimezone(pytz.utc)
    try:
        client = caldav.DAVClient(url=CALDAV_URL, username=YANDEX_LOGIN, password=YANDEX_PASSWORD)
        calendars = client.principal().calendars()
        if not calendars: return []
        events = calendars[0].date_search(start=start_utc, end=end_utc, expand=True)
        result = []
        for event in events:
            try:
                ical = event.icalendar_instance
                if not ical: continue
                vevent = next((c for c in ical.walk() if c.name == "VEVENT"), None)
                if not vevent: continue
                uid = str(vevent.get('UID', ''))
                summary = str(vevent.get('SUMMARY', 'Без названия'))
                dt_prop = vevent.get('DTSTART')
                if not dt_prop: continue
                dt_val = dt_prop.dt
                dt_utc = dt_val if dt_val.tzinfo else dt_val.replace(tzinfo=timezone.utc)
                result.append({"summary": summary, "time": dt_utc.astimezone(moscow_tz), "uid": uid})
            except Exception as e: logger.warning(f"Parse error: {e}")
        return sorted(result, key=lambda x: x['time'])
    except Exception as e:
        logger.error(f"CalDAV fetch error: {e}")
        return []

def get_event_by_uid(uid):
    """Поиск события по UID во всём календаре (без ограничения по дате)"""
    # Убираем префикс "notify_" если есть
    if uid.startswith("notify_"):
        uid = uid.replace("notify_", "", 1)
        logger.info(f"Убран префикс notify_, реальный UID: {uid}")
    
    if not caldav_connected: 
        logger.warning("CalDAV не подключен")
        return None
    
    try:
        client = caldav.DAVClient(url=CALDAV_URL, username=YANDEX_LOGIN, password=YANDEX_PASSWORD)
        calendars = client.principal().calendars()
        if not calendars: 
            logger.warning("Календари не найдены")
            return None
        
        # Ищем событие напрямую по UID
        try:
            ev = calendars[0].event_by_uid(uid)
            if ev:
                ical = ev.icalendar_instance
                vevent = next((c for c in ical.walk() if c.name == "VEVENT"), None)
                if vevent:
                    summary = str(vevent.get('SUMMARY', 'Без названия'))
                    dt_prop = vevent.get('DTSTART')
                    if dt_prop:
                        dt_val = dt_prop.dt
                        moscow_tz = pytz.timezone('Europe/Moscow')
                        dt_utc = dt_val if dt_val.tzinfo else dt_val.replace(tzinfo=timezone.utc)
                        logger.info(f"Найдено событие по UID: {uid} -> {summary}")
                        return {"summary": summary, "time": dt_utc.astimezone(moscow_tz), "uid": uid}
        except Exception as e:
            logger.warning(f"Прямой поиск по UID не удался: {e}")
        
        # Если не нашли, пробуем широкий диапазон
        logger.info(f"Пробуем широкий диапазон поиска для UID: {uid}")
        now = get_local_time()
        start = now - timedelta(days=90)  # 3 месяца назад
        end = now + timedelta(days=90)    # 3 месяца вперед
        
        all_events = get_events_for_range(start, end)
        for ev in all_events:
            if ev['uid'] == uid:
                logger.info(f"Найдено событие в широком диапазоне: {uid}")
                return ev
        
        logger.warning(f"Событие с UID {uid} не найдено")
        return None
    except Exception as e:
        logger.error(f"Ошибка поиска события по UID {uid}: {e}", exc_info=True)
        return None

def delete_event(uid):
    # Убираем префикс "notify_" если есть
    if uid.startswith("notify_"):
        uid = uid.replace("notify_", "", 1)
    try:
        client = caldav.DAVClient(url=CALDAV_URL, username=YANDEX_LOGIN, password=YANDEX_PASSWORD)
        cal = client.principal().calendars()[0]
        ev = cal.event_by_uid(uid)
        if ev: ev.delete(); return True
    except Exception as e: logger.error(f"Delete error: {e}")
    return False

def create_event_in_yandex(summary, start_dt, duration_hours=1):
    try:
        client = caldav.DAVClient(url=CALDAV_URL, username=YANDEX_LOGIN, password=YANDEX_PASSWORD)
        cal = client.principal().calendars()[0]
        moscow_tz = pytz.timezone('Europe/Moscow')
        local_dt = start_dt if start_dt.tzinfo else moscow_tz.localize(start_dt)
        utc_dt = local_dt.astimezone(timezone.utc)
        end_dt = utc_dt + timedelta(hours=duration_hours)
        ical_data = f"BEGIN:VEVENT\nSUMMARY:{summary}\nDTSTART:{utc_dt.strftime('%Y%m%dT%H%M%SZ')}\nDTEND:{end_dt.strftime('%Y%m%dT%H%M%SZ')}\nEND:VEVENT"
        cal.save_event(ical_data)
        return True
    except Exception as e: logger.error(f"Create error: {e}")
    return False

# ==========================================
# КЛАВИАТУРЫ И ОТЧЁТ
# ==========================================
def get_main_kb():
    global VIEW_MODE
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text="📅 Сегодня/Завтра", callback_data="view_short"),
        InlineKeyboardButton(text="🗓 Неделя", callback_data="view_week")
    )
    step = 1 if VIEW_MODE == "short" else 7
    b.row(
        InlineKeyboardButton(text="◀️ Назад", callback_data=f"view_back_{step}"),
        InlineKeyboardButton(text="Вперёд ▶️", callback_data=f"view_next_{step}")
    )
    b.row(
        InlineKeyboardButton(text="🔄 Обновить", callback_data="force_refresh"),
        InlineKeyboardButton(text="✏️ Управление", callback_data="manage_list")
    )
    return b.as_markup()

def get_notification_keyboard(uid):
    b = InlineKeyboardBuilder()
    b.button(text="✅ Выполнено", callback_data=f"done_notify_{uid}")
    b.button(text="📅 Изменить дату", callback_data=f"edit_date_notify_{uid}")
    b.button(text="📝 Изменить текст", callback_data=f"edit_text_notify_{uid}")
    b.adjust(1)
    return b.as_markup()

def get_manage_action_keyboard(uid):
    b = InlineKeyboardBuilder()
    b.button(text="📅 Изменить дату/время", callback_data=f"edit_date_{uid}")
    b.button(text="📝 Изменить текст", callback_data=f"edit_text_{uid}")
    b.button(text="✅ Выполнено (Удалить)", callback_data=f"done_{uid}")
    b.adjust(1); return b.as_markup()

def get_time_options_kb():
    b = InlineKeyboardBuilder()
    now = get_local_time()
    t1 = now + timedelta(hours=1); t2 = (now + timedelta(days=1)).replace(hour=9, minute=0)
    b.button(text=f"Через 1 час ({t1.strftime('%H:%M')})", callback_data=f"time_{int(t1.timestamp())}")
    b.button(text=f"Завтра утром ({t2.strftime('%d.%m %H:%M')})", callback_data=f"time_{int(t2.timestamp())}")
    b.button(text="📅 Выбрать дату и время", callback_data="datetime_wizard")
    b.button(text="Отмена", callback_data="cancel_add")
    b.adjust(1)
    return b.as_markup()

def get_settings_kb(current_interval):
    b = InlineKeyboardBuilder()
    for mins in [5, 15, 30, 60]:
        b.button(text=f"{mins} мин {'✅' if mins==current_interval else ''}", callback_data=f"set_{mins}")
    b.adjust(2); return b.as_markup()

# ИСПРАВЛЕНО: Группировка событий по дням
async def build_report():
    global VIEW_MODE, VIEW_OFFSET_DAYS
    now = get_local_time()
    base_date = (now + timedelta(days=VIEW_OFFSET_DAYS)).replace(hour=0, minute=0, second=0, microsecond=0)

    if VIEW_MODE == "short":
        start = base_date
        end = start + timedelta(days=2)
        title = "🔥 Сегодня и Завтра"
    else:
        start = base_date
        end = start + timedelta(days=7)
        title = "📅 Неделя"

    view_label = f"<i>{format_date_full(start)} — {format_date_full(end - timedelta(seconds=1))}</i>"
    events = get_events_for_range(start, end)
    
    # Группируем события по дням
    events_by_day = {}
    for ev in events:
        day_key = ev['time'].strftime('%Y-%m-%d')
        if day_key not in events_by_day:
            events_by_day[day_key] = []
        events_by_day[day_key].append(ev)
    
    # Формируем текст с группировкой
    text = f"<b>{title}</b>\n{view_label}\n\n"
    
    if not events:
        text += "✨ Нет событий."
    else:
        day_names = {"Monday": "Понедельник", "Tuesday": "Вторник", "Wednesday": "Среда", 
                     "Thursday": "Четверг", "Friday": "Пятница", "Saturday": "Суббота", "Sunday": "Воскресенье"}
        
        for day_key in sorted(events_by_day.keys()):
            day_events = events_by_day[day_key]
            # Получаем название дня недели
            day_dt = datetime.strptime(day_key, '%Y-%m-%d')
            day_name_ru = day_names.get(day_dt.strftime('%A'), day_key)
            day_date = day_dt.strftime('%d.%m')
            
            # Добавляем заголовок дня
            text += f"-------------{day_name_ru} {day_date}-------------\n"
            
            # Добавляем события дня
            for ev in day_events:
                text += f" - 📍 <b>{format_time_only(ev['time'])}</b> — {escape_html(ev['summary'])}\n"
            
            text += "\n"
    
    text += f"Обновлено: {now.strftime('%d.%m.%Y %H:%M')}"
    return text, get_main_kb()

async def send_or_edit_main_message(message=None):
    global MAIN_MESSAGE_ID
    text, kb = await build_report()
    try:
        if MAIN_MESSAGE_ID is None:
            logger.info("Отправка главного сообщения (впервые)...")
            if message:
                msg = await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb)
                MAIN_MESSAGE_ID = msg.message_id
                await message.answer("📋", reply_markup=ReplyKeyboardBuilder().row(KeyboardButton(text="➕ Добавить заметку"), KeyboardButton(text="⚙️ Настройки")).as_markup(resize_keyboard=True))
            else:
                msg = await bot.send_message(ADMIN_ID, text, parse_mode=ParseMode.HTML, reply_markup=kb)
                MAIN_MESSAGE_ID = msg.message_id
                await bot.send_message(ADMIN_ID, "📋", reply_markup=ReplyKeyboardBuilder().row(KeyboardButton(text="➕ Добавить заметку"), KeyboardButton(text="⚙️ Настройки")).as_markup(resize_keyboard=True))
        else:
            logger.info(f"Обновление главного сообщения (ID: {MAIN_MESSAGE_ID})...")
            await bot.edit_message_text(chat_id=ADMIN_ID, message_id=MAIN_MESSAGE_ID, text=text, parse_mode=ParseMode.HTML, reply_markup=kb)
    except Exception as e:
        if "message is not modified" in str(e): logger.debug("Сообщение не изменено (игнорируем)")
        elif "message to edit not found" in str(e):
            MAIN_MESSAGE_ID = None
            logger.info("Сброс MAIN_MESSAGE_ID.")
        else: logger.error(f"Ошибка отправки/редактирования: {e}")

# ==========================================
# ОБРАБОТЧИКИ
# ==========================================
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    await send_or_edit_main_message(message)

@dp.callback_query(F.data == "force_refresh")
async def force_refresh(callback: types.CallbackQuery):
    global VIEW_MODE, VIEW_OFFSET_DAYS
    VIEW_MODE = "short"; VIEW_OFFSET_DAYS = 0
    await callback.answer("Обновлено")
    await send_or_edit_main_message()

@dp.callback_query(F.data == "view_short")
async def set_view_short(callback: types.CallbackQuery):
    global VIEW_MODE, VIEW_OFFSET_DAYS
    VIEW_MODE = "short"; VIEW_OFFSET_DAYS = 0
    await callback.answer()
    await send_or_edit_main_message()

@dp.callback_query(F.data == "view_week")
async def set_view_week(callback: types.CallbackQuery):
    global VIEW_MODE, VIEW_OFFSET_DAYS
    VIEW_MODE = "week"; VIEW_OFFSET_DAYS = 0
    await callback.answer()
    await send_or_edit_main_message()

@dp.callback_query(F.data.startswith("view_back_") | F.data.startswith("view_next_"))
async def nav_view(callback: types.CallbackQuery):
    global VIEW_OFFSET_DAYS
    step = int(callback.data.split("_")[-1])
    if "back" in callback.data: VIEW_OFFSET_DAYS -= step
    else: VIEW_OFFSET_DAYS += step
    await callback.answer()
    await send_or_edit_main_message()

# --- УПРАВЛЕНИЕ (Список -> Действия) ---
@dp.callback_query(F.data == "manage_list")
async def show_manage(callback: types.CallbackQuery):
    now = get_local_time()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=14)
    events = get_events_for_range(start, end)
    kb = InlineKeyboardBuilder()
    if not events:
        kb.button(text="Нет событий", callback_data="ignore")
    else:
        for ev in events:
            kb.button(text=f"📋 {escape_html(ev['summary'])} ({format_time_only(ev['time'])})", callback_data=f"sel_event_{ev['uid']}")
    kb.button(text="🔙 Закрыть", callback_data="close_manage")
    kb.adjust(1)
    await callback.answer()
    await bot.send_message(ADMIN_ID, "Выберите задачу для управления:", reply_markup=kb.as_markup(), parse_mode=ParseMode.HTML)

@dp.callback_query(F.data.startswith("sel_event_"))
async def select_event_manage(callback: types.CallbackQuery):
    uid = callback.data.split("_", maxsplit=2)[2]
    await callback.message.edit_text("Выберите действие:", reply_markup=get_manage_action_keyboard(uid))
    await callback.answer()

@dp.callback_query(F.data.startswith("done_"))
async def mark_done(callback: types.CallbackQuery):
    uid = callback.data.split("_", maxsplit=1)[1]
    if delete_event(uid):
        await callback.message.edit_text("✅ Удалено.")
        await send_or_edit_main_message()
    else: await callback.answer("Ошибка удаления", show_alert=True)

# ИСПРАВЛЕНО: Используем get_event_by_uid вместо поиска по диапазону
@dp.callback_query(F.data.startswith("edit_date_"))
async def start_edit_date(callback: types.CallbackQuery, state: FSMContext):
    uid = callback.data.split("_", maxsplit=2)[2]
    logger.info(f"Запрос на изменение даты для UID: {uid}")
    
    # ИСПРАВЛЕНО: Используем прямой поиск по UID
    target = get_event_by_uid(uid)
    
    if not target:
        logger.error(f"Событие {uid} не найдено в календаре")
        await callback.answer("Событие не найдено в календаре", show_alert=True)
        return
    
    logger.info(f"Найдено событие: {target['summary']} на {target['time']}")
    await state.update_data(original_uid=uid, original_summary=target['summary'], original_time=target['time'])
    await state.set_state(EditNoteState.waiting_for_datetime)
    try: 
        await callback.message.edit_text(f"📅 Изменяем: <b>{escape_html(target['summary'])}</b>\nВыберите новую дату:", reply_markup=get_calendar_keyboard(), parse_mode=ParseMode.HTML)
    except Exception as e: 
        logger.error(f"Ошибка edit_text: {e}")
    await callback.answer()

@dp.callback_query(F.data.startswith("edit_text_"))
async def start_edit_text(callback: types.CallbackQuery, state: FSMContext):
    uid = callback.data.split("_", maxsplit=2)[2]
    logger.info(f"Запрос на изменение текста для UID: {uid}")
    
    # ИСПРАВЛЕНО: Используем прямой поиск по UID
    target = get_event_by_uid(uid)
    
    if not target:
        logger.error(f"Событие {uid} не найдено в календаре")
        await callback.answer("Событие не найдено в календаре", show_alert=True)
        return
    
    logger.info(f"Найдено событие: {target['summary']}")
    await state.update_data(original_uid=uid, original_summary=target['summary'], original_time=target['time'])
    await state.set_state(EditNoteState.waiting_for_new_text)
    try: 
        await callback.message.edit_text(f"📝 <b>{escape_html(target['summary'])}</b>\nВведите новый текст:", parse_mode=ParseMode.HTML)
    except Exception as e: 
        logger.error(f"Ошибка edit_text: {e}")
    await callback.answer()

# --- КАЛЕНДАРЬ И ВРЕМЯ ---
@dp.callback_query(F.data.startswith("cal_prev_") | F.data.startswith("cal_next_"))
async def cal_nav(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    y, m = int(parts[2]), int(parts[3])
    try: await callback.message.edit_text("📅 Выберите дату:", reply_markup=get_calendar_keyboard(y, m))
    except: pass
    await callback.answer()

@dp.callback_query(F.data.startswith("cal_day_"))
async def cal_day(callback: types.CallbackQuery, state: FSMContext):
    try:
        parts = callback.data.split("_")
        y, m, d = int(parts[2]), int(parts[3]), int(parts[4])
        await callback.message.edit_text(f"🕐 Час для {d}.{m}.{y}:", reply_markup=get_hours_keyboard(y, m, d))
    except Exception as e: logger.error(f"cal_day error: {e}", exc_info=True)
    await callback.answer()

@dp.callback_query(F.data.startswith("hour_"))
async def sel_hour(callback: types.CallbackQuery, state: FSMContext):
    try:
        parts = callback.data.split("_")
        y, m, d, h = int(parts[1]), int(parts[2]), int(parts[3]), int(parts[4])
        await callback.message.edit_text(f"⏱ Минуты для {h}:__:", reply_markup=get_minutes_keyboard(y, m, d, h))
    except Exception as e: logger.error(f"sel_hour error: {e}", exc_info=True)
    await callback.answer()

@dp.callback_query(F.data.startswith("min_"))
async def sel_min(callback: types.CallbackQuery, state: FSMContext):
    try:
        parts = callback.data.split("_")
        y, m, d, h, mn = int(parts[1]), int(parts[2]), int(parts[3]), int(parts[4]), int(parts[5])
        dt = pytz.timezone('Europe/Moscow').localize(datetime(y, m, d, h, mn))
        st = await state.get_state()
        logger.info(f"Текущее состояние: {st} | Выбрано время: {dt}")

        if st == AddNoteState.waiting_for_datetime.state:
            data = await state.get_data()
            txt = data.get("note_text", "Без названия")
            if create_event_in_yandex(txt, dt):
                await callback.message.edit_text(f"✅ <b>Создано!</b>\n{format_date_full(dt)} {format_time_only(dt)}", parse_mode=ParseMode.HTML)
                await send_or_edit_main_message()
        elif st == EditNoteState.waiting_for_datetime.state:
            data = await state.get_data()
            uid = data.get("original_uid")
            summ = data.get("original_summary")
            logger.info(f"Редактирование UID:{uid} '{summ}' на {dt}")
            if uid and summ:
                if delete_event(uid):
                    create_event_in_yandex(summ, dt)
                    active_notifications.pop(uid, None)
                    await callback.message.edit_text(f"✅ <b>Изменено!</b>\n{format_date_full(dt)} {format_time_only(dt)}", parse_mode=ParseMode.HTML)
                    await send_or_edit_main_message()
                else: 
                    await callback.answer("Не удалось удалить старое событие", show_alert=True)
                    return
        await state.clear()
        await callback.answer()
    except Exception as e: 
        logger.error(f"sel_min error: {e}", exc_info=True)
        await callback.answer("Ошибка сохранения", show_alert=True)

@dp.callback_query(F.data.startswith("back_"))
async def go_back(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    try:
        if "calendar" in callback.data:
            y, m = int(parts[2]), int(parts[3])
            await callback.message.edit_text("📅 Выберите дату:", reply_markup=get_calendar_keyboard(y, m))
        elif "hours" in callback.data:
            y, m, d = int(parts[2]), int(parts[3]), int(parts[4])
            await callback.message.edit_text(f"🕐 Час для {d}.{m}.{y}:", reply_markup=get_hours_keyboard(y, m, d))
    except: pass
    await callback.answer()

@dp.callback_query(F.data == "cancel_datetime")
async def cancel_dt(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    try: await callback.message.delete()
    except: pass
    await callback.answer()

# --- ДОБАВЛЕНИЕ ЗАМЕТОК ---
@dp.message(F.text == "➕ Добавить заметку")
async def add_note(message: types.Message, state: FSMContext):
    await state.clear()
    await state.set_state(AddNoteState.waiting_for_text)
    await message.answer("✍️ Текст заметки:")

@dp.message(AddNoteState.waiting_for_text)
async def note_text(message: types.Message, state: FSMContext):
    await state.update_data(note_text=message.text)
    await state.set_state(AddNoteState.waiting_for_time)
    await message.answer(f"📝 <b>{escape_html(message.text)}</b>\n⏰ Когда?", parse_mode=ParseMode.HTML, reply_markup=get_time_options_kb())

@dp.callback_query(AddNoteState.waiting_for_time, F.data.startswith("time_"))
async def quick_time(callback: types.CallbackQuery, state: FSMContext):
    ts = int(callback.data.split("_")[1])
    dt = datetime.fromtimestamp(ts, tz=pytz.timezone('Europe/Moscow'))
    data = await state.get_data()
    if create_event_in_yandex(data.get("note_text", ""), dt):
        await callback.message.answer("✅ Добавлено!")
        await send_or_edit_main_message()
    await state.clear()
    await callback.answer()

@dp.callback_query(AddNoteState.waiting_for_time, F.data == "datetime_wizard")
async def start_wizard(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(AddNoteState.waiting_for_datetime)
    try: await callback.message.edit_text("📅 Выберите дату:", reply_markup=get_calendar_keyboard())
    except: pass
    await callback.answer()

@dp.callback_query(F.data == "cancel_add")
async def cancel_add(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    try: await callback.message.delete()
    except: pass
    await callback.answer()

# --- ОБРАБОТКА НОВОГО ТЕКСТА (FSM) ---
@dp.message(EditNoteState.waiting_for_new_text)
async def save_new_text(message: types.Message, state: FSMContext):
    new_text = message.text
    data = await state.get_data()
    uid = data.get("original_uid")
    old_time = data.get("original_time")
    
    if uid and old_time:
        logger.info(f"Изменение текста: UID:{uid} -> '{new_text}'")
        if delete_event(uid):
            create_event_in_yandex(new_text, old_time)
            active_notifications.pop(uid, None)
            await message.answer(f"✅ <b>Текст изменён!</b>\n{escape_html(new_text)}", parse_mode=ParseMode.HTML)
            await send_or_edit_main_message()
        else: await message.answer("❌ Ошибка удаления старого события.")
    else: await message.answer("❌ Ошибка: данные события потеряны.")
    await state.clear()

# --- ИЗМЕНЕНИЕ ДАТЫ/ТЕКСТА ИЗ УВЕДОМЛЕНИЯ ---
@dp.callback_query(F.data.startswith("edit_date_notify_"))
async def edit_date_from_notification(callback: types.CallbackQuery, state: FSMContext):
    try:
        uid = callback.data.split("_", maxsplit=3)[3]
        logger.info(f"Запрос на изменение даты из уведомления: {uid}")
        
        # ИСПРАВЛЕНО: Используем прямой поиск по UID (функция сама уберет префикс)
        target = get_event_by_uid(uid)
        
        if not target: 
            await callback.answer("Событие не найдено в календаре", show_alert=True)
            return
        
        await state.update_data(original_uid=uid, original_summary=target['summary'], original_time=target['time'])
        await state.set_state(EditNoteState.waiting_for_datetime)
        try: 
            await callback.message.edit_text(f"📅 Изменяем: <b>{escape_html(target['summary'])}</b>\nВыберите новую дату:", reply_markup=get_calendar_keyboard(), parse_mode=ParseMode.HTML)
        except: pass
    except Exception as e: 
        logger.error(f"Edit from notify error: {e}", exc_info=True)
        await callback.answer("Ошибка", show_alert=True)
    await callback.answer()

@dp.callback_query(F.data.startswith("edit_text_notify_"))
async def edit_text_from_notification(callback: types.CallbackQuery, state: FSMContext):
    try:
        uid = callback.data.split("_", maxsplit=3)[3]
        logger.info(f"Запрос на изменение текста из уведомления: {uid}")
        
        # ИСПРАВЛЕНО: Используем прямой поиск по UID (функция сама уберет префикс)
        target = get_event_by_uid(uid)
        
        if not target: 
            await callback.answer("Событие не найдено в календаре", show_alert=True)
            return
        
        await state.update_data(original_uid=uid, original_summary=target['summary'], original_time=target['time'])
        await state.set_state(EditNoteState.waiting_for_new_text)
        try: 
            await callback.message.edit_text(f"📝 <b>{escape_html(target['summary'])}</b>\nВведите новый текст:", parse_mode=ParseMode.HTML)
        except: pass
    except Exception as e: 
        logger.error(f"Edit text from notify error: {e}", exc_info=True)
        await callback.answer("Ошибка", show_alert=True)
    await callback.answer()

@dp.callback_query(F.data.startswith("done_notify_"))
async def done_notify(callback: types.CallbackQuery):
    uid = callback.data.split("_", maxsplit=2)[2]
    if delete_event(uid):
        await callback.message.edit_text("✅ Выполнено.")
        active_notifications.pop(uid, None)
        await send_or_edit_main_message()
    else: await callback.answer("Ошибка", show_alert=True)

@dp.message(F.text == "⚙️ Настройки")
async def settings(message: types.Message):
    await message.answer(f"Интервал: {CHECK_INTERVAL_MINUTES} мин", reply_markup=get_settings_kb(CHECK_INTERVAL_MINUTES))

@dp.callback_query(F.data.startswith("set_"))
async def set_int(callback: types.CallbackQuery):
    global CHECK_INTERVAL_MINUTES
    CHECK_INTERVAL_MINUTES = int(callback.data.split("_")[1])
    await callback.message.edit_text(f"✅ Установлено: {CHECK_INTERVAL_MINUTES} мин")
    await callback.answer()

# ==========================================
# ФОНОВЫЕ ЗАДАЧИ
# ==========================================
async def notification_loop():
    while True:
        await asyncio.sleep(60)
        now = get_local_time()
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        for ev in get_events_for_range(today, today + timedelta(days=2)):
            if ev['time'] <= now:
                last = active_notifications.get(ev['uid'])
                is_repeat = last and (now - last['time']).total_seconds() >= 3600
                
                if not last or is_repeat:
                    try:
                        if last and 'msg_id' in last:
                            try: await bot.delete_message(ADMIN_ID, last['msg_id'])
                            except: pass
                        kb = get_notification_keyboard(ev['uid'])
                        prefix = "🔔 " if not is_repeat else "🔁 Повторное уведомление (каждый час)\n"
                        msg = await bot.send_message(ADMIN_ID, f"{prefix}<b>{escape_html(ev['summary'])}</b>\n⏰ {format_time_only(ev['time'])}", reply_markup=kb, parse_mode=ParseMode.HTML)
                        active_notifications[ev['uid']] = {'msg_id': msg.message_id, 'time': now}
                    except Exception as e: logger.error(f"Notify error: {e}")

# ==========================================
# ЗАПУСК
# ==========================================
async def check_startup_status():
    status_lines = [f"<b>🤖 Бот запущен!</b>\n🔖 Версия: v{BOT_VERSION}"]
    ok, _ = await check_caldav_connection()
    if ok: status_lines.append("✅ Яндекс.Календарь: <b>Подключен успешно</b>")
    else:
        status_lines.append("❌ Яндекс.Календарь: <b>ОШИБКА ПОДКЛЮЧЕНИЯ</b>")
        status_lines.append("🔧 Проверьте в .env:\n- YANDEX_LOGIN\n- YANDEX_APP_PASSWORD")
    await bot.send_message(ADMIN_ID, "\n".join(status_lines), parse_mode=ParseMode.HTML)
    logger.info("Стартовое сообщение отправлено")

async def main():
    await check_startup_status()
    await send_or_edit_main_message()
    asyncio.create_task(notification_loop())
    asyncio.create_task(delete_temp_messages())
    logger.info("Бот готов к работе.")
    await dp.start_polling(bot)

if __name__ == "__main__":
    logger.info(f"Запуск бота v{BOT_VERSION}...")
    asyncio.run(main())