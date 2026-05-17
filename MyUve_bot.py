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
# НАСТРОЙКИ И ЛОГИРОВАНИЕ
# ==========================================
BOT_VERSION = "1.8.0"
load_dotenv()

# Безопасное чтение переменных (удаляем лишние пробелы и комментарии)
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

# Настройка логирования (DEBUG для отладки)
LOG_FILE = "bot.log"
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Включено подробное логирование
file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8')
formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(funcName)-15s | %(message)s')
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

if not all([BOT_TOKEN, ADMIN_ID, YANDEX_LOGIN, YANDEX_PASSWORD]):
    raise ValueError("ОШИБКА: Проверьте .env! Убедитесь, что BOT_TOKEN, ADMIN_ID, YANDEX_LOGIN и YANDEX_APP_PASSWORD заполнены корректно.")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Глобальные переменные
MAIN_MESSAGE_ID = None
TEMP_MESSAGES = []
active_notifications = {}
caldav_connected = False

# ==========================================
# FSM СОСТОЯНИЯ
# ==========================================
class AddNoteState(StatesGroup):
    waiting_for_text = State()
    waiting_for_time = State()
    waiting_for_datetime = State()

class EditNoteState(StatesGroup):
    waiting_for_datetime = State()
    original_uid = State()
    original_summary = State()

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

def delete_event(uid):
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
# КЛАВИАТУРЫ
# ==========================================
def get_reply_keyboard():
    b = ReplyKeyboardBuilder()
    b.row(KeyboardButton(text="➕ Добавить заметку"), KeyboardButton(text="⚙️ Настройки"))
    return b.as_markup(resize_keyboard=True)

def get_main_nav_keyboard():
    b = InlineKeyboardBuilder()
    today = get_local_time().replace(hour=0, minute=0, second=0, microsecond=0)
    b.row(InlineKeyboardButton(text="🔄 Обновить", callback_data="force_refresh"),
          InlineKeyboardButton(text="📅 Сегодня/Завтра", callback_data=f"nav_today_{int(today.timestamp())}"))
    b.row(InlineKeyboardButton(text="✏️ Управление", callback_data="manage_list"))
    return b.as_markup()

def get_manage_list_keyboard(events):
    if not events:
        b = InlineKeyboardBuilder(); b.button(text="🔙 Закрыть", callback_data="close_manage"); return b.as_markup()
    b = InlineKeyboardBuilder()
    for ev in events: b.button(text=f"📋 {ev['summary']} ({format_time_only(ev['time'])})", callback_data=f"manage_{ev['uid']}")
    b.adjust(1); b.row(InlineKeyboardButton(text="🔙 Закрыть", callback_data="close_manage"))
    return b.as_markup()

def get_notification_keyboard(uid):
    b = InlineKeyboardBuilder()
    b.button(text="✅ Выполнено", callback_data=f"done_notify_{uid}")
    b.button(text="📅 Изменить дату", callback_data=f"edit_date_notify_{uid}")
    b.adjust(1)
    return b.as_markup()

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

def get_manage_action_keyboard(uid):
    b = InlineKeyboardBuilder()
    b.button(text="📅 Изменить дату/время", callback_data=f"edit_date_{uid}")
    b.button(text="✅ Выполнено (Удалить)", callback_data=f"done_{uid}")
    b.adjust(1); return b.as_markup()

# ==========================================
# ЛОГИКА ОТОБРАЖЕНИЯ
# ==========================================
async def build_report():
    now = get_local_time(); start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=2); events = get_events_for_range(start, end)
    text = f"🔥 Сегодня и Завтра\n_Период: {format_date_full(start)} — {format_date_full(end - timedelta(seconds=1))}\n\n"
    text += "✨ Нет событий." if not events else "\n".join([f"📍 {format_time_only(ev['time'])} — {ev['summary']} ({format_date_full(ev['time'])})" for ev in events])
    text += f"\n\n_Обновлено: {now.strftime('%d.%m.%Y %H:%M')}"
    return text, get_main_nav_keyboard()

async def send_or_edit_main_message(message=None):
    global MAIN_MESSAGE_ID
    text, kb = await build_report()
    try:
        if MAIN_MESSAGE_ID is None:
            if message:
                MAIN_MESSAGE_ID = (await message.answer(text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)).message_id
                await message.answer("📋", reply_markup=get_reply_keyboard())
            else:
                MAIN_MESSAGE_ID = (await bot.send_message(ADMIN_ID, text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)).message_id
                await bot.send_message(ADMIN_ID, "📋", reply_markup=get_reply_keyboard())
        else:
            await bot.edit_message_text(chat_id=ADMIN_ID, message_id=MAIN_MESSAGE_ID, text=text, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)
    except Exception as e:
        if "message to edit not found" in str(e): MAIN_MESSAGE_ID = None
        else: logger.error(f"Edit error: {e}")

# ==========================================
# ОБРАБОТЧИКИ
# ==========================================
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
    logger.debug(f"/start от {message.from_user.id}")
    await send_or_edit_main_message(message)

@dp.callback_query(F.data == "force_refresh")
async def force_refresh(callback: types.CallbackQuery):
    await callback.answer("Обновлено")
    await send_or_edit_main_message()

@dp.callback_query(F.data == "manage_list")
async def show_manage(callback: types.CallbackQuery):
    now = get_local_time()
    events = get_events_for_range(now.replace(hour=0, minute=0, second=0, microsecond=0), now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=2))
    kb = get_manage_list_keyboard(events)
    await callback.answer()
    await bot.send_message(ADMIN_ID, "Выберите задачу:" if events else "Нет задач.", reply_markup=kb)

@dp.callback_query(F.data.startswith("manage_"))
async def show_actions(callback: types.CallbackQuery):
    await callback.message.edit_text("Действие:", reply_markup=get_manage_action_keyboard(callback.data.split("_")[1]))
    await callback.answer()

@dp.callback_query(F.data.startswith("done_"))
async def mark_done(callback: types.CallbackQuery):
    uid = callback.data.split("_")[1]
    if delete_event(uid):
        await callback.message.edit_text("✅ Удалено.")
        await send_or_edit_main_message()
    else: await callback.answer("Ошибка удаления", show_alert=True)

@dp.callback_query(F.data.startswith("edit_date_"))
async def start_edit_date_from_manage(callback: types.CallbackQuery, state: FSMContext):
    uid = callback.data.split("_")[2]
    logger.debug(f"Начало редактирования: {uid}")
    events = get_events_for_range(get_local_time() - timedelta(days=14), get_local_time() + timedelta(days=20))
    target = next((e for e in events if e['uid'] == uid), None)
    if not target: await callback.answer("Событие не найдено", show_alert=True); return
    await state.update_data(original_uid=uid, original_summary=target['summary'])
    await state.set_state(EditNoteState.waiting_for_datetime)
    await callback.message.edit_text(f"📅 Изменяем: {target['summary']}\nВыберите новую дату:", reply_markup=get_calendar_keyboard(), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

# --- КАЛЕНДАРЬ И ВРЕМЯ ---
@dp.callback_query(F.data.startswith("cal_prev_") | F.data.startswith("cal_next_"))
async def cal_nav(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    y, m = int(parts[2]), int(parts[3])
    try: await callback.message.edit_text("📅 Выберите дату:", reply_markup=get_calendar_keyboard(y, m))
    except Exception as e: logger.error(f"cal_nav edit error: {e}")
    await callback.answer()

@dp.callback_query(F.data.startswith("cal_day_"))
async def cal_day(callback: types.CallbackQuery, state: FSMContext):
    logger.debug(f"callback: {callback.data} | split: {callback.data.split('_')}")
    _, y, m, d = map(int, callback.data.split("_"))
    logger.debug(f"Выбрана дата: {d}.{m}.{y} | State: {await state.get_state()}")
    try: await callback.message.edit_text(f"🕐 Час для {d}.{m}.{y}:", reply_markup=get_hours_keyboard(y, m, d))
    except Exception as e: logger.error(f"cal_day edit error: {e}")
    await callback.answer()

@dp.callback_query(F.data.startswith("hour_"))
async def sel_hour(callback: types.CallbackQuery, state: FSMContext):
    logger.debug(f"Выбран час: {callback.data}")
    _, y, m, d, h = map(int, callback.data.split("_"))
    try: await callback.message.edit_text(f"⏱ Минуты для {h}:__:", reply_markup=get_minutes_keyboard(y, m, d, h))
    except Exception as e: logger.error(f"sel_hour edit error: {e}")
    await callback.answer()

@dp.callback_query(F.data.startswith("min_"))
async def sel_min(callback: types.CallbackQuery, state: FSMContext):
    try:
        logger.debug(f"Финальный выбор: {callback.data}")
        _, y, m, d, h, mn = map(int, callback.data.split("_"))
        dt = pytz.timezone('Europe/Moscow').localize(datetime(y, m, d, h, mn))
        st = await state.get_state()
        logger.debug(f"Текущий state: {st}")
        
        if st == AddNoteState.waiting_for_datetime.state:
            data = await state.get_data()
            txt = data.get("note_text", "Без названия")
            logger.info(f"Создание события: {txt} на {dt}")
            if create_event_in_yandex(txt, dt):
                await callback.message.edit_text(f"✅ Создано!\n{format_date_full(dt)} {format_time_only(dt)}")
                await send_or_edit_main_message()
                
        elif st == EditNoteState.waiting_for_datetime.state:
            data = await state.get_data()
            uid = data.get("original_uid")
            summ = data.get("original_summary")
            logger.info(f"Редактирование события {uid} -> {summ} на {dt}")
            if uid and summ:
                if delete_event(uid):
                    create_event_in_yandex(summ, dt)
                    active_notifications.pop(uid, None)
                    await callback.message.edit_text(f"✅ Изменено!\n{format_date_full(dt)} {format_time_only(dt)}")
                    await send_or_edit_main_message()
                    
        await state.clear()
    except Exception as e:
        logger.error(f"Time select error: {e}", exc_info=True)
        await callback.answer("Ошибка формата", show_alert=True)
    await callback.answer()

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
    except Exception as e: logger.error(f"go_back error: {e}")
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
    await message.answer(f"📝 {message.text}\n⏰ Когда?", reply_markup=get_time_options_kb())

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

# --- ИЗМЕНЕНИЕ ДАТЫ ИЗ УВЕДОМЛЕНИЯ ---
@dp.callback_query(F.data.startswith("edit_date_notify_"))
async def edit_date_from_notification(callback: types.CallbackQuery, state: FSMContext):
    try:
        uid = callback.data.split("_")[3]
        events = get_events_for_range(get_local_time() - timedelta(days=14), get_local_time() + timedelta(days=20))
        target = next((e for e in events if e['uid'] == uid), None)
        if not target: await callback.answer("Событие не найдено", show_alert=True); return
        await state.update_data(original_uid=uid, original_summary=target['summary'])
        await state.set_state(EditNoteState.waiting_for_datetime)
        try: await callback.message.edit_text(f"📅 Изменяем: {target['summary']}\nВыберите новую дату:", reply_markup=get_calendar_keyboard(), parse_mode=ParseMode.MARKDOWN)
        except: pass
    except Exception as e:
        logger.error(f"Edit from notify error: {e}", exc_info=True)
        await callback.answer("Ошибка", show_alert=True)
    await callback.answer()

@dp.callback_query(F.data.startswith("done_notify_"))
async def done_notify(callback: types.CallbackQuery):
    uid = callback.data.split("_")[2]
    if delete_event(uid):
        await callback.message.edit_text("✅ Выполнено.")
        active_notifications.pop(uid, None)
        await send_or_edit_main_message()
    else: await callback.answer("Ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("snooze_"))
async def snooze_notify(callback: types.CallbackQuery):
    await callback.answer("Напомню через час ⏳")

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
                if not last or (now - last['time']).total_seconds() >= 3600:
                    try:
                        if last and 'msg_id' in last:
                            try: await bot.delete_message(ADMIN_ID, last['msg_id'])
                            except: pass
                        kb = get_notification_keyboard(ev['uid'])
                        msg = await bot.send_message(ADMIN_ID, f"🔔 **{ev['summary']}**\n⏰ {format_time_only(ev['time'])}", reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
                        active_notifications[ev['uid']] = {'msg_id': msg.message_id, 'time': now}
                    except Exception as e: logger.error(f"Notify error: {e}")

# ==========================================
# ЗАПУСК И ПРОВЕРКА СТАТУСА
# ==========================================
async def check_startup_status():
    status_lines = [f"🤖 Бот запущен!\n🔖 Версия: v{BOT_VERSION}"]
    ok, cal_obj = await check_caldav_connection()
    if ok:
        status_lines.append("✅ Яндекс.Календарь: Подключен успешно")
        try:
            events_count = len(get_events_for_range(get_local_time(), get_local_time()+timedelta(days=1)))
            status_lines.append(f"📅 Найдено событий на сегодня: {events_count}")
        except: pass
    else:
        status_lines.append("❌ Яндекс.Календарь: ОШИБКА ПОДКЛЮЧЕНИЯ")
        status_lines.append("🔧 Проверьте в .env:\n- YANDEX_LOGIN\n- YANDEX_APP_PASSWORD (пароль приложения, не основной!)\n- Доступ к календарю в настройках Яндекса")
    
    text = "\n".join(status_lines)
    await bot.send_message(ADMIN_ID, text, parse_mode=ParseMode.MARKDOWN)
    logger.info("Стартовое сообщение отправлено администратору")

async def main():
    asyncio.create_task(notification_loop())
    asyncio.create_task(delete_temp_messages())
    await dp.start_polling(bot)

if __name__ == "__main__":
    logger.info("Запуск бота...")
    asyncio.run(check_startup_status())  # Проверка перед polling
    asyncio.run(main())