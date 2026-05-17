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
BOT_VERSION = "1.7.1"
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
file_handler = RotatingFileHandler(LOG_FILE, maxBytes=500*1024, backupCount=5, encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Глобальные переменные
MAIN_MESSAGE_ID = None
TEMP_MESSAGES = []
active_notifications = {}  # {uid: {'msg_id': int, 'time': datetime}}

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
    if dt_obj.tzinfo is None:
        dt_obj = pytz.timezone('Europe/Moscow').localize(dt_obj)
    else:
        dt_obj = dt_obj.astimezone(pytz.timezone('Europe/Moscow'))
    day_name = dt_obj.strftime("%A").replace("Monday", "Пн").replace("Tuesday", "Вт").replace("Wednesday", "Ср").replace("Thursday", "Чт").replace("Friday", "Пт").replace("Saturday", "Сб").replace("Sunday", "Вс")
    return f"{day_name}, {dt_obj.strftime('%d.%m')}"

def format_time_only(dt_obj):
    if dt_obj is None: return "--:--"
    if dt_obj.tzinfo is None:
        dt_obj = pytz.timezone('Europe/Moscow').localize(dt_obj)
    else:
        dt_obj = dt_obj.astimezone(pytz.timezone('Europe/Moscow'))
    return dt_obj.strftime("%H:%M")

async def delete_temp_messages():
    while True:
        await asyncio.sleep(900)  # 15 минут
        for msg_id in TEMP_MESSAGES[:]:
            try:
                await bot.delete_message(ADMIN_ID, msg_id)
                if msg_id in TEMP_MESSAGES: TEMP_MESSAGES.remove(msg_id)
            except Exception:
                if msg_id in TEMP_MESSAGES: TEMP_MESSAGES.remove(msg_id)

def add_to_delete_list(message_obj):
    if message_obj and hasattr(message_obj, 'message_id'):
        if message_obj.message_id not in TEMP_MESSAGES:
            TEMP_MESSAGES.append(message_obj.message_id)

# ==========================================
# КАЛЕНДАРЬ И ВЫБОР ВРЕМЕНИ
# ==========================================
def get_calendar_keyboard(year=None, month=None):
    now = get_local_time()
    year = year or now.year
    month = month or now.month
    builder = InlineKeyboardBuilder()
    month_name = ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"][month - 1]
    builder.row(InlineKeyboardButton(text=f"{month_name} {year}", callback_data="ignore"))

    builder.row(
        InlineKeyboardButton(text="◀️", callback_data=f"cal_prev_{year}_{month-1 if month > 1 else 12}_{year if month > 1 else year-1}"),
        InlineKeyboardButton(text="▶️", callback_data=f"cal_next_{year}_{month+1 if month < 12 else 1}_{year if month < 12 else year+1}")
    )
    builder.row(*[InlineKeyboardButton(text=d, callback_data="ignore") for d in ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]])

    cal = cal_module.Calendar(firstweekday=0)
    for week in cal.monthdayscalendar(year, month):
        row = []
        for day in week:
            if day == 0:
                row.append(InlineKeyboardButton(text="  ", callback_data="ignore"))
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
# РАБОТА С CALDAV
# ==========================================
def get_calendar():
    try:
        client = caldav.DAVClient(url=CALDAV_URL, username=YANDEX_LOGIN, password=YANDEX_PASSWORD)
        calendars = client.principal().calendars()
        return calendars[0] if calendars else None
    except Exception as e:
        logger.error(f"CalDAV Error: {e}")
        return None

def get_events_for_range(start_date, end_date):
    calendar = get_calendar()
    if not calendar: return []
    moscow_tz = pytz.timezone('Europe/Moscow')
    start_utc = start_date.astimezone(pytz.utc) if start_date.tzinfo else moscow_tz.localize(start_date).astimezone(pytz.utc)
    end_utc = end_date.astimezone(pytz.utc) if end_date.tzinfo else moscow_tz.localize(end_date).astimezone(pytz.utc)
    try:
        events = calendar.date_search(start=start_utc, end=end_utc, expand=True)
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
            except Exception as e:
                logger.warning(f"Parse error: {e}")
        return sorted(result, key=lambda x: x['time'])
    except Exception as e:
        logger.error(f"CalDAV fetch error: {e}")
        return []

def delete_event(uid):
    cal = get_calendar()
    if not cal: return False
    try:
        ev = cal.event_by_uid(uid)
        if ev: ev.delete(); return True
    except Exception as e: logger.error(f"Delete error: {e}")
    return False

def create_event_in_yandex(summary, start_dt, duration_hours=1):
    cal = get_calendar()
    if not cal: return False
    try:
        moscow_tz = pytz.timezone('Europe/Moscow')
        local_dt = start_dt if start_dt.tzinfo else moscow_tz.localize(start_dt)
        utc_dt = local_dt.astimezone(timezone.utc)
        end_dt = utc_dt + timedelta(hours=duration_hours)
        ical_data = f"""BEGIN:VEVENT
SUMMARY:{summary}
DTSTART:{utc_dt.strftime('%Y%m%dT%H%M%SZ')}
DTEND:{end_dt.strftime('%Y%m%dT%H%M%SZ')}
END:VEVENT"""
        cal.save_event(ical_data)
        return True
    except Exception as e:
        logger.error(f"Create error: {e}")
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
        b = InlineKeyboardBuilder()
        b.button(text="🔙 Закрыть", callback_data="close_manage")
        return b.as_markup()
    b = InlineKeyboardBuilder()
    for ev in events:
        b.button(text=f"📋 {ev['summary']} ({format_time_only(ev['time'])})", callback_data=f"manage_{ev['uid']}")
    b.adjust(1)
    b.row(InlineKeyboardButton(text="🔙 Закрыть", callback_data="close_manage"))
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
    t1 = now + timedelta(hours=1)
    t2 = (now + timedelta(days=1)).replace(hour=9, minute=0)
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
    b.adjust(2)
    return b.as_markup()

# ==========================================
# ЛОГИКА ОТОБРАЖЕНИЯ
# ==========================================
async def build_report():
    now = get_local_time()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=2)
    events = get_events_for_range(start, end)
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
        logger.error(f"Edit error: {e}")
        if "message to edit not found" in str(e): MAIN_MESSAGE_ID = None

async def send_temp_message(text, reply_markup=None):
    msg = await bot.send_message(ADMIN_ID, text, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
    add_to_delete_list(msg)
    return msg

# ==========================================
# ОБРАБОТЧИКИ
# ==========================================
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if message.from_user.id != ADMIN_ID: return
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

def get_manage_action_keyboard(uid):
    b = InlineKeyboardBuilder()
    b.button(text="📅 Изменить дату/время", callback_data=f"edit_date_{uid}")
    b.button(text="✅ Выполнено (Удалить)", callback_data=f"done_{uid}")
    b.adjust(1)
    return b.as_markup()

@dp.callback_query(F.data.startswith("done_"))
async def mark_done(callback: types.CallbackQuery):
    uid = callback.data.split("_")[1]
    if delete_event(uid):
        await callback.message.edit_text("✅ Удалено.")
        await send_or_edit_main_message()
    else:
        await callback.answer("Ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("edit_date_"))
async def start_edit_date_from_manage(callback: types.CallbackQuery, state: FSMContext):
    uid = callback.data.split("_")[2]
    events = get_events_for_range(get_local_time() - timedelta(days=14), get_local_time() + timedelta(days=20))
    target = next((e for e in events if e['uid'] == uid), None)
    if not target:
        await callback.answer("Событие не найдено", show_alert=True)
        return
    await state.update_data(original_uid=uid, original_summary=target['summary'])
    await state.set_state(EditNoteState.waiting_for_datetime)
    await callback.message.edit_text(f"📅 Изменяем: {target['summary']}\nВыберите новую дату:", reply_markup=get_calendar_keyboard(), parse_mode=ParseMode.MARKDOWN)
    await callback.answer()

# --- КАЛЕНДАРЬ И ВРЕМЯ ---
@dp.callback_query(F.data.startswith("cal_prev_") | F.data.startswith("cal_next_"))
async def cal_nav(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    y, m = int(parts[2]), int(parts[3])
    await callback.message.edit_text("📅 Выберите дату:", reply_markup=get_calendar_keyboard(y, m))
    await callback.answer()

@dp.callback_query(F.data.startswith("cal_day_"))
async def cal_day(callback: types.CallbackQuery, state: FSMContext):
    _, y, m, d = map(int, callback.data.split("_"))
    await callback.message.edit_text(f"🕐 Час для {d}.{m}.{y}:", reply_markup=get_hours_keyboard(y, m, d))
    await callback.answer()

@dp.callback_query(F.data.startswith("hour_"))
async def sel_hour(callback: types.CallbackQuery, state: FSMContext):
    _, y, m, d, h = map(int, callback.data.split("_"))
    await callback.message.edit_text(f"⏱ Минуты для {h}:__:", reply_markup=get_minutes_keyboard(y, m, d, h))
    await callback.answer()

@dp.callback_query(F.data.startswith("min_"))
async def sel_min(callback: types.CallbackQuery, state: FSMContext):
    try:
        _, y, m, d, h, mn = map(int, callback.data.split("_"))
        dt = pytz.timezone('Europe/Moscow').localize(datetime(y, m, d, h, mn))
        st = await state.get_state()
        
        if st == AddNoteState.waiting_for_datetime.state:
            data = await state.get_data()
            txt = data.get("note_text", "Без названия")
            if create_event_in_yandex(txt, dt):
                await callback.message.edit_text(f"✅ Создано!\n{format_date_full(dt)} {format_time_only(dt)}")
                await send_or_edit_main_message()
                
        elif st == EditNoteState.waiting_for_datetime.state:
            data = await state.get_data()
            uid = data.get("original_uid")
            summ = data.get("original_summary")
            if uid and summ:
                if delete_event(uid):
                    create_event_in_yandex(summ, dt)
                    active_notifications.pop(uid, None)
                    await callback.message.edit_text(f"✅ Изменено!\n{format_date_full(dt)} {format_time_only(dt)}")
                    await send_or_edit_main_message()
                    
        await state.clear()
    except Exception as e:
        logger.error(f"Time select error: {e}")
        await callback.answer("Ошибка формата", show_alert=True)

@dp.callback_query(F.data.startswith("back_"))
async def go_back(callback: types.CallbackQuery):
    parts = callback.data.split("_")
    if "calendar" in callback.data:
        y, m = int(parts[2]), int(parts[3])
        await callback.message.edit_text("📅 Дата:", reply_markup=get_calendar_keyboard(y, m))
    else:
        y, m, d = int(parts[2]), int(parts[3]), int(parts[4])
        await callback.message.edit_text(f"🕐 Час для {d}.{m}.{y}:", reply_markup=get_hours_keyboard(y, m, d))
    await callback.answer()

@dp.callback_query(F.data == "cancel_datetime")
async def cancel_dt(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
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

@dp.callback_query(AddNoteState.waiting_for_time, F.data == "datetime_wizard")
async def start_wizard(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(AddNoteState.waiting_for_datetime)
    await callback.message.edit_text("📅 Выберите дату:", reply_markup=get_calendar_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "cancel_add")
async def cancel_add(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
    await callback.answer()

# --- ИЗМЕНЕНИЕ ДАТЫ ИЗ УВЕДОМЛЕНИЯ ---
@dp.callback_query(F.data.startswith("edit_date_notify_"))
async def edit_date_from_notification(callback: types.CallbackQuery, state: FSMContext):
    try:
        uid = callback.data.split("_")[3]
        events = get_events_for_range(get_local_time() - timedelta(days=14), get_local_time() + timedelta(days=20))
        target = next((e for e in events if e['uid'] == uid), None)
        if not target:
            await callback.answer("Событие не найдено", show_alert=True)
            return
        await state.update_data(original_uid=uid, original_summary=target['summary'])
        await state.set_state(EditNoteState.waiting_for_datetime)
        await callback.message.edit_text(f"📅 Изменяем: {target['summary']}\nВыберите новую дату:", reply_markup=get_calendar_keyboard(), parse_mode=ParseMode.MARKDOWN)
        await callback.answer()
    except Exception as e:
        logger.error(f"Edit from notify error: {e}")
        await callback.answer("Ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("done_notify_"))
async def done_notify(callback: types.CallbackQuery):
    uid = callback.data.split("_")[2]
    if delete_event(uid):
        await callback.message.edit_text("✅ Выполнено.")
        active_notifications.pop(uid, None)
        await send_or_edit_main_message()
    else:
        await callback.answer("Ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("snooze_"))
async def snooze_notify(callback: types.CallbackQuery):
    await callback.answer("Напомню через час ⏳")

# --- НАСТРОЙКИ ---
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
                # Отправляем, если это первое уведомление или прошел час
                if not last or (now - last['time']).total_seconds() >= 3600:
                    try:
                        # Удаляем старое уведомление перед отправкой нового
                        if last and 'msg_id' in last:
                            try: await bot.delete_message(ADMIN_ID, last['msg_id'])
                            except: pass
                        kb = get_notification_keyboard(ev['uid'])
                        msg = await bot.send_message(ADMIN_ID, f"🔔 **{ev['summary']}**\n⏰ {format_time_only(ev['time'])}", reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
                        active_notifications[ev['uid']] = {'msg_id': msg.message_id, 'time': now}
                    except Exception as e:
                        logger.error(f"Notify error: {e}")

# ==========================================
# ЗАПУСК
# ==========================================
async def main():
    logger.info(f"Bot v{BOT_VERSION} started")
    asyncio.create_task(notification_loop())
    asyncio.create_task(delete_temp_messages())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())