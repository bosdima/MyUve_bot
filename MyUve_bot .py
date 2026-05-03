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
import re

# --- НАСТРОЙКИ И ВЕРСИЯ ---
BOT_VERSION = "1.4.8"
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

# --- ЛОГИРОВАНИЕ ---
LOG_FILE = "bot.log"
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # DEBUG уровень для максимальной детализации

file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=5, encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Глобальные переменные состояния
MAIN_MESSAGE_ID = None
VIEW_MODE = 'TODAY_TOMORROW' 
CURRENT_START_DATE = None 
TEMP_MESSAGES = []

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
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
    while True:
        await asyncio.sleep(300)
        for msg_id in TEMP_MESSAGES[:]:
            try:
                await bot.delete_message(ADMIN_ID, msg_id)
                if msg_id in TEMP_MESSAGES:
                    TEMP_MESSAGES.remove(msg_id)
            except Exception as e:
                if msg_id in TEMP_MESSAGES:
                    TEMP_MESSAGES.remove(msg_id)

def add_to_delete_list(message_obj):
    if message_obj and hasattr(message_obj, 'message_id'):
        if message_obj.message_id not in TEMP_MESSAGES:
            TEMP_MESSAGES.append(message_obj.message_id)

# --- РАБОТА С CALDAV ---
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

def check_caldav_connection():
    """Проверяет подключение к календарю"""
    try:
        cal = get_calendar()
        if cal:
            logger.info("CalDAV: Подключение успешно установлено.")
            return True
        else:
            logger.error("CalDAV: Не удалось получить объект календаря.")
            return False
    except Exception as e:
        logger.error(f"CalDAV: Ошибка проверки подключения: {e}")
        return False

def get_events_for_range(start_date, end_date):
    calendar = get_calendar()
    if not calendar:
        return []
    
    moscow_tz = pytz.timezone('Europe/Moscow')
    
    if start_date.tzinfo is None:
        start_date = moscow_tz.localize(start_date)
    if end_date.tzinfo is None:
        end_date = moscow_tz.localize(end_date)
        
    start_utc = start_date.astimezone(pytz.utc)
    end_utc = end_date.astimezone(pytz.utc)

    logger.debug(f"CalDAV Query: {start_utc} - {end_utc}")

    try:
        events = calendar.date_search(start=start_utc, end=end_utc, expand=True)
        result = []
        
        for event in events:
            try:
                vevent = None
                if hasattr(event, 'icalendar_instance') and event.icalendar_instance:
                    for component in event.icalendar_instance.walk():
                        if component.name == "VEVENT":
                            vevent = component
                            break
                
                if not vevent:
                    continue

                uid = str(vevent.get('UID', ''))
                summary_obj = vevent.get('SUMMARY')
                summary = str(summary_obj) if summary_obj else "Без названия"
                
                dt_start_prop = vevent.get('DTSTART')
                if not dt_start_prop:
                    continue
                    
                dt_start_val = dt_start_prop.dt
                
                if isinstance(dt_start_val, datetime):
                    if dt_start_val.tzinfo is None:
                        dt_start_dt = dt_start_val.replace(tzinfo=timezone.utc)
                    else:
                        dt_start_dt = dt_start_val
                else:
                    dt_start_dt = datetime.combine(dt_start_val, datetime.min.time()).replace(tzinfo=timezone.utc)

                local_dt = dt_start_dt.astimezone(moscow_tz)
                
                result.append({
                     "summary": summary,
                     "time": local_dt,
                     "uid": uid,
                     "is_overdue": local_dt < get_local_time()
                })
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
        if start_dt.tzinfo is None:
            start_dt = moscow_tz.localize(start_dt)
        else:
            start_dt = start_dt.astimezone(moscow_tz)
            
        utc_dt = start_dt.astimezone(timezone.utc)
        end_dt = utc_dt + timedelta(hours=duration_hours)
        
        dt_str_start = utc_dt.strftime('%Y%m%dT%H%M%SZ')
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
    return builder.as_markup(resize_keyboard=True, one_time_keyboard=False)

def get_main_nav_keyboard():
    global VIEW_MODE
    
    builder = InlineKeyboardBuilder()
    
    if VIEW_MODE == 'TODAY_TOMORROW':
        mode_btn_text = "📅 Показать всю неделю"
        mode_cb_data = "switch_to_week"
    else:
        mode_btn_text = "🔥 Сегодня и Завтра"
        mode_cb_data = "switch_to_today_tomorrow"
        
    builder.row(InlineKeyboardButton(text=mode_btn_text, callback_data=mode_cb_data))

    if VIEW_MODE == 'WEEK':
        week_start = CURRENT_START_DATE
        prev_week = week_start - timedelta(days=7)
        next_week = week_start + timedelta(days=7)
        week_end = week_start + timedelta(days=6)
        
        builder.row(
            InlineKeyboardButton(text="⬅️ Пред. неделя", callback_data=f"nav_prev_{int(prev_week.timestamp())}"),
            InlineKeyboardButton(text="След. неделя ➡️", callback_data=f"nav_next_{int(next_week.timestamp())}")
        )
        builder.row(
             InlineKeyboardButton(text=f"📆 {week_start.strftime('%d.%m')} - {week_end.strftime('%d.%m')}", callback_data="current_week_info")
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
        btn_text = f"✏️ {ev['summary']} ({date_str} {time_str})"
        builder.button(text=btn_text, callback_data=f"edit_{ev['uid']}")

    builder.adjust(1)
    builder.row(InlineKeyboardButton(text="🔙 Закрыть список", callback_data="close_manage"))
    return builder.as_markup()

def get_edit_action_keyboard(uid):
    builder = InlineKeyboardBuilder()
    builder.button(text="📝 Изменить текст", callback_data=f"act_edit_text_{uid}")
    builder.button(text="📅 Изменить дату/время", callback_data=f"act_edit_date_{uid}")
    builder.button(text="❌ Удалить", callback_data=f"del_{uid}")
    builder.button(text="🔙 Назад", callback_data="manage_list")
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
    builder.button(text="✍️ Ввести свое время", callback_data="time_custom")
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
async def build_report():
    global VIEW_MODE, CURRENT_START_DATE
    
    now = get_local_time()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    if VIEW_MODE == 'TODAY_TOMORROW':
        start_date = today_start
        end_date = today_start + timedelta(days=2)
        
        header = f"**🔥 Ближайшие дела (Сегодня и Завтра)**\n"
        header += f"_Период: {format_date_full(start_date)} — {format_date_full(end_date - timedelta(seconds=1))}_\n\n"
        
    else:
        if CURRENT_START_DATE is None:
            CURRENT_START_DATE = get_week_start(now)
            
        start_date = CURRENT_START_DATE
        end_date = start_date + timedelta(days=7)
        
        header = f"**📅 Календарь на неделю**\n"
        header += f"_Период: {format_date_full(start_date)} — {format_date_full(start_date + timedelta(days=6))}_\n\n"

    events = get_events_for_range(start_date, end_date)

    sync_time = now.strftime("%d.%m.%Y %H:%M:%S")
    text = header

    if not events:
        text += "✨ Нет событий на этот период."
    else:
        for ev in events:
            date_str = format_date_full(ev['time'])
            time_str = format_time_only(ev['time'])
            
            status_icon = ""
            if ev['time'] < now:
                status_icon = "⚠️ "
            elif ev['time'].date() == now.date():
                status_icon = "📍 "
            
            text += f"{status_icon}`{time_str}` — **{ev['summary']}**\n_{date_str}_\n\n"

    text += f"------------------\n_Обновлено: {sync_time}_"
    return text, get_main_nav_keyboard()

async def send_or_edit_main_message(message=None):
    global MAIN_MESSAGE_ID

    text, keyboard = await build_report()

    try:
        if MAIN_MESSAGE_ID is None:
            if message:
                sent_msg = await message.answer(text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)
                MAIN_MESSAGE_ID = sent_msg.message_id
                temp_msg = await message.answer("Меню:", reply_markup=get_reply_keyboard())
                add_to_delete_list(temp_msg)
            else:
                sent_msg = await bot.send_message(ADMIN_ID, text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)
                MAIN_MESSAGE_ID = sent_msg.message_id
                temp_msg = await bot.send_message(ADMIN_ID, "Меню:", reply_markup=get_reply_keyboard())
                add_to_delete_list(temp_msg)
        else:
            await bot.edit_message_text(
                chat_id=ADMIN_ID,
                message_id=MAIN_MESSAGE_ID,
                text=text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=keyboard
            )
            
    except Exception as e:
        logger.error(f"Edit error: {e}")
        if "message to edit not found" in str(e):
            MAIN_MESSAGE_ID = None

async def send_temp_message(text, reply_markup=None):
    msg = await bot.send_message(ADMIN_ID, text, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
    add_to_delete_list(msg)
    return msg

# --- ОБРАБОТЧИКИ И СОСТОЯНИЯ ---

class AddNoteState(StatesGroup):
    waiting_for_text = State()
    waiting_for_time = State()
    waiting_for_custom_time = State()

class EditNoteState(StatesGroup):
    waiting_for_new_text = State()
    waiting_for_new_time = State()
    waiting_for_custom_time = State()
    original_uid = State()
    original_summary = State()
    original_time = State()

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    
    # Проверка подключения
    is_connected = check_caldav_connection()
    
    await state.clear()
    add_to_delete_list(message)
    global VIEW_MODE, CURRENT_START_DATE
    VIEW_MODE = 'TODAY_TOMORROW'
    CURRENT_START_DATE = None
    
    # Отправляем сообщение о версии и статусе
    status_text = f"✅ Бот запущен!\n**Версия: {BOT_VERSION}**\n"
    if is_connected:
        status_text += "🟢 Подключение к календарю: OK"
    else:
        status_text += "🔴 Подключение к календарю: ОШИБКА (проверьте логи)"
        
    await message.answer(status_text, parse_mode=ParseMode.MARKDOWN)
    
    await send_or_edit_main_message(message)

@dp.callback_query(F.data == "switch_to_week")
async def switch_to_week(callback: types.CallbackQuery):
    global VIEW_MODE, CURRENT_START_DATE
    VIEW_MODE = 'WEEK'
    if CURRENT_START_DATE is None:
        CURRENT_START_DATE = get_week_start(get_local_time())
    await callback.answer("Режим: Неделя")
    await send_or_edit_main_message()

@dp.callback_query(F.data == "switch_to_today_tomorrow")
async def switch_to_today_tomorrow(callback: types.CallbackQuery):
    global VIEW_MODE
    VIEW_MODE = 'TODAY_TOMORROW'
    await callback.answer("Режим: Сегодня и Завтра")
    await send_or_edit_main_message()

@dp.callback_query(F.data.startswith("nav_prev_"))
async def nav_prev(callback: types.CallbackQuery):
    ts = int(callback.data.split("_")[2])
    new_start = datetime.fromtimestamp(ts).replace(tzinfo=timezone.utc).astimezone(pytz.timezone('Europe/Moscow'))
    global CURRENT_START_DATE, VIEW_MODE
    VIEW_MODE = 'WEEK'
    CURRENT_START_DATE = new_start
    await callback.answer()
    await send_or_edit_main_message()

@dp.callback_query(F.data.startswith("nav_next_"))
async def nav_next(callback: types.CallbackQuery):
    ts = int(callback.data.split("_")[2])
    new_start = datetime.fromtimestamp(ts).replace(tzinfo=timezone.utc).astimezone(pytz.timezone('Europe/Moscow'))
    global CURRENT_START_DATE, VIEW_MODE
    VIEW_MODE = 'WEEK'
    CURRENT_START_DATE = new_start
    await callback.answer()
    await send_or_edit_main_message()

@dp.callback_query(F.data == "force_refresh")
async def force_refresh(callback: types.CallbackQuery):
    await callback.answer("Обновление...")
    await send_or_edit_main_message()

@dp.callback_query(F.data == "manage_list")
async def show_manage_list(callback: types.CallbackQuery):
    global VIEW_MODE, CURRENT_START_DATE
    now = get_local_time()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    if VIEW_MODE == 'TODAY_TOMORROW':
        start_date = today_start
        end_date = today_start + timedelta(days=2)
    else:
        if CURRENT_START_DATE is None: CURRENT_START_DATE = get_week_start(now)
        start_date = CURRENT_START_DATE
        end_date = start_date + timedelta(days=7)
        
    events = get_events_for_range(start_date, end_date)
    kb = get_manage_list_keyboard(events)
    if events:
        await send_temp_message("Выберите задачу для редактирования:", reply_markup=kb)
    else:
        await send_temp_message("Нет задач для редактирования в этом периоде.", reply_markup=kb)
    await callback.answer()

# 1. Обработчик нажатия на задачу в списке управления (префикс edit_)
@dp.callback_query(F.data.startswith("edit_") & ~F.data.startswith("act_edit_"))
async def ask_edit_action(callback: types.CallbackQuery):
    uid = callback.data.split("_")[1]
    logger.debug(f"User clicked edit for UID: {uid}")
    kb = get_edit_action_keyboard(uid)
    await callback.message.edit_text(f"Что сделать с задачей?", reply_markup=kb)
    await callback.answer()

# 2. Обработчик удаления (префикс del_)
@dp.callback_query(F.data.startswith("del_"))
async def delete_from_list(callback: types.CallbackQuery):
    uid = callback.data.split("_")[1]
    if delete_event(uid):
        await callback.message.edit_text("✅ Задача удалена.", reply_markup=None)
        await send_or_edit_main_message()
    else:
        await callback.answer("Ошибка удаления", show_alert=True)

# 3. Обработчик изменения текста (префикс act_edit_text_)
@dp.callback_query(F.data.startswith("act_edit_text_"))
async def start_edit_text(callback: types.CallbackQuery, state: FSMContext):
    uid = callback.data.split("_")[3]
    logger.debug(f"Start editing text for UID: {uid}")
    
    now = get_local_time()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    # Ищем в широком диапазоне
    events = get_events_for_range(today_start - timedelta(days=14), today_start + timedelta(days=14))
    
    target_event = None
    for ev in events:
        if ev['uid'] == uid:
            target_event = ev
            break
            
    if not target_event:
        logger.error(f"Event not found for UID: {uid}")
        await callback.answer("Ошибка: Событие не найдено", show_alert=True)
        return

    await state.update_data(original_uid=uid, original_time=target_event['time'])
    await callback.message.edit_text(f"Текущий текст: {target_event['summary']}\n\n✍️ Введите новый текст:", reply_markup=None)
    await state.set_state(EditNoteState.waiting_for_new_text)
    await callback.answer()

@dp.message(EditNoteState.waiting_for_new_text)
async def process_new_text_final(message: types.Message, state: FSMContext):
    new_text = message.text
    data = await state.get_data()
    uid = data.get('original_uid')
    old_time = data.get('original_time')
    
    logger.debug(f"Received new text: {new_text} for UID: {uid}")
    
    if uid and old_time:
        delete_event(uid)
        if create_event_in_yandex(new_text, old_time):
            await message.answer("✅ Текст изменен!")
            await send_or_edit_main_message()
        else:
            await message.answer("❌ Ошибка при создании нового события.")
    else:
        await message.answer("❌ Ошибка данных.")
        
    await state.clear()

# 4. Обработчик изменения даты (префикс act_edit_date_)
@dp.callback_query(F.data.startswith("act_edit_date_"))
async def start_edit_date(callback: types.CallbackQuery, state: FSMContext):
    uid = callback.data.split("_")[3]
    logger.debug(f"Start editing date for UID: {uid}")
    
    now = get_local_time()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    # Ищем в широком диапазоне
    events = get_events_for_range(today_start - timedelta(days=14), today_start + timedelta(days=14))
    
    target_event = None
    for ev in events:
        if ev['uid'] == uid:
            target_event = ev
            break
            
    if not target_event:
        logger.error(f"Event not found for UID: {uid}")
        await callback.answer("Ошибка: Событие не найдено", show_alert=True)
        return

    await state.update_data(original_uid=uid, original_summary=target_event['summary'])
    await callback.message.edit_text(f"Задача: {target_event['summary']}\n\n📅 Выберите новое время или введите вручную (ДД.ММ ЧЧ:ММ):", reply_markup=get_time_options_kb())
    await state.set_state(EditNoteState.waiting_for_new_time)
    await callback.answer()

# Обработка выбора стандартного времени при редактировании
@dp.callback_query(EditNoteState.waiting_for_new_time, F.data.startswith("time_"))
async def process_new_time(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    uid = data.get('original_uid')
    summary = data.get('original_summary')
    
    ts = int(callback.data.split("_")[1])
    new_time = datetime.fromtimestamp(ts, tz=pytz.timezone('Europe/Moscow'))
    
    logger.debug(f"Selected preset time: {new_time} for UID: {uid}")
    
    if uid and summary:
        delete_event(uid)
        if create_event_in_yandex(summary, new_time):
            await callback.message.edit_text("✅ Дата и время изменены!", reply_markup=None)
            await send_or_edit_main_message()
        else:
            await callback.message.edit_text("❌ Ошибка при создании.", reply_markup=None)
    else:
        await callback.message.edit_text("❌ Ошибка данных.", reply_markup=None)
        
    await state.clear()

# Обработка кнопки "Ввести свое время" при редактировании
@dp.callback_query(EditNoteState.waiting_for_new_time, F.data == "time_custom")
async def request_custom_time_edit(callback: types.CallbackQuery, state: FSMContext):
    logger.debug("User requested custom time input")
    await callback.message.edit_text("✍️ Введите дату и время в формате:\n`ДД.ММ ЧЧ:ММ`\nили `ДД.ММ.ГГГГ ЧЧ:ММ`\nПример: `05.05 14:30`", parse_mode=ParseMode.MARKDOWN, reply_markup=None)
    await state.set_state(EditNoteState.waiting_for_custom_time)
    await callback.answer()

# Обработка ручного ввода времени при редактировании
@dp.message(EditNoteState.waiting_for_custom_time)
async def process_custom_time_edit(message: types.Message, state: FSMContext):
    text = message.text.strip()
    logger.debug(f"Received custom time input: '{text}'")
    
    data = await state.get_data()
    uid = data.get('original_uid')
    summary = data.get('original_summary')
    
    # Парсинг времени
    new_time = parse_custom_time(text)
    
    if new_time:
        logger.debug(f"Parsed time: {new_time}")
        if uid and summary:
            delete_event(uid)
            if create_event_in_yandex(summary, new_time):
                await message.answer("✅ Дата и время изменены!")
                await send_or_edit_main_message()
            else:
                await message.answer("❌ Ошибка при создании события.")
        else:
            await message.answer("❌ Ошибка данных (UID или Summary отсутствуют).")
    else:
        logger.warning(f"Failed to parse time: '{text}'")
        await message.answer("❌ Неверный формат. Попробуйте еще раз (например: 05.05 14:30)")
        return # Не сбрасываем состояние, даем попробовать снова
        
    await state.clear()

@dp.callback_query(F.data == "close_manage")
async def close_manage(callback: types.CallbackQuery):
    await callback.message.delete()
    await callback.answer()

@dp.message(F.text == "➕ Добавить заметку")
async def start_add_note(message: types.Message, state: FSMContext):
    add_to_delete_list(message)
    prompt = await message.answer("✍️ Введите текст новой заметки:", parse_mode=ParseMode.MARKDOWN)
    add_to_delete_list(prompt)
    await state.set_state(AddNoteState.waiting_for_text)

@dp.message(AddNoteState.waiting_for_text)
async def process_note_text(message: types.Message, state: FSMContext):
    add_to_delete_list(message)
    await state.update_data(note_text=message.text)
    prompt = await message.answer(f"Текст: {message.text}\nКогда добавить?", reply_markup=get_time_options_kb())
    add_to_delete_list(prompt)
    await state.set_state(AddNoteState.waiting_for_time)

# Обработка выбора стандартного времени при создании
@dp.callback_query(AddNoteState.waiting_for_time, F.data.startswith("time_"))
async def process_time_selection(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    text = data.get("note_text")
    ts = int(callback.data.split("_")[1])
    event_time = datetime.fromtimestamp(ts, tz=pytz.timezone('Europe/Moscow'))
    
    if create_event_in_yandex(text, event_time):
        confirm_msg = await callback.message.answer("✅ Добавлено!", reply_markup=None)
        add_to_delete_list(confirm_msg)
        await send_or_edit_main_message()
    else:
        err_msg = await callback.message.answer("❌ Ошибка", reply_markup=None)
        add_to_delete_list(err_msg)
    await state.clear()

# Обработка кнопки "Ввести свое время" при создании
@dp.callback_query(AddNoteState.waiting_for_time, F.data == "time_custom")
async def request_custom_time_add(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("✍️ Введите дату и время в формате:\n`ДД.ММ ЧЧ:ММ`\nили `ДД.ММ.ГГГГ ЧЧ:ММ`\nПример: `05.05 14:30`", parse_mode=ParseMode.MARKDOWN, reply_markup=None)
    await state.set_state(AddNoteState.waiting_for_custom_time)
    await callback.answer()

# Обработка ручного ввода времени при создании
@dp.message(AddNoteState.waiting_for_custom_time)
async def process_custom_time_add(message: types.Message, state: FSMContext):
    text = message.text.strip()
    logger.debug(f"Received custom time input for new event: '{text}'")
    
    data = await state.get_data()
    note_text = data.get("note_text")
    
    # Парсинг времени
    new_time = parse_custom_time(text)
    
    if new_time:
        logger.debug(f"Parsed time: {new_time}")
        if note_text:
            if create_event_in_yandex(note_text, new_time):
                await message.answer("✅ Добавлено!")
                await send_or_edit_main_message()
            else:
                await message.answer("❌ Ошибка при создании события.")
        else:
            await message.answer("❌ Ошибка данных (текст заметки отсутствует).")
    else:
        logger.warning(f"Failed to parse time: '{text}'")
        await message.answer("❌ Неверный формат. Попробуйте еще раз (например: 05.05 14:30)")
        return # Не сбрасываем состояние
        
    await state.clear()

@dp.callback_query(F.data == "cancel_add")
async def cancel_add(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
    await callback.answer()

@dp.message(F.text == "⚙️ Настройки")
async def open_settings(message: types.Message):
    add_to_delete_list(message)
    settings_msg = await message.answer(f"Интервал проверки: {CHECK_INTERVAL_MINUTES} мин", reply_markup=get_settings_kb(CHECK_INTERVAL_MINUTES))
    add_to_delete_list(settings_msg)

@dp.callback_query(F.data.startswith("set_interval_"))
async def set_interval(callback: types.CallbackQuery):
    global CHECK_INTERVAL_MINUTES
    CHECK_INTERVAL_MINUTES = int(callback.data.split("_")[2])
    await callback.message.edit_text(f"✅ Интервал установлен: {CHECK_INTERVAL_MINUTES} мин", reply_markup=None)
    await send_or_edit_main_message()

@dp.callback_query(F.data == "close_settings")
async def close_settings(callback: types.CallbackQuery):
    await callback.message.delete()

# --- ФУНКЦИЯ ПАРСИНГА ВРЕМЕНИ ---
def parse_custom_time(text: str) -> datetime:
    """
    Парсит строку вида '03.05 20:30', '03.05.26 20:30', '03.05.2026 20:30'
    Возвращает datetime в MSK или None, если формат неверный.
    """
    moscow_tz = pytz.timezone('Europe/Moscow')
    
    # Удаляем лишние пробелы
    text = text.strip()
    
    # Паттерны
    # ДД.ММ ЧЧ:ММ
    pattern1 = r'^(\d{1,2})\.(\d{1,2})\s+(\d{1,2}):(\d{2})$'
    # ДД.ММ.ГГ ЧЧ:ММ
    pattern2 = r'^(\d{1,2})\.(\d{1,2})\.(\d{2})\s+(\d{1,2}):(\d{2})$'
    # ДД.ММ.ГГГГ ЧЧ:ММ
    pattern3 = r'^(\d{1,2})\.(\d{1,2})\.(\d{4})\s+(\d{1,2}):(\d{2})$'
    
    now = get_local_time()
    
    match = re.match(pattern1, text)
    if match:
        day, month, hour, minute = map(int, match.groups())
        year = now.year
        try:
            dt = datetime(year, month, day, hour, minute)
            return moscow_tz.localize(dt)
        except ValueError as e:
            logger.warning(f"Date validation error (pattern1): {e}")
            return None
            
    match = re.match(pattern2, text)
    if match:
        day, month, year_short, hour, minute = map(int, match.groups())
        year = 2000 + year_short
        try:
            dt = datetime(year, month, day, hour, minute)
            return moscow_tz.localize(dt)
        except ValueError as e:
            logger.warning(f"Date validation error (pattern2): {e}")
            return None
            
    match = re.match(pattern3, text)
    if match:
        day, month, year, hour, minute = map(int, match.groups())
        try:
            dt = datetime(year, month, day, hour, minute)
            return moscow_tz.localize(dt)
        except ValueError as e:
            logger.warning(f"Date validation error (pattern3): {e}")
            return None
            
    logger.warning(f"No regex pattern matched for: '{text}'")
    return None

# --- СИСТЕМА УВЕДОМЛЕНИЙ ---
active_notifications = {}

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
                        text = f"**Напоминание:** {ev['summary']}\nВремя: {format_time_only(event_time)}"
                        notify_msg = await bot.send_message(ADMIN_ID, text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
                         
                        active_notifications[uid] = now
                        logger.info(f"Sent notification for {uid}")
                    except Exception as e:
                        logger.error(f"Notify error: {e}")

@dp.callback_query(F.data.startswith("done_notify_"))
async def done_notify(callback: types.CallbackQuery):
    uid = callback.data.split("_")[2]
    if delete_event(uid):
        await callback.message.edit_text("✅ Задача выполнена и удалена.")
        add_to_delete_list(callback.message)
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
    
    # Проверка подключения при старте
    check_caldav_connection()
    
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