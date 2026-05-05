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

# --- НАСТРОЙКИ И ВЕРСИЯ ---
BOT_VERSION = "1.6.0"
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
TEMP_MESSAGES = []  # Список ID сообщений для автоудаления
LAST_NOTIFICATION_IDS = {} # Словарь для хранения ID последних уведомлений {uid: message_id}

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def get_local_time():
    """Получает текущее время в московском часовом поясе"""
    moscow_tz = pytz.timezone('Europe/Moscow')
    return datetime.now(moscow_tz)

def get_week_start(date_obj):
    """Возвращает понедельник текущей недели для date_obj"""
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
    """Автоматическое удаление временных сообщений (бота и пользователя) через 15 минут"""
    while True:
        await asyncio.sleep(900)  # 15 минут (было 300 = 5 минут)
        for msg_id in TEMP_MESSAGES[:]:
            try:
                await bot.delete_message(ADMIN_ID, msg_id)
                if msg_id in TEMP_MESSAGES:
                    TEMP_MESSAGES.remove(msg_id)
                logger.info(f"Временное сообщение {msg_id} удалено")
            except Exception as e:
                if msg_id in TEMP_MESSAGES:
                    TEMP_MESSAGES.remove(msg_id)
                else:
                    logger.warning(f"Не удалось удалить сообщение {msg_id}: {e}")

def add_to_delete_list(message_obj):
    """Добавляет ID сообщения в список на удаление"""
    if message_obj and hasattr(message_obj, 'message_id'):
        if message_obj.message_id not in TEMP_MESSAGES:
            TEMP_MESSAGES.append(message_obj.message_id)
            logger.debug(f"Сообщение {message_obj.message_id} добавлено в очередь удаления")

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

def get_events_for_week(start_date, end_date):
    """
    Получает события за неделю. 
    start_date и end_date ожидаются как datetime объекты (желательно с таймзоной или наивные в Москве).
    """
    calendar = get_calendar()
    if not calendar:
        return []
    
    moscow_tz = pytz.timezone('Europe/Moscow')
    
    # 1. Нормализация входных данных в UTC для запроса к серверу
    if start_date.tzinfo is None:
        start_date = moscow_tz.localize(start_date)
    if end_date.tzinfo is None:
        end_date = moscow_tz.localize(end_date)
        
    start_utc = start_date.astimezone(pytz.utc)
    end_utc = end_date.astimezone(pytz.utc)

    logger.info(f"Загрузка событий с {start_date} (MSK) по {end_date} (MSK)")
    logger.debug(f"Query range UTC: {start_utc} - {end_utc}")

    try:
        # Используем date_search, так как он стабильнее работает с Яндексом
        events = calendar.date_search(start=start_utc, end=end_utc, expand=True)
        result = []
        
        for event in events:
            try:
                # Получаем объект icalendar (современный метод)
                ical_event = event.icalendar_instance
                
                if not ical_event:
                    continue
                    
                # Ищем компонент VEVENT
                vevent = None
                for component in ical_event.walk():
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
                
                # Обработка типа даты (Дата или Дата+Время)
                if isinstance(dt_start_val, datetime):
                    if dt_start_val.tzinfo is None:
                        dt_start_dt = dt_start_val.replace(tzinfo=timezone.utc)
                    else:
                        dt_start_dt = dt_start_val
                else:
                    # Дата без времени (целый день)
                    dt_start_dt = datetime.combine(dt_start_val, datetime.min.time()).replace(tzinfo=timezone.utc)

                # Переводим полученное время обратно в Москву для отображения пользователю
                local_dt = dt_start_dt.astimezone(moscow_tz)
                
                now_msk = get_local_time()
                
                result.append({
                     "summary": summary,
                     "time": local_dt,
                     "uid": uid,
                     "is_overdue": local_dt < now_msk
                })
            except Exception as e:
                logger.warning(f"Ошибка парсинга отдельного события: {e}")
                continue
        
        result.sort(key=lambda x: x['time'])
        return result
        
    except Exception as e:
        logger.error(f"Error fetching events from CalDAV: {e}")
        import traceback
        logger.error(traceback.format_exc())
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

def get_main_nav_keyboard(week_start):
    week_end = week_start + timedelta(days=6)
    builder = InlineKeyboardBuilder()
    
    today = get_local_time().replace(hour=0, minute=0, second=0, microsecond=0)
    builder.row(
        InlineKeyboardButton(text="🔄 Обновить", callback_data="force_refresh"),
        InlineKeyboardButton(text="📅 Сегодня/Завтра", callback_data=f"nav_today_{int(today.timestamp())}")
    )

    prev_week = week_start - timedelta(days=7)
    next_week = week_start + timedelta(days=7)

    builder.row(
        InlineKeyboardButton(text="⬅️ Назад", callback_data=f"nav_prev_{int(prev_week.timestamp())}"),
        InlineKeyboardButton(text=f"📅 {week_start.strftime('%d.%m')} - {week_end.strftime('%d.%m')}", callback_data="current_week"),
        InlineKeyboardButton(text="Вперед ➡️", callback_data=f"nav_next_{int(next_week.timestamp())}")
    )

    builder.row(InlineKeyboardButton(text="✏️ Управление (Удалить)", callback_data="manage_list"))

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
    sync_time = now.strftime("%d.%m.%Y %H:%M:%S")

    text = f"**Бот запущен! Версия: {BOT_VERSION}**\n"
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
                status_icon = "️"
            elif ev['time'].date() == now.date():
                status_icon = ""
            
            text += f"{color_mark}{time_str} — {ev['summary']} ({date_str}){status_icon}\n"

    text += f"\n_Последняя синхронизация: {sync_time}_"
    return text, get_main_nav_keyboard(week_start)

async def send_or_edit_main_message(message=None, force_current_week=False):
    global MAIN_MESSAGE_ID, CURRENT_WEEK_START
    
    if force_current_week or CURRENT_WEEK_START is None:
        CURRENT_WEEK_START = get_week_start(get_local_time())

    text, keyboard = await build_week_report(CURRENT_WEEK_START)

    try:
        if MAIN_MESSAGE_ID is None:
            if message:
                sent_msg = await message.answer(text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)
                MAIN_MESSAGE_ID = sent_msg.message_id
                # ВАЖНО: ReplyKeyboard отправляем ОТДЕЛЬНО и НЕ добавляем в список удаления!
                await message.answer("Меню:", reply_markup=get_reply_keyboard())
            else:
                sent_msg = await bot.send_message(ADMIN_ID, text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)
                MAIN_MESSAGE_ID = sent_msg.message_id
                # ВАЖНО: ReplyKeyboard отправляем ОТДЕЛЬНО и НЕ добавляем в список удаления!
                await bot.send_message(ADMIN_ID, "Меню:", reply_markup=get_reply_keyboard())
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

# --- ОБРАБОТЧИКИ ---
class AddNoteState(StatesGroup):
    waiting_for_text = State()
    waiting_for_time = State()

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID: return
    
    # Проверка подключения
    is_connected = check_caldav_connection()
    
    await state.clear()
    add_to_delete_list(message)
    global VIEW_MODE, CURRENT_WEEK_START
    VIEW_MODE = 'TODAY_TOMORROW'
    CURRENT_WEEK_START = None
    
    # Отправляем сообщение о версии и статусе
    status_text = f"✅ Бот запущен!\n**Версия: {BOT_VERSION}**\n"
    if is_connected:
        status_text += "🟢 Подключение к календарю: OK"
    else:
        status_text += "🔴 Подключение к календарю: ОШИБКА (проверьте логи)"
        
    # Статус тоже можно добавить в удаление, если нужно, но пусть висит для наглядности при старте
    # add_to_delete_list(await message.answer(status_text, parse_mode=ParseMode.MARKDOWN))
    await message.answer(status_text, parse_mode=ParseMode.MARKDOWN)
    
    await send_or_edit_main_message(message, force_current_week=True)

@dp.callback_query(F.data.startswith("nav_prev_"))
async def nav_prev(callback: types.CallbackQuery):
    ts = int(callback.data.split("_")[2])
    new_start = datetime.fromtimestamp(ts).replace(tzinfo=timezone.utc).astimezone(pytz.timezone('Europe/Moscow'))
    await callback.answer()
    global CURRENT_WEEK_START
    CURRENT_WEEK_START = new_start
    await send_or_edit_main_message()

@dp.callback_query(F.data.startswith("nav_next_"))
async def nav_next(callback: types.CallbackQuery):
    ts = int(callback.data.split("_")[2])
    new_start = datetime.fromtimestamp(ts).replace(tzinfo=timezone.utc).astimezone(pytz.timezone('Europe/Moscow'))
    await callback.answer()
    global CURRENT_WEEK_START
    CURRENT_WEEK_START = new_start
    await send_or_edit_main_message()

@dp.callback_query(F.data.startswith("nav_today_"))
async def nav_today(callback: types.CallbackQuery):
    ts = int(callback.data.split("_")[2])
    new_start = get_week_start(datetime.fromtimestamp(ts).replace(tzinfo=timezone.utc).astimezone(pytz.timezone('Europe/Moscow')))
    await callback.answer()
    global CURRENT_WEEK_START
    CURRENT_WEEK_START = new_start
    await send_or_edit_main_message()

@dp.callback_query(F.data == "force_refresh")
async def force_refresh(callback: types.CallbackQuery):
    await callback.answer("Обновление...")
    await send_or_edit_main_message(force_current_week=True)

@dp.callback_query(F.data == "manage_list")
async def show_manage_list(callback: types.CallbackQuery):
    events = get_events_for_week(CURRENT_WEEK_START, CURRENT_WEEK_START + timedelta(days=6))
    kb = get_manage_list_keyboard(events)
    if events:
        await send_temp_message("Выберите задачу для удаления:", reply_markup=kb)
    else:
        await send_temp_message("Нет задач для удаления в этой неделе.", reply_markup=kb)
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

# --- СИСТЕМА УВЕДОМЛЕНИЙ ---
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
                last_notify_time = LAST_NOTIFICATION_IDS.get(uid + "_time")
                
                should_notify = False
                if last_notify_time is None:
                    if (now - event_time).total_seconds() < 3600: 
                        should_notify = True
                else:
                    if (now - last_notify_time).total_seconds() >= 3600:
                        should_notify = True
                
                if should_notify:
                    try:
                        kb = get_notification_keyboard(uid)
                        
                        # Определяем, является ли это повторным уведомлением
                        is_repeat = (uid in LAST_NOTIFICATION_IDS)
                        prefix = "🔁 **(Повторное напоминание)**\n" if is_repeat else ""
                        
                        text = f"{prefix}**Напоминание:** {ev['summary']}\nВремя: {format_time_only(event_time)}"
                        
                        # Если есть предыдущее уведомление, удаляем его
                        if uid in LAST_NOTIFICATION_IDS:
                            old_msg_id = LAST_NOTIFICATION_IDS[uid]
                            try:
                                await bot.delete_message(ADMIN_ID, old_msg_id)
                                logger.debug(f"Deleted old notification {old_msg_id} for UID {uid}")
                            except Exception as e:
                                logger.warning(f"Could not delete old notification {old_msg_id}: {e}")
                        
                        # Отправляем новое уведомление
                        notify_msg = await bot.send_message(ADMIN_ID, text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
                         
                        # Сохраняем ID нового сообщения и время отправки
                        LAST_NOTIFICATION_IDS[uid] = notify_msg.message_id
                        LAST_NOTIFICATION_IDS[uid + "_time"] = now
                        
                        logger.info(f"Sent notification for {uid} (Repeat: {is_repeat})")
                    except Exception as e:
                        logger.error(f"Notify error: {e}")

@dp.callback_query(F.data.startswith("done_notify_"))
async def done_notify(callback: types.CallbackQuery):
    uid = callback.data.split("_")[2]
    if delete_event(uid):
        await callback.message.edit_text("✅ Задача выполнена и удалена.", parse_mode=ParseMode.MARKDOWN)
        add_to_delete_list(callback.message)
        
        # Очищаем данные о последнем уведомлении
        if uid in LAST_NOTIFICATION_IDS:
            del LAST_NOTIFICATION_IDS[uid]
        if uid + "_time" in LAST_NOTIFICATION_IDS:
            del LAST_NOTIFICATION_IDS[uid + "_time"]
            
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
            await send_or_edit_main_message(force_current_week=True)

    asyncio.create_task(refresh_loop())

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())