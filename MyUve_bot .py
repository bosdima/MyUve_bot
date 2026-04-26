import os
import asyncio
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta, timezone
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
BOT_VERSION = "1.2.0"  # Новая версия с исправлениями отображения

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
    raise ValueError("Ошибка: Проверьте .env. Убедитесь, что заполнены все поля!")

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

# Глобальная переменная для хранения ID главного сообщения, чтобы редактировать его
MAIN_MESSAGE_ID = None

# --- МАШИНА СОСТОЯНИЙ ---
class AddNoteState(StatesGroup):
    waiting_for_text = State()
    waiting_for_time = State()

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

def get_local_time():
    return datetime.now().astimezone()

def format_time_only(dt_obj):
    if dt_obj is None: return "--:--"
    local_dt = dt_obj.astimezone() if hasattr(dt_obj, 'tzinfo') else dt_obj
    return local_dt.strftime("%H:%M")

def format_date_full(dt_obj):
    if dt_obj is None: return ""
    local_dt = dt_obj.astimezone() if hasattr(dt_obj, 'tzinfo') else dt_obj
    day_name = local_dt.strftime("%A").replace("Monday", "Понедельник").replace("Tuesday", "Вторник").replace("Wednesday", "Среда").replace("Thursday", "Четверг").replace("Friday", "Пятница").replace("Saturday", "Суббота").replace("Sunday", "Воскресенье")
    return f"{day_name}, {local_dt.strftime('%d.%m')}"

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

def get_events_data():
    calendar = get_calendar()
    if not calendar:
        return [], [], []
    
    now = get_local_time()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = now.replace(hour=23, minute=59, second=59, microsecond=0)
    tomorrow_start = today_end + timedelta(seconds=1)
    tomorrow_end = tomorrow_start + timedelta(days=1)
    
    # Запрос за большой период для захвата просроченных
    search_start = now - timedelta(days=30) 
    
    try:
        events = calendar.date_search(start=search_start, end=tomorrow_end, expand=True)
        
        overdue_list = []
        today_list = []
        tomorrow_list = []
        
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
                
                event_data = {
                    "summary": summary,
                    "time": local_dt,
                    "uid": uid,
                    "time_str": format_time_only(local_dt),
                    "date_str": format_date_full(local_dt)
                }
                
                if local_dt < now:
                    overdue_list.append(event_data)
                elif local_dt.date() == today_start.date():
                    today_list.append(event_data)
                elif local_dt.date() == tomorrow_start.date():
                    tomorrow_list.append(event_data)
                    
            except Exception as e:
                logger.warning(f"Ошибка парсинга события: {e}")
                continue
        
        # Сортировка
        overdue_list.sort(key=lambda x: x['time'])
        today_list.sort(key=lambda x: x['time'])
        tomorrow_list.sort(key=lambda x: x['time'])
        
        return overdue_list, today_list, tomorrow_list
    except Exception as e:
        logger.error(f"Error fetching events: {e}")
        return [], [], []

def delete_event(uid):
    calendar = get_calendar()
    if not calendar: return False
    try:
        ev = calendar.event_by_uid(uid)
        if ev:
            ev.delete()
            logger.info(f"Deleted event: {uid}")
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
        logger.info(f"Created event: {summary}")
        return True
    except Exception as e:
        logger.error(f"Create error: {e}")
        return False

# --- КЛАВИАТУРЫ ---

def get_main_action_keyboard():
    """Клавиатура действий под главным сообщением"""
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Добавить новую заметку", callback_data="add_note_start")
    builder.button(text="️ Настройки", callback_data="open_settings")
    builder.button(text="🔄 Обновить сейчас", callback_data="force_refresh")
    return builder.as_markup()

def get_time_options_kb():
    builder = InlineKeyboardBuilder()
    now = get_local_time()
    
    t1 = now + timedelta(hours=1)
    t2 = (now + timedelta(days=1)).replace(hour=9, minute=0, second=0)
    t3 = (now + timedelta(days=1)).replace(hour=18, minute=0, second=0)
    
    builder.button(text=f" Через 1 час ({t1.strftime('%H:%M')})", callback_data=f"time_{int(t1.timestamp())}")
    builder.button(text=f" Завтра утром ({t2.strftime('%d.%m %H:%M')})", callback_data=f"time_{int(t2.timestamp())}")
    builder.button(text=f" Завтра вечером ({t3.strftime('%d.%m %H:%M')})", callback_data=f"time_{int(t3.timestamp())}")
    builder.button(text=" Отмена", callback_data="cancel_add")
    builder.adjust(1)
    return builder.as_markup()

def get_done_kb(uid):
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Выполнено (Удалить)", callback_data=f"done_{uid}")
    return builder.as_markup()

def get_settings_kb(current_interval):
    builder = InlineKeyboardBuilder()
    intervals = [5, 15, 30, 60]
    for mins in intervals:
        text = f"{mins} мин" + (" ✅" if mins == current_interval else "")
        builder.button(text=text, callback_data=f"set_interval_{mins}")
    builder.button(text="🔙 Назад", callback_data="close_settings")
    builder.adjust(2)
    return builder.as_markup()

# --- ФОРМИРОВАНИЕ ГЛАВНОГО СООБЩЕНИЯ ---

async def send_or_edit_main_message(message=None):
    global MAIN_MESSAGE_ID
    
    overdue, today, tomorrow = get_events_data()
    now = get_local_time()
    sync_time = now.strftime("%d.%m.%Y %H:%M:%S")
    
    text = f"🤖 **Бот запущен! Версия: {BOT_VERSION}**\n\n"
    
    # Просроченные (Красный)
    if overdue:
        text += "🔴 **ПРОСРОЧЕНО:**\n"
        for ev in overdue:
            text += f"   • {ev['time_str']} — {ev['summary']} ⚠️\n"
    else:
        text += "🟢 Просроченных задач нет.\n"
    
    text += "\n"
    
    # Сегодня (Зеленый)
    day_str = format_date_full(now)
    text += f"🟢 **СЕГОДНЯ ({day_str}):**\n"
    if today:
        for ev in today:
            text += f"   • {ev['time_str']} — {ev['summary']} 🔁\n"
    else:
        text += "   Нет задач на сегодня.\n"
        
    text += "\n"
    
    # Завтра (Желтый)
    tomorrow_date = (now + timedelta(days=1))
    tomorrow_day_str = format_date_full(tomorrow_date)
    text += f" **ЗАВТРА ({tomorrow_day_str}):**\n"
    if tomorrow:
        for ev in tomorrow:
            text += f"   • {ev['time_str']} — {ev['summary']}\n"
    else:
        text += "   Нет задач на завтра.\n"
    
    text += f"\n_Последняя синхронизация: {sync_time}_"

    keyboard = get_main_action_keyboard()

    try:
        if MAIN_MESSAGE_ID is None:
            # Если сообщения еще нет, создаем новое
            if message:
                sent_msg = await message.answer(text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)
                MAIN_MESSAGE_ID = sent_msg.message_id
                logger.info("Главное сообщение создано.")
            else:
                # Если вызвано из таймера и сообщения нет (редкий случай), отправляем админу
                sent_msg = await bot.send_message(ADMIN_ID, text, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard)
                MAIN_MESSAGE_ID = sent_msg.message_id
        else:
            # Редактируем существующее
            await bot.edit_message_text(
                chat_id=ADMIN_ID,
                message_id=MAIN_MESSAGE_ID,
                text=text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=keyboard
            )
            logger.debug("Главное сообщение обновлено.")
    except Exception as e:
        logger.error(f"Ошибка при отправке/редактировании главного сообщения: {e}")
        # Если сообщение было удалено пользователем, сбрасываем ID
        if "message to edit not found" in str(e):
            MAIN_MESSAGE_ID = None

# --- ОБРАБОТЧИКИ ---

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await state.clear()
    logger.info("Команда /start получена.")
    await send_or_edit_main_message(message)

@dp.callback_query(F.data == "force_refresh")
async def force_refresh(callback: types.CallbackQuery):
    await callback.answer("Обновление...", show_alert=False)
    await send_or_edit_main_message()

@dp.callback_query(F.data == "add_note_start")
async def start_add_note(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("✍️ **Введите текст новой заметки:**", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(AddNoteState.waiting_for_text)
    await callback.answer()

@dp.message(AddNoteState.waiting_for_text)
async def process_note_text(message: types.Message, state: FSMContext):
    await state.update_data(note_text=message.text)
    await message.answer(
        f"📝 Текст: *{message.text}*\n **Когда оповестить?**",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_time_options_kb()
    )
    await state.set_state(AddNoteState.waiting_for_time)

@dp.callback_query(AddNoteState.waiting_for_time, F.data.startswith("time_"))
async def process_time_selection(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    text = data.get("note_text")
    
    if callback.data == "time_manual":
        await callback.answer("Ручной ввод пока недоступен.", show_alert=True)
        return

    timestamp = int(callback.data.split("_")[1])
    event_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    
    if create_event_in_yandex(text, event_time):
        await callback.message.answer("✅ Заметка успешно добавлена!", reply_markup=None)
        # Обновляем главное сообщение
        await send_or_edit_main_message()
    else:
        await callback.message.answer("❌ Ошибка при добавлении.", reply_markup=None)
    
    await state.clear()

@dp.callback_query(F.data == "cancel_add")
async def cancel_add(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
    await callback.answer("Добавление отменено.")

@dp.callback_query(F.data == "open_settings")
async def open_settings(callback: types.CallbackQuery):
    text = f"⚙️ **Настройки**\nИнтервал проверки: {CHECK_INTERVAL_MINUTES} мин."
    await callback.message.answer(text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_settings_kb(CHECK_INTERVAL_MINUTES))
    await callback.answer()

@dp.callback_query(F.data.startswith("set_interval_"))
async def change_interval(callback: types.CallbackQuery):
    global CHECK_INTERVAL_MINUTES
    new_interval = int(callback.data.split("_")[2])
    CHECK_INTERVAL_MINUTES = new_interval
    logger.info(f"Интервал изменен на {new_interval}")
    
    await callback.message.edit_text(
        f"✅ Интервал установлен: **{new_interval} мин**",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_main_action_keyboard() # Возвращаем к главному меню действий
    )
    # Обновляем и главное информационное сообщение
    await send_or_edit_main_message()

@dp.callback_query(F.data == "close_settings")
async def close_settings(callback: types.CallbackQuery):
    await callback.message.delete()
    await callback.answer()

@dp.callback_query(F.data.startswith("done_"))
async def mark_done(callback: types.CallbackQuery):
    uid = callback.data.split("_")[1]
    if delete_event(uid):
        await callback.message.edit_text("✅ Задача удалена.", reply_markup=None)
        await send_or_edit_main_message()
    else:
        await callback.answer("Не удалось удалить.", show_alert=True)

# --- ПЛАНИРОВЩИК ---
async def background_scheduler():
    while True:
        interval_seconds = CHECK_INTERVAL_MINUTES * 60
        logger.info(f"Сон {CHECK_INTERVAL_MINUTES} мин...")
        await asyncio.sleep(interval_seconds)
        logger.info("Плановое обновление.")
        await send_or_edit_main_message()

async def main():
    logger.info(f"Bot started v{BOT_VERSION}")
    # Небольшая задержка перед стартом, чтобы бот успел подключиться
    await asyncio.sleep(2)
    asyncio.create_task(background_scheduler())
    # При старте пытаемся сразу показать сообщение (если есть ID админа в контексте, но лучше ждать /start)
    # Поэтому ждем команды /start для первого сообщения
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())