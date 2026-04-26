import os
import asyncio
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta, timezone
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
BOT_VERSION = "1.1.2"  # Обновленная версия с исправлениями

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
    raise ValueError("Ошибка: Проверьте .env. Убедитесь, что заполнены все поля, включая YANDEX_LOGIN!")

# --- ЛОГИРОВАНИЕ (Подробное) ---
LOG_FILE = "bot.log"
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Уровень DEBUG для максимальной детализации

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

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

def get_local_time():
    # Получаем текущее время в локальном часовом поясе системы
    return datetime.now().astimezone()

def format_datetime(dt_obj):
    """Форматирует дату и время, гарантируя наличие часов и минут"""
    if dt_obj is None:
        return "Неизвестно"
    
    # Если это объект date (без времени), считаем его 00:00, но лучше конвертировать в datetime
    if isinstance(dt_obj, datetime):
        local_dt = dt_obj.astimezone()
    else:
        # Если пришел просто date, добавляем время 00:00
        local_dt = datetime.combine(dt_obj, datetime.min.time()).replace(tzinfo=timezone.utc).astimezone()
    
    day_name = local_dt.strftime("%A").replace("Monday", "Понедельник").replace("Tuesday", "Вторник").replace("Wednesday", "Среда").replace("Thursday", "Четверг").replace("Friday", "Пятница").replace("Saturday", "Суббота").replace("Sunday", "Воскресенье")
    return f"{day_name}, {local_dt.strftime('%d.%m')} в {local_dt.strftime('%H:%M')}"

# --- РАБОТА С CALDAV ---
def get_calendar():
    try:
        logger.debug("Подключение к CalDAV...")
        client = caldav.DAVClient(url=CALDAV_URL, username=YANDEX_LOGIN, password=YANDEX_PASSWORD)
        principal = client.principal()
        calendars = principal.calendars()
        if not calendars:
            logger.error("Календари не найдены.")
            return None
        logger.info(f"Найден календарь: {calendars[0].name}")
        return calendars[0]
    except Exception as e:
        logger.error(f"CalDAV Error: {e}", exc_info=True)
        return None

def get_events_range(start_date, end_date):
    calendar = get_calendar()
    if not calendar:
        return []
    
    logger.info(f"Поиск событий с {start_date} по {end_date}")
    try:
        # expand=True важно для повторяющихся событий
        events = calendar.date_search(start=start_date, end=end_date, expand=True)
        result = []
        
        for event in events:
            try:
                dt_start = event.instance.vevent.dtstart.value
                summary = event.instance.vevent.summary.value if event.instance.vevent.summary else "Без названия"
                uid = event.instance.vevent.uid.value
                
                logger.debug(f"Найдено событие: {summary} | Время сырое: {dt_start} | Тип: {type(dt_start)}")
                
                # Нормализация времени
                if hasattr(dt_start, 'date') and not hasattr(dt_start, 'hour'):
                    # Это объект date (без времени), добавляем 00:00 UTC и конвертируем в локальное
                    dt_start = datetime.combine(dt_start, datetime.min.time()).replace(tzinfo=timezone.utc)
                elif hasattr(dt_start, 'tzinfo') and dt_start.tzinfo is None:
                    # naive datetime, считаем что это UTC или локальное (зависит от сервера Яндекса, обычно UTC)
                    dt_start = dt_start.replace(tzinfo=timezone.utc)
                
                # Конвертация в локальное время пользователя для отображения
                local_dt = dt_start.astimezone()
                
                result.append({
                    "summary": summary, 
                    "time": local_dt, 
                    "uid": uid,
                    "original_obj": event
                })
            except Exception as e:
                logger.warning(f"Ошибка парсинга события: {e}")
                continue
        
        result.sort(key=lambda x: x['time'])
        logger.info(f"Всего найдено событий: {len(result)}")
        return result
    except Exception as e:
        logger.error(f"Error fetching events: {e}", exc_info=True)
        return []

def delete_event(uid):
    calendar = get_calendar()
    if not calendar: return False
    try:
        logger.info(f"Попытка удаления события UID: {uid}")
        ev = calendar.event_by_uid(uid)
        if ev:
            ev.delete()
            logger.info(f"Событие {uid} успешно удалено.")
            return True
        else:
            logger.warning(f"Событие {uid} не найдено для удаления.")
            return False
    except Exception as e:
        logger.error(f"Delete error: {e}", exc_info=True)
        return False

def create_event_in_yandex(summary, start_dt, duration_hours=1):
    calendar = get_calendar()
    if not calendar: return False
    
    try:
        # Убеждаемся, что время в UTC для отправки в CalDAV
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
        
        logger.info(f"Создание события: {summary} на {dt_str_start}")
        calendar.save_event(event_data)
        logger.info("Событие успешно создано.")
        return True
    except Exception as e:
        logger.error(f"Create error: {e}", exc_info=True)
        return False

# --- КЛАВИАТУРЫ ---

def get_reply_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="➕ Добавить заметку"), KeyboardButton(text="⚙️ Настройки"))
    return builder.as_markup(resize_keyboard=True)

def get_time_options_kb():
    builder = InlineKeyboardBuilder()
    now = get_local_time()
    
    t1 = now + timedelta(hours=1)
    t2 = (now + timedelta(days=1)).replace(hour=9, minute=0, second=0)
    t3 = (now + timedelta(days=1)).replace(hour=18, minute=0, second=0)
    
    builder.button(text=f"⏰ Через 1 час ({t1.strftime('%H:%M')})", callback_data=f"time_{int(t1.timestamp())}")
    builder.button(text=f" Завтра утром ({t2.strftime('%d.%m %H:%M')})", callback_data=f"time_{int(t2.timestamp())}")
    builder.button(text=f"🌆 Завтра вечером ({t3.strftime('%d.%m %H:%M')})", callback_data=f"time_{int(t3.timestamp())}")
    builder.button(text="📅 Выбрать дату вручную", callback_data="time_manual")
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
    builder.adjust(2)
    return builder.as_markup()

# --- ОБРАБОТЧИКИ ---

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    await state.clear()
    logger.info("Обработка команды /start")
    
    now = get_local_time()
    
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = now.replace(hour=23, minute=59, second=59, microsecond=0)
    tomorrow_start = today_end + timedelta(seconds=1)
    tomorrow_end = tomorrow_start + timedelta(days=1)
    past_end = today_start - timedelta(seconds=1)

    # Сбор данных
    overdue = get_events_range(datetime(2020, 1, 1, tzinfo=timezone.utc), past_end)
    today = get_events_range(today_start, today_end)
    tomorrow = get_events_range(tomorrow_start, tomorrow_end)

    msg = f"🤖 **Бот запущен! Версия: {BOT_VERSION}**\n\n"
    
    # 1. Просроченные
    if overdue:
        msg += " 🔴 **ПРОСРОЧЕНО:**\n"
        for ev in overdue:
            date_str = format_datetime(ev['time'])
            msg += f"• {ev['summary']} ({date_str})\n"
            await message.answer(f" **ПРОСРОЧЕНО:** {ev['summary']}\n⏰ Было: {date_str}", 
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

@dp.message(F.text == "➕ Добавить заметку")
async def start_add_note(message: types.Message, state: FSMContext):
    logger.info("Начало добавления заметки")
    await message.answer("️ **Введите текст вашей новой заметки:**", parse_mode=ParseMode.MARKDOWN)
    await state.set_state(AddNoteState.waiting_for_text)

@dp.message(AddNoteState.waiting_for_text)
async def process_note_text(message: types.Message, state: FSMContext):
    await state.update_data(note_text=message.text)
    logger.info(f"Текст заметки получен: {message.text}")
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
        await callback.message.answer("Функция ручного ввода даты пока в разработке.", show_alert=True)
        return

    timestamp = int(callback.data.split("_")[1])
    event_time = datetime.fromtimestamp(timestamp, tz=timezone.utc) # Сохраняем как UTC
    
    logger.info(f"Создание события: {text} на время {event_time}")
    
    if create_event_in_yandex(text, event_time):
        date_str = format_datetime(event_time)
        
        # ИСПРАВЛЕНИЕ: Используем InlineKeyboardMarkup вместо ReplyKeyboardMarkup для edit_text
        # Или просто отправляем новое сообщение с ReplyKeyboard
        try:
            await callback.message.edit_text(
                f"✅ **Заметка добавлена!**\n{text}\n⏰ На: {date_str}",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=None # Убираем старую клавиатуру
            )
            # Отправляем новое сообщение с главной клавиатурой
            await callback.message.answer("Что делаем дальше?", reply_markup=get_reply_keyboard())
        except Exception as e:
            logger.error(f"Ошибка редактирования сообщения: {e}")
            await callback.message.answer(f"✅ Заметка добавлена!\n{text}\n На: {date_str}", reply_markup=get_reply_keyboard())
            
        logger.info("Заметка успешно добавлена и уведомление отправлено.")
    else:
        await callback.message.answer("❌ Ошибка при добавлении в календарь.", reply_markup=get_reply_keyboard())
    
    await state.clear()

@dp.message(F.text == "⚙️ Настройки")
async def open_settings(message: types.Message):
    logger.info("Открытие настроек")
    global CHECK_INTERVAL_MINUTES
    text = f"️ **Настройки бота**\n\nТекущий интервал обновления: **{CHECK_INTERVAL_MINUTES} мин**\nВыберите новый интервал:"
    await message.answer(text, parse_mode=ParseMode.MARKDOWN, reply_markup=get_settings_kb(CHECK_INTERVAL_MINUTES))

@dp.callback_query(F.data.startswith("set_interval_"))
async def change_interval(callback: types.CallbackQuery):
    global CHECK_INTERVAL_MINUTES
    new_interval = int(callback.data.split("_")[2])
    CHECK_INTERVAL_MINUTES = new_interval
    
    logger.info(f"Интервал изменен на {new_interval} мин")
    
    await callback.message.edit_text(
        f"✅ Интервал обновлен: **{new_interval} мин**",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_reply_keyboard() # Возвращаем главную клавиатуру
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("done_"))
async def mark_done(callback: types.CallbackQuery):
    uid = callback.data.split("_")[1]
    logger.info(f"Получен запрос на удаление задачи: {uid}")
    
    if delete_event(uid):
        await callback.message.edit_text("✅ Задача выполнена и удалена из календаря.", reply_markup=get_reply_keyboard())
        await callback.answer("Удалено!")
    else:
        await callback.answer("Не удалось удалить задачу.", show_alert=True)

# --- ПЛАНИРОВЩИК ---
async def background_scheduler():
    while True:
        interval_seconds = CHECK_INTERVAL_MINUTES * 60
        logger.info(f"Сон на {CHECK_INTERVAL_MINUTES} минут...")
        await asyncio.sleep(interval_seconds)
        logger.info("Запуск плановой проверки календаря...")
        # Здесь можно добавить логику авто-уведомлений

async def main():
    logger.info(f"Bot started v{BOT_VERSION}")
    asyncio.create_task(background_scheduler())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())