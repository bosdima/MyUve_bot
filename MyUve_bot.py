import asyncio
import json
import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
from urllib.parse import urlencode
import pytz
import re
import caldav
import hashlib

import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils import executor
from dotenv import load_dotenv

# Настройка логирования
log_file = 'bot_debug.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Версия бота
BOT_VERSION = "1.4"
BOT_VERSION_DATE = "19.04.2026"

# Загрузка переменных окружения
load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN')
ADMIN_ID = int(os.getenv('ADMIN_ID')) if os.getenv('ADMIN_ID') else None

# CalDAV переменные
YANDEX_EMAIL = os.getenv('YANDEX_EMAIL')
YANDEX_APP_PASSWORD = os.getenv('YANDEX_APP_PASSWORD')
YANDEX_CALDAV_URL = "https://caldav.yandex.ru"

if not BOT_TOKEN:
    logger.error("❌ Ошибка: BOT_TOKEN не задан!")
    exit(1)

storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot, storage=storage)
dp.middleware.setup(LoggingMiddleware())

DATA_FILE = 'notifications.json'
CONFIG_FILE = 'config.json'

notifications: Dict = {}
pending_notifications: Dict = {}
config: Dict = {}
notifications_enabled = True
calendar_events_cache: Dict[str, List[Dict]] = {}
last_calendar_update = {}
event_id_map: Dict[str, str] = {}
last_sync_time: Optional[datetime] = None  # Время последней синхронизации

TIMEZONES = {
    'Москва (UTC+3)': 'Europe/Moscow',
    'Санкт-Петербург (UTC+3)': 'Europe/Moscow',
    'Калининград (UTC+2)': 'Europe/Kaliningrad',
    'Екатеринбург (UTC+5)': 'Asia/Yekaterinburg',
    'Новосибирск (UTC+7)': 'Asia/Novosibirsk',
    'Красноярск (UTC+7)': 'Asia/Krasnoyarsk',
    'Иркутск (UTC+8)': 'Asia/Irkutsk',
    'Владивосток (UTC+10)': 'Asia/Vladivostok',
    'Магадан (UTC+11)': 'Asia/Magadan',
    'Камчатка (UTC+12)': 'Asia/Kamchatka'
}

WEEKDAYS_BUTTONS = [("Пн", 0), ("Вт", 1), ("Ср", 2), ("Чт", 3), ("Пт", 4), ("Сб", 5), ("Вс", 6)]
WEEKDAYS_NAMES = {0: "Понедельник", 1: "Вторник", 2: "Среда", 3: "Четверг", 4: "Пятница", 5: "Суббота", 6: "Воскресенье"}

MONTHS_NAMES = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля",
    5: "мая", 6: "июня", 7: "июля", 8: "августа",
    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря"
}


def get_current_time():
    timezone_str = config.get('timezone', 'Europe/Moscow')
    tz = pytz.timezone(timezone_str)
    return datetime.now(tz)


def parse_datetime(date_str: str) -> Optional[datetime]:
    full_str = date_str.strip()
    now = get_current_time()
    current_year = now.year
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    
    match = re.match(r'^(\d{1,2})\.(\d{1,2})\.(\d{2,4})\s+(\d{1,2}):(\d{2})$', full_str)
    if match:
        day, month, year, hour, minute = match.groups()
        year = int(year)
        if year < 100:
            year = 2000 + year
        try:
            return tz.localize(datetime(year, int(month), int(day), int(hour), int(minute)))
        except:
            return None
    
    match = re.match(r'^(\d{1,2})\.(\d{1,2})\s+(\d{1,2}):(\d{2})$', full_str)
    if match:
        day, month, hour, minute = match.groups()
        year = current_year
        try:
            result = tz.localize(datetime(year, int(month), int(day), int(hour), int(minute)))
            if result < now:
                result = tz.localize(datetime(year + 1, int(month), int(day), int(hour), int(minute)))
            return result
        except:
            return None
    
    match = re.match(r'^(\d{1,2})\.(\d{1,2})\.(\d{2,4})$', full_str)
    if match:
        day, month, year = match.groups()
        year = int(year)
        if year < 100:
            year = 2000 + year
        try:
            return tz.localize(datetime(year, int(month), int(day), now.hour, now.minute))
        except:
            return None
    
    match = re.match(r'^(\d{1,2})\.(\d{1,2})$', full_str)
    if match:
        day, month = match.groups()
        try:
            result = tz.localize(datetime(current_year, int(month), int(day), now.hour, now.minute))
            if result < now:
                result = tz.localize(datetime(current_year + 1, int(month), int(day), now.hour, now.minute))
            return result
        except:
            return None
    
    return None


def get_next_weekday(target_weekdays: List[int], hour: int, minute: int, from_date: datetime = None) -> Optional[datetime]:
    now = from_date or get_current_time()
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    
    if now.tzinfo is None:
        now = tz.localize(now)
    
    if now.weekday() in target_weekdays:
        today_trigger = tz.localize(datetime(now.year, now.month, now.day, hour, minute))
        if today_trigger > now:
            return today_trigger
    
    for i in range(1, 15):
        next_date = now + timedelta(days=i)
        if next_date.weekday() in target_weekdays:
            result = tz.localize(datetime(next_date.year, next_date.month, next_date.day, hour, minute))
            return result
    
    return None


class CalDAVCalendarAPI:
    def __init__(self, email: str, app_password: str):
        self.email = email
        self.app_password = app_password
        self.client = None
        self.principal = None
        self.calendar = None
    
    def _connect(self) -> bool:
        try:
            self.client = caldav.DAVClient(url=YANDEX_CALDAV_URL, username=self.email, password=self.app_password)
            self.principal = self.client.principal()
            return True
        except Exception as e:
            logger.error(f"Ошибка подключения к CalDAV: {e}")
            return False
    
    def get_default_calendar(self):
        if not self._connect():
            return None
        try:
            calendars = self.principal.calendars()
            if calendars:
                self.calendar = calendars[0]
                return self.calendar
            return None
        except Exception as e:
            logger.error(f"Ошибка получения календаря: {e}")
            return None
    
    async def test_connection(self) -> tuple[bool, str]:
        try:
            if not self._connect():
                return False, "Не удалось подключиться к CalDAV серверу. Проверьте интернет-соединение."
            calendars = self.principal.calendars()
            if calendars:
                return True, f"CalDAV подключен, найдено {len(calendars)} календарей"
            return False, "Календари не найдены. Создайте календарь на calendar.yandex.ru"
        except caldav.lib.error.AuthorizationError:
            return False, "Ошибка авторизации! Получите новый пароль приложения на https://id.yandex.ru/security/app-passwords"
        except Exception as e:
            error_msg = str(e)
            if "401" in error_msg or "Unauthorized" in error_msg:
                return False, "Неверный пароль приложения. Получите новый пароль на https://id.yandex.ru/security/app-passwords"
            return False, f"Ошибка подключения: {str(e)[:150]}"
    
    async def create_event(self, summary: str, start_time: datetime, description: str = "") -> Optional[str]:
        try:
            calendar = self.get_default_calendar()
            if not calendar:
                return None
            
            end_time = start_time + timedelta(hours=1)
            tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
            if start_time.tzinfo is None:
                start_time = tz.localize(start_time)
            if end_time.tzinfo is None:
                end_time = tz.localize(end_time)
            
            start_str = start_time.strftime('%Y%m%dT%H%M%S')
            end_str = end_time.strftime('%Y%m%dT%H%M%S')
            tzid = config.get('timezone', 'Europe/Moscow')
            
            ical_data = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//MyUved Bot//Calendar//RU
BEGIN:VEVENT
UID:{datetime.now().timestamp()}@myuved.bot
DTSTAMP:{datetime.now().strftime('%Y%m%dT%H%M%SZ')}
DTSTART;TZID={tzid}:{start_str}
DTEND;TZID={tzid}:{end_str}
SUMMARY:{summary[:255]}
DESCRIPTION:{description[:500]}
END:VEVENT
END:VCALENDAR"""
            
            event = calendar.save_event(ical_data)
            if event:
                logger.info(f"Создано событие в календаре (CalDAV): {summary}")
                return str(event.url)
            return None
        except Exception as e:
            logger.error(f"Ошибка создания события в CalDAV: {e}")
            return None
    
    async def delete_event(self, event_url: str) -> bool:
        try:
            calendar = self.get_default_calendar()
            if not calendar:
                return False
            events = calendar.events()
            for event in events:
                if str(event.url) == event_url:
                    event.delete()
                    logger.info(f"Удалено событие из календаря (CalDAV): {event_url}")
                    return True
            return False
        except Exception as e:
            logger.error(f"Ошибка удаления события из CalDAV: {e}")
            return False
    
    async def update_event(self, event_url: str, new_summary: str = None, new_start: datetime = None) -> bool:
        try:
            delete_success = await self.delete_event(event_url)
            if not delete_success:
                logger.error(f"Не удалось удалить старое событие: {event_url}")
                return False
            
            if new_summary and new_start:
                new_id = await self.create_event(new_summary, new_start, "Обновлено через бота")
                if new_id:
                    logger.info(f"Событие обновлено: новое ID={new_id}")
                    return True
                else:
                    logger.error("Не удалось создать новое событие")
                    return False
            return False
        except Exception as e:
            logger.error(f"Ошибка обновления события: {e}")
            return False
    
    async def get_events(self, from_date: datetime, to_date: datetime) -> List[Dict]:
        try:
            calendar = self.get_default_calendar()
            if not calendar:
                return []
            
            tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
            if from_date.tzinfo is None:
                from_date = tz.localize(from_date)
            if to_date.tzinfo is None:
                to_date = tz.localize(to_date)
            
            from_utc = from_date.astimezone(pytz.UTC)
            to_utc = to_date.astimezone(pytz.UTC)
            
            # Используем search вместо date_search
            events = calendar.search(start=from_utc, end=to_utc, expand=True)
            
            result = []
            for event in events:
                try:
                    vcal = event.vobject_instance
                    vevent = vcal.vevent
                    dtstart = vevent.dtstart.value
                    if hasattr(dtstart, 'dt'):
                        dtstart = dtstart.dt
                    if dtstart.tzinfo is None:
                        dtstart = tz.localize(dtstart)
                    else:
                        dtstart = dtstart.astimezone(tz)
                    
                    result.append({
                        'id': str(event.url),
                        'summary': str(vevent.summary.value) if hasattr(vevent, 'summary') else 'Без названия',
                        'start': dtstart.isoformat(),
                        'description': str(vevent.description.value) if hasattr(vevent, 'description') else ''
                    })
                except Exception as e:
                    continue
            return result
        except Exception as e:
            logger.error(f"Ошибка получения событий: {e}")
            return []
    
    async def get_month_events(self, year: int, month: int) -> List[Dict]:
        tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
        start_date = tz.localize(datetime(year, month, 1, 0, 0, 0))
        if month == 12:
            end_date = tz.localize(datetime(year + 1, 1, 1, 0, 0, 0)) - timedelta(seconds=1)
        else:
            end_date = tz.localize(datetime(year, month + 1, 1, 0, 0, 0)) - timedelta(seconds=1)
        return await self.get_events(start_date, end_date)


def get_caldav_available() -> bool:
    return bool(YANDEX_EMAIL and YANDEX_APP_PASSWORD)


async def check_caldav_connection() -> tuple[bool, str]:
    if not get_caldav_available():
        return False, "CalDAV не настроен. Добавьте YANDEX_EMAIL и YANDEX_APP_PASSWORD в .env файл"
    
    caldav_api = CalDAVCalendarAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
    return await caldav_api.test_connection()


async def update_calendar_events_cache(year: int, month: int, force: bool = False):
    global calendar_events_cache, last_calendar_update, last_sync_time
    cache_key = f"{year}_{month}"
    now = get_current_time()
    
    if cache_key in last_calendar_update and not force:
        last_update = last_calendar_update[cache_key]
        if (now - last_update).total_seconds() < 300:
            return
    
    if not get_caldav_available():
        return
    
    try:
        caldav_api = CalDAVCalendarAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
        events = await caldav_api.get_month_events(year, month)
        events.sort(key=lambda x: x.get('start', ''))
        calendar_events_cache[cache_key] = events
        last_calendar_update[cache_key] = now
        last_sync_time = now  # Обновляем время последней синхронизации
        
        # Очистка старых кэшей
        keys_to_remove = [k for k in calendar_events_cache.keys() if k != cache_key]
        for key in keys_to_remove:
            if len(calendar_events_cache) > 3:
                del calendar_events_cache[key]
        
        logger.info(f"Обновлён кэш календаря для {year}.{month}: {len(events)} событий")
    except Exception as e:
        logger.error(f"Ошибка обновления кэша календаря: {e}")


async def get_formatted_calendar_events(year: int, month: int, force_refresh: bool = False) -> str:
    global last_sync_time
    if force_refresh:
        await update_calendar_events_cache(year, month, force=True)
    else:
        await update_calendar_events_cache(year, month)
    
    cache_key = f"{year}_{month}"
    events = calendar_events_cache.get(cache_key, [])
    now = get_current_time()
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    
    # Показываем все будущие события (включая завтра, послезавтра и т.д.)
    # Не ограничиваем только сегодняшними
    future_events = []
    
    for event in events:
        try:
            start_dt = datetime.fromisoformat(event['start'])
            if start_dt.tzinfo is None:
                start_dt = tz.localize(start_dt)
            else:
                start_dt = start_dt.astimezone(tz)
            
            # Показываем события, которые еще не начались
            if start_dt >= now:
                future_events.append((start_dt, event))
            # Также показываем события, которые начались сегодня, но еще не прошли
            elif start_dt.date() == now.date() and start_dt >= now.replace(hour=0, minute=0, second=0):
                future_events.append((start_dt, event))
        except Exception as e:
            logger.error(f"Ошибка обработки события: {e}")
            continue
    
    if not future_events:
        text = f"📅 **Нет предстоящих событий на {MONTHS_NAMES[month]} {year}**"
    else:
        future_events.sort(key=lambda x: x[0])
        text = f"📅 **СОБЫТИЯ КАЛЕНДАРЯ**\n📆 {MONTHS_NAMES[month]} {year}\n\n"
        
        for start_dt, event in future_events[:30]:  # Показываем до 30 событий
            day = start_dt.day
            month_num = start_dt.month
            year_num = start_dt.year
            time_str = start_dt.strftime('%H:%M')
            summary = event['summary']
            weekday_name = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"][start_dt.weekday()]
            
            # Определяем, сегодня это или нет
            if start_dt.date() == now.date():
                prefix = "🔴 СЕГОДНЯ"
            elif start_dt.date() == now.date() + timedelta(days=1):
                prefix = "🟠 ЗАВТРА"
            elif start_dt.date() == now.date() + timedelta(days=2):
                prefix = "🟡 ПОСЛЕЗАВТРА"
            else:
                prefix = "📌"
            
            text += f"{prefix} {day:02d}.{month_num:02d}.{year_num} ({weekday_name}) в {time_str} — **{summary}**\n"
        
        if len(future_events) > 30:
            text += f"\n... и еще {len(future_events) - 30} событий"
    
    # Добавляем информацию о последней синхронизации
    if last_sync_time:
        sync_str = last_sync_time.strftime('%d.%m.%Y %H:%M:%S')
        text += f"\n\n🔄 *Последняя синхронизация:* {sync_str}"
    
    return text


async def show_calendar_events(chat_id: int, year: int = None, month: int = None, force_refresh: bool = False, persistent: bool = False):
    if year is None or month is None:
        now = get_current_time()
        year = now.year
        month = now.month
    formatted_events = await get_formatted_calendar_events(year, month, force_refresh)
    keyboard = InlineKeyboardMarkup(row_width=3)
    prev_month = month - 1
    prev_year = year
    if prev_month < 1:
        prev_month = 12
        prev_year = year - 1
    next_month = month + 1
    next_year = year
    if next_month > 12:
        next_month = 1
        next_year = year + 1
    keyboard.add(
        InlineKeyboardButton("◀️", callback_data=f"cal_prev_{prev_year}_{prev_month}"),
        InlineKeyboardButton(f"{MONTHS_NAMES[month]} {year}", callback_data="curr_month"),
        InlineKeyboardButton("▶️", callback_data=f"cal_next_{next_year}_{next_month}")
    )
    keyboard.add(
        InlineKeyboardButton("🔄 Обновить", callback_data=f"cal_refresh_{year}_{month}"),
        InlineKeyboardButton("📥 Синхр.", callback_data=f"cal_sync_{year}_{month}"),
        InlineKeyboardButton("✏️ Ред.", callback_data="edit_calendar")
    )
    if persistent:
        await send_persistent_message(chat_id, formatted_events, reply_markup=keyboard)
    else:
        await send_with_auto_delete(chat_id, formatted_events, reply_markup=keyboard, delay=3600)


class NotificationStates(StatesGroup):
    waiting_for_text = State()
    waiting_for_time_type = State()
    waiting_for_hours = State()
    waiting_for_days = State()
    waiting_for_months = State()
    waiting_for_specific_date = State()
    waiting_for_weekdays = State()
    waiting_for_weekday_time = State()
    waiting_for_every_day_time = State()
    waiting_for_every_hour_time = State()
    waiting_for_edit_text = State()
    waiting_for_edit_time = State()
    waiting_for_edit_new_time = State()


class SnoozeStates(StatesGroup):
    waiting_for_snooze_type = State()
    waiting_for_hours = State()
    waiting_for_days = State()
    waiting_for_specific_date = State()


class SettingsStates(StatesGroup):
    waiting_for_max_backups = State()
    waiting_for_check_time = State()
    waiting_for_timezone = State()


class EditCalendarEventStates(StatesGroup):
    waiting_for_event_selection = State()
    waiting_for_new_text = State()
    waiting_for_new_time = State()
    waiting_for_new_date = State()
    waiting_for_new_datetime = State()


def init_folders():
    if not os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump({}, f)
    if not os.path.exists(CONFIG_FILE):
        default_config = {
            'max_backups': 5,
            'daily_check_time': '06:00',
            'notifications_enabled': True,
            'timezone': 'Europe/Moscow',
            'calendar_sync_enabled': True,
            'calendar_update_interval': 15,
            'auto_show_calendar': True
        }
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(default_config, f)


def load_data():
    global notifications, pending_notifications, config, notifications_enabled
    with open(DATA_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
        notifications = data.get('notifications', {})
        pending_notifications = data.get('pending_notifications', {})
    
    # Очистка завершенных
    notifications = {k: v for k, v in notifications.items() if not v.get('is_completed', False)}
    pending_notifications = {k: v for k, v in pending_notifications.items() if not v.get('is_completed', False)}
    
    for notif_id, notif in notifications.items():
        if 'is_completed' not in notif:
            notif['is_completed'] = False
        if 'last_repeat_time' not in notif:
            notif['last_repeat_time'] = None
        if 'repeat_count' not in notif:
            notif['repeat_count'] = 0
        if 'is_repeat' not in notif:
            notif['is_repeat'] = False
        if 'last_trigger' not in notif:
            notif['last_trigger'] = None
        if 'reminder_sent' not in notif:
            notif['reminder_sent'] = False
        if 'last_reminder_time' not in notif:
            notif['last_reminder_time'] = None
    
    for notif_id, notif in pending_notifications.items():
        if 'is_completed' not in notif:
            notif['is_completed'] = False
        if 'last_repeat_time' not in notif:
            notif['last_repeat_time'] = None
        if 'repeat_count' not in notif:
            notif['repeat_count'] = 0
        if 'is_repeat' not in notif:
            notif['is_repeat'] = False
        if 'last_trigger' not in notif:
            notif['last_trigger'] = None
        if 'reminder_sent' not in notif:
            notif['reminder_sent'] = False
        if 'last_reminder_time' not in notif:
            notif['last_reminder_time'] = None
    
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        config = json.load(f)
        notifications_enabled = config.get('notifications_enabled', True)
    
    logger.info(f"Загружено уведомлений: {len(notifications)}, неотмеченных: {len(pending_notifications)}")


def save_data():
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump({
            'notifications': notifications,
            'pending_notifications': pending_notifications
        }, f, indent=2, ensure_ascii=False)
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    logger.info(f"Сохранено уведомлений: {len(notifications)}, неотмеченных: {len(pending_notifications)}")


async def sync_calendar_to_pending():
    if not get_caldav_available() or not config.get('calendar_sync_enabled', True):
        return
    
    now = get_current_time()
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    year = now.year
    month = now.month
    
    await update_calendar_events_cache(year, month)
    cache_key = f"{year}_{month}"
    events = calendar_events_cache.get(cache_key, [])
    
    # Показываем все будущие события, а не только с сегодняшнего дня
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    for event in events:
        try:
            start_dt = datetime.fromisoformat(event['start'])
            if start_dt.tzinfo is None:
                start_dt = tz.localize(start_dt)
            else:
                start_dt = start_dt.astimezone(tz)
            
            # Добавляем события, которые еще не начались или начались сегодня
            if start_dt >= today_start:
                event_id = event['id']
                summary = event['summary']
                
                exists = False
                for nid, notif in pending_notifications.items():
                    if notif.get('calendar_event_id') == event_id:
                        exists = True
                        break
                if not exists:
                    for nid, notif in notifications.items():
                        if notif.get('calendar_event_id') == event_id:
                            exists = True
                            break
                
                if not exists:
                    notif_id = f"pending_{int(start_dt.timestamp())}_{hashlib.md5(event_id.encode()).hexdigest()[:8]}"
                    pending_notifications[notif_id] = {
                        'text': summary,
                        'time': start_dt.isoformat(),
                        'created': get_current_time().isoformat(),
                        'calendar_event_id': event_id,
                        'is_completed': False,
                        'reminder_sent': False,
                        'repeat_count': 0,
                        'last_reminder_time': None,
                        'is_pending': True
                    }
                    save_data()
                    logger.info(f"Событие {summary} добавлено в неотмеченные уведомления")
        except Exception as e:
            logger.error(f"Ошибка синхронизации события: {e}")


async def sync_notification_to_calendar(notif_id: str, action: str = 'create'):
    if not config.get('calendar_sync_enabled', True):
        return
    if not get_caldav_available():
        return
    
    notif = notifications.get(notif_id) or pending_notifications.get(notif_id)
    if not notif:
        return
    
    caldav_api = CalDAVCalendarAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
    
    try:
        if action == 'create':
            event_time_str = notif.get('time')
            if not event_time_str:
                return
            event_time = datetime.fromisoformat(event_time_str)
            if event_time.tzinfo is None:
                tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
                event_time = tz.localize(event_time)
            
            description = f"Уведомление из бота\nТекст: {notif['text']}\n"
            description += f"\nСоздано: {get_current_time().strftime('%d.%m.%Y %H:%M')}"
            
            event_id = await caldav_api.create_event(summary=notif['text'][:100], start_time=event_time, description=description)
            if event_id:
                notif['calendar_event_id'] = event_id
                save_data()
                logger.info(f"Уведомление {notif_id} синхронизировано с календарём")
                now = get_current_time()
                await update_calendar_events_cache(now.year, now.month, force=True)
        
        elif action == 'delete':
            if 'calendar_event_id' in notif:
                await caldav_api.delete_event(notif['calendar_event_id'])
                if 'calendar_event_id' in notif:
                    del notif['calendar_event_id']
                save_data()
                logger.info(f"Уведомление {notif_id} удалено из календаря")
                now = get_current_time()
                await update_calendar_events_cache(now.year, now.month, force=True)
    
    except Exception as e:
        logger.error(f"Ошибка синхронизации с календарём: {e}")


async def auto_delete_message(chat_id: int, message_id: int, delay: int = 3600):
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception as e:
        logger.debug(f"Не удалось удалить сообщение {message_id}: {e}")


async def send_with_auto_delete(chat_id: int, text: str, parse_mode: str = 'Markdown', reply_markup=None, delay: int = 3600):
    try:
        msg = await bot.send_message(chat_id, text, parse_mode=parse_mode, reply_markup=reply_markup)
        asyncio.create_task(auto_delete_message(chat_id, msg.message_id, delay))
        return msg
    except Exception as e:
        logger.error(f"Ошибка отправки сообщения: {e}")
        return None


async def send_persistent_message(chat_id: int, text: str, parse_mode: str = 'Markdown', reply_markup=None):
    """Отправляет сообщение без автоудаления"""
    try:
        return await bot.send_message(chat_id, text, parse_mode=parse_mode, reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"Ошибка отправки постоянного сообщения: {e}")
        return None


async def delete_user_message(message: types.Message, delay: int = 3600):
    asyncio.create_task(auto_delete_message(message.chat.id, message.message_id, delay))


async def show_pending_notification_actions(chat_id: int, notif_id: str, notif_text: str, repeat_count: int = 0):
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Выполнено (удалить)", callback_data=f"pend_done_{notif_id}"),
        InlineKeyboardButton("✏️ Изменить время/дату", callback_data=f"pend_edit_{notif_id}"),
        InlineKeyboardButton("📅 Отложить", callback_data=f"pend_snooze_{notif_id}"),
        InlineKeyboardButton("❌ Отложить на час", callback_data=f"pend_hour_{notif_id}")
    )
    
    repeat_text = f" (повтор #{repeat_count})" if repeat_count > 0 else ""
    await bot.send_message(
        chat_id,
        f"🔔 **НЕОТМЕЧЕННОЕ НАПОМИНАНИЕ!**{repeat_text}\n\n"
        f"📝 {notif_text}\n\n"
        f"⏰ Время истекло! Пожалуйста, отметьте выполнение или измените время.\n\n"
        f"⚠️ Напоминание будет повторяться каждый час, пока вы не отметите его как выполненное.",
        reply_markup=keyboard,
        parse_mode='Markdown'
    )


async def check_pending_notifications():
    global notifications_enabled
    while True:
        if notifications_enabled:
            now = get_current_time()
            tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
            
            for notif_id, notif in list(pending_notifications.items()):
                if notif.get('is_completed', False):
                    continue
                
                notify_time = datetime.fromisoformat(notif['time'])
                if notify_time.tzinfo is None:
                    notify_time = tz.localize(notify_time)
                else:
                    notify_time = notify_time.astimezone(tz)
                
                last_reminder = notif.get('last_reminder_time')
                if last_reminder:
                    last_reminder_time = datetime.fromisoformat(last_reminder)
                    if last_reminder_time.tzinfo is None:
                        last_reminder_time = tz.localize(last_reminder_time)
                else:
                    last_reminder_time = None
                
                # Первое уведомление
                if not notif.get('reminder_sent', False) and now >= notify_time:
                    await show_pending_notification_actions(ADMIN_ID, notif_id, notif['text'])
                    notif['reminder_sent'] = True
                    notif['last_reminder_time'] = now.isoformat()
                    notif['repeat_count'] = 1
                    save_data()
                    logger.info(f"Отправлено первое уведомление для неотмеченного #{notif_id}")
                
                # Повторные уведомления каждый час
                elif notif.get('reminder_sent', False) and not notif.get('is_completed', False):
                    if last_reminder_time is None:
                        last_reminder_time = notify_time
                    
                    time_since_last = (now - last_reminder_time).total_seconds()
                    if time_since_last >= 3600:  # Каждый час
                        repeat_count = notif.get('repeat_count', 0) + 1
                        await show_pending_notification_actions(ADMIN_ID, notif_id, notif['text'], repeat_count)
                        notif['last_reminder_time'] = now.isoformat()
                        notif['repeat_count'] = repeat_count
                        save_data()
                        logger.info(f"Отправлено повторное уведомление #{repeat_count} для неотмеченного #{notif_id}")
        
        await asyncio.sleep(30)


async def check_regular_notifications():
    global notifications_enabled
    while True:
        if notifications_enabled:
            now = get_current_time()
            tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
            
            for notif_id, notif in list(notifications.items()):
                if notif.get('is_completed', False):
                    continue
                
                repeat_type = notif.get('repeat_type', 'no')
                
                if repeat_type == 'every_hour':
                    last_trigger = notif.get('last_trigger')
                    if last_trigger:
                        last_trigger_time = datetime.fromisoformat(last_trigger)
                        if last_trigger_time.tzinfo is None:
                            last_trigger_time = tz.localize(last_trigger_time)
                    else:
                        created_str = notif.get('created')
                        if created_str:
                            created_time = datetime.fromisoformat(created_str)
                            if created_time.tzinfo is None:
                                created_time = tz.localize(created_time)
                            last_trigger_time = created_time
                        else:
                            last_trigger_time = now - timedelta(hours=1)
                    
                    if (now - last_trigger_time).total_seconds() >= 3600:
                        repeat_count = notif.get('repeat_count', 0)
                        await show_pending_notification_actions(ADMIN_ID, notif_id, notif['text'], repeat_count + 1)
                        notif['last_trigger'] = now.isoformat()
                        notif['repeat_count'] = repeat_count + 1
                        notif['is_repeat'] = True
                        # Переносим в pending_notifications
                        pending_notifications[notif_id] = notif.copy()
                        pending_notifications[notif_id]['reminder_sent'] = True
                        pending_notifications[notif_id]['last_reminder_time'] = now.isoformat()
                        del notifications[notif_id]
                        save_data()
                        logger.info(f"Уведомление #{notif_id} перенесено в неотмеченные (каждый час)")
                
                elif repeat_type == 'every_day':
                    hour = notif.get('repeat_hour', 0)
                    minute = notif.get('repeat_minute', 0)
                    today_trigger = tz.localize(datetime(now.year, now.month, now.day, hour, minute, 0))
                    last_trigger = notif.get('last_trigger')
                    
                    if last_trigger:
                        last_trigger_time = datetime.fromisoformat(last_trigger)
                        if last_trigger_time.tzinfo is None:
                            last_trigger_time = tz.localize(last_trigger_time)
                    else:
                        last_trigger_time = None
                    
                    should_send = False
                    if last_trigger_time is None:
                        should_send = now >= today_trigger
                    else:
                        if last_trigger_time.date() < now.date() and now >= today_trigger:
                            should_send = True
                    
                    if should_send:
                        repeat_count = notif.get('repeat_count', 0)
                        await show_pending_notification_actions(ADMIN_ID, notif_id, notif['text'], repeat_count + 1)
                        notif['last_trigger'] = now.isoformat()
                        notif['repeat_count'] = repeat_count + 1
                        notif['is_repeat'] = True
                        # Переносим в pending_notifications
                        pending_notifications[notif_id] = notif.copy()
                        pending_notifications[notif_id]['reminder_sent'] = True
                        pending_notifications[notif_id]['last_reminder_time'] = now.isoformat()
                        del notifications[notif_id]
                        save_data()
                        logger.info(f"Уведомление #{notif_id} перенесено в неотмеченные (ежедневно)")
                
                elif repeat_type == 'weekdays':
                    hour = notif.get('repeat_hour', 0)
                    minute = notif.get('repeat_minute', 0)
                    weekdays_list = notif.get('weekdays_list', [])
                    last_trigger = notif.get('last_trigger')
                    
                    if last_trigger:
                        last_trigger_time = datetime.fromisoformat(last_trigger)
                        if last_trigger_time.tzinfo is None:
                            last_trigger_time = tz.localize(last_trigger_time)
                    else:
                        last_trigger_time = None
                    
                    if now.weekday() in weekdays_list:
                        today_trigger = tz.localize(datetime(now.year, now.month, now.day, hour, minute, 0))
                        already_sent_today = last_trigger_time and last_trigger_time.date() == now.date()
                        
                        if now >= today_trigger and not already_sent_today:
                            repeat_count = notif.get('repeat_count', 0)
                            await show_pending_notification_actions(ADMIN_ID, notif_id, notif['text'], repeat_count + 1)
                            notif['last_trigger'] = now.isoformat()
                            notif['repeat_count'] = repeat_count + 1
                            notif['is_repeat'] = True
                            # Переносим в pending_notifications
                            pending_notifications[notif_id] = notif.copy()
                            pending_notifications[notif_id]['reminder_sent'] = True
                            pending_notifications[notif_id]['last_reminder_time'] = now.isoformat()
                            del notifications[notif_id]
                            save_data()
                            logger.info(f"Уведомление #{notif_id} перенесено в неотмеченные (по дням недели)")
                
                elif repeat_type == 'no' and notif.get('time') and not notif.get('reminder_sent', False):
                    notify_time = datetime.fromisoformat(notif['time'])
                    if notify_time.tzinfo is None:
                        notify_time = tz.localize(notify_time)
                    else:
                        notify_time = notify_time.astimezone(tz)
                    
                    if now >= notify_time:
                        await show_pending_notification_actions(ADMIN_ID, notif_id, notif['text'])
                        notif['reminder_sent'] = True
                        notif['last_reminder_time'] = now.isoformat()
                        notif['repeat_count'] = 1
                        # Переносим в pending_notifications
                        pending_notifications[notif_id] = notif.copy()
                        pending_notifications[notif_id]['reminder_sent'] = True
                        pending_notifications[notif_id]['last_reminder_time'] = now.isoformat()
                        del notifications[notif_id]
                        save_data()
                        logger.info(f"Уведомление #{notif_id} перенесено в неотмеченные (одноразовое)")
        
        await asyncio.sleep(30)


async def sync_calendar_task():
    while True:
        try:
            await sync_calendar_to_pending()
            logger.info("Синхронизация календаря с неотмеченными уведомлениями выполнена")
        except Exception as e:
            logger.error(f"Ошибка синхронизации: {e}")
        await asyncio.sleep(300)


def get_main_keyboard():
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        KeyboardButton("➕ Добавить"),
        KeyboardButton("📋 Список"),
        KeyboardButton("📅 События")
    )
    keyboard.add(
        KeyboardButton("⚠️ Неотмеченные"),
        KeyboardButton("⚙️ Настройки")
    )
    return keyboard


async def update_notifications_list(chat_id: int, persistent: bool = False):
    local_text = ""
    if notifications:
        now = get_current_time()
        tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
        sorted_notifs = []
        for notif_id, notif in notifications.items():
            if notif.get('is_completed', False):
                continue
            repeat_type = notif.get('repeat_type', 'no')
            is_passed = False
            time_str = ""
            sort_time = None
            if repeat_type == 'every_hour':
                time_str = "🕐 Каждый час"
                is_passed = False
                sort_time = datetime.now()
            elif repeat_type == 'every_day':
                hour = notif.get('repeat_hour', 0)
                minute = notif.get('repeat_minute', 0)
                time_str = f"📅 Ежедневно в {hour:02d}:{minute:02d}"
                is_passed = False
                sort_time = datetime.now()
            elif repeat_type == 'weekdays':
                hour = notif.get('repeat_hour', 0)
                minute = notif.get('repeat_minute', 0)
                days_names = [WEEKDAYS_NAMES[d] for d in notif.get('weekdays_list', [])]
                time_str = f"📆 {', '.join(days_names)} в {hour:02d}:{minute:02d}"
                is_passed = False
                sort_time = datetime.now()
            elif notif.get('time'):
                notify_time = datetime.fromisoformat(notif['time'])
                if notify_time.tzinfo is None:
                    notify_time = tz.localize(notify_time)
                local_time = notify_time.astimezone(tz)
                time_str = f"⏰ {local_time.strftime('%d.%m.%Y %H:%M')}"
                is_passed = now > local_time and not notif.get('reminder_sent', False)
                sort_time = local_time
            sorted_notifs.append({
                'id': notif_id,
                'num': notif.get('num', notif_id),
                'text': notif['text'],
                'time_str': time_str,
                'is_passed': is_passed,
                'sort_time': sort_time,
                'type': 'local'
            })
        sorted_notifs.sort(key=lambda x: (x['is_passed'], x['sort_time'] if x['sort_time'] else datetime.now()))
        if sorted_notifs:
            local_text = "📋 **Мои напоминания:**\n\n"
            for item in sorted_notifs[:50]:
                if item['is_passed']:
                    local_text += f"~~{item['num']}. {item['text']}~~ — {item['time_str']} *(просрочено)*\n"
                else:
                    local_text += f"**{item['num']}. {item['text']}** — {item['time_str']}\n"
            local_text += f"\n📊 **Всего:** {len(sorted_notifs)}\n\n"
    
    if not local_text:
        msg_text = "📭 **Нет активных напоминаний**"
        if persistent:
            await send_persistent_message(chat_id, msg_text)
        else:
            await send_with_auto_delete(chat_id, msg_text, delay=3600)
    else:
        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("✏️ Ред. уведомление", callback_data="edit_local"),
            InlineKeyboardButton("🔄 Обновить", callback_data="refresh_list")
        )
        if persistent:
            await send_persistent_message(chat_id, local_text, reply_markup=keyboard)
        else:
            await send_with_auto_delete(chat_id, local_text, reply_markup=keyboard, delay=3600)


async def update_pending_list(chat_id: int, persistent: bool = False):
    if not pending_notifications:
        msg_text = "✅ **Нет неотмеченных уведомлений!**\n\nВсе напоминания выполнены или перенесены."
        if persistent:
            await send_persistent_message(chat_id, msg_text)
        else:
            await send_with_auto_delete(chat_id, msg_text, delay=3600)
        return
    
    now = get_current_time()
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    sorted_pending = []
    
    for notif_id, notif in pending_notifications.items():
        if notif.get('is_completed', False):
            continue
        
        notify_time = datetime.fromisoformat(notif['time'])
        if notify_time.tzinfo is None:
            notify_time = tz.localize(notify_time)
        else:
            notify_time = notify_time.astimezone(tz)
        
        repeat_count = notif.get('repeat_count', 0)
        time_str = notify_time.strftime('%d.%m.%Y %H:%M')
        
        sorted_pending.append({
            'id': notif_id,
            'text': notif['text'],
            'time_str': time_str,
            'repeat_count': repeat_count,
            'sort_time': notify_time
        })
    
    sorted_pending.sort(key=lambda x: x['sort_time'])
    
    text = "⚠️ **НЕОТМЕЧЕННЫЕ УВЕДОМЛЕНИЯ**\n\n"
    text += "Эти напоминания просрочены и будут повторяться каждый час,\n"
    text += "пока вы не отметите их как выполненные или не измените время.\n\n"
    
    for item in sorted_pending[:50]:
        repeat_text = f" (повторений: {item['repeat_count']})" if item['repeat_count'] > 0 else ""
        text += f"• **{item['text']}**\n  ⏰ {item['time_str']}{repeat_text}\n\n"
    
    text += f"\n📊 **Всего неотмеченных:** {len(sorted_pending)}"
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Выполнить все", callback_data="pend_complete_all"),
        InlineKeyboardButton("✏️ Редактировать", callback_data="pend_edit_list"),
        InlineKeyboardButton("🔄 Обновить", callback_data="refresh_pending")
    )
    
    if persistent:
        await send_persistent_message(chat_id, text, reply_markup=keyboard)
    else:
        await send_with_auto_delete(chat_id, text, reply_markup=keyboard, delay=3600)


# === ОСНОВНЫЕ ОБРАБОТЧИКИ ===

@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    logger.info(f"Пользователь {message.from_user.id} запустил бота")
    await state.finish()
    
    if ADMIN_ID and message.from_user.id != ADMIN_ID:
        await message.reply("❌ У вас нет доступа к этому боту")
        return
    
    caldav_ok, caldav_message = await check_caldav_connection()
    caldav_status = "✅ Доступен" if caldav_ok else "❌ Ошибка"
    
    welcome_text = f"👋 **Добро пожаловать!**\n\n"
    welcome_text += f"🤖 **Версия бота:** v{BOT_VERSION} ({BOT_VERSION_DATE})\n"
    welcome_text += f"📧 **CalDAV:** {caldav_status}\n"
    welcome_text += f"📅 **Синхронизация с календарём:** {'✅ Вкл' if config.get('calendar_sync_enabled', True) else '❌ Выкл'}\n"
    welcome_text += f"🌍 **Часовой пояс:** {config.get('timezone', 'Europe/Moscow')}\n\n"
    welcome_text += f"⚠️ **Неотмеченные уведомления** — это напоминания, время которых истекло.\n"
    welcome_text += f"Они будут повторяться каждый час, пока вы не отметите их как выполненные.\n\n"
    welcome_text += f"Используйте кнопку **⚠️ Неотмеченные** для управления ими."
    
    if not caldav_ok:
        welcome_text += f"\n\n⚠️ **Внимание! Проблема с подключением к Яндекс.Календарю!**\n\n📋 {caldav_message}\n\n🔧 Получите новый пароль приложения: https://id.yandex.ru/security/app-passwords"
    
    # Постоянные сообщения
    await send_persistent_message(message.chat.id, welcome_text)
    await send_persistent_message(message.chat.id, "👋 **Выберите действие:**", reply_markup=get_main_keyboard())
    await update_notifications_list(message.chat.id, persistent=True)
    await update_pending_list(message.chat.id, persistent=True)
    now = get_current_time()
    await show_calendar_events(message.chat.id, now.year, now.month, persistent=True)


@dp.message_handler(commands=['menu'])
async def show_menu_command(message: types.Message):
    """Команда для ручного отображения главного меню"""
    await delete_user_message(message)
    await send_persistent_message(message.chat.id, "👋 **Главное меню:**", reply_markup=get_main_keyboard())
    await update_notifications_list(message.chat.id, persistent=True)
    await update_pending_list(message.chat.id, persistent=True)
    now = get_current_time()
    await show_calendar_events(message.chat.id, now.year, now.month, persistent=True)


@dp.message_handler(lambda m: m.text == "➕ Добавить", state='*')
async def add_notification_universal(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    await state.finish()
    await add_notification_start(message, state)


async def add_notification_start(message: types.Message, state: FSMContext):
    await send_with_auto_delete(message.chat.id, "✏️ **Введите текст уведомления:**\n\n💡 Для отмены /cancel", delay=3600)
    await NotificationStates.waiting_for_text.set()


@dp.message_handler(state=NotificationStates.waiting_for_text)
async def get_notification_text(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    if not message.text:
        await send_with_auto_delete(message.chat.id, "❌ **Введите текст уведомления.**", delay=3600)
        return
    
    await state.update_data(text=message.text)
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("⏰ В часах", callback_data="time_hours"),
        InlineKeyboardButton("📅 В днях", callback_data="time_days"),
        InlineKeyboardButton("📆 В месяцах", callback_data="time_months"),
        InlineKeyboardButton("🗓️ Конкретная дата/время", callback_data="time_specific"),
        InlineKeyboardButton("🕐 Каждый час", callback_data="time_every_hour"),
        InlineKeyboardButton("📅 Каждый день", callback_data="time_every_day"),
        InlineKeyboardButton("📆 По дням недели", callback_data="time_weekdays")
    )
    await send_with_auto_delete(message.chat.id, "⏱️ **Когда уведомить?**", reply_markup=keyboard, delay=3600)
    await NotificationStates.waiting_for_time_type.set()


# === ОБРАБОТЧИКИ ВРЕМЕНИ (callback'и) ===

@dp.callback_query_handler(lambda c: c.data == "time_specific", state=NotificationStates.waiting_for_time_type)
async def process_specific_time(callback: types.CallbackQuery, state: FSMContext):
    await send_with_auto_delete(callback.from_user.id, "🗓️ **Введите дату и время**\n📝 Форматы:\n• `17.04 21:00`\n• `31.12.2025 23:59`\n• `20.04` (только дата)\n\n💡 Для отмены /cancel", delay=3600)
    await NotificationStates.waiting_for_specific_date.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "time_hours", state=NotificationStates.waiting_for_time_type)
async def process_hours(callback: types.CallbackQuery, state: FSMContext):
    await send_with_auto_delete(callback.from_user.id, "⌛ **Введите количество часов**\n📝 Например: `5`\n\n💡 Для отмены /cancel", delay=3600)
    await NotificationStates.waiting_for_hours.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "time_days", state=NotificationStates.waiting_for_time_type)
async def process_days(callback: types.CallbackQuery, state: FSMContext):
    await send_with_auto_delete(callback.from_user.id, "📅 **Введите количество дней**\n📝 Например: `7`\n\n💡 Для отмены /cancel", delay=3600)
    await NotificationStates.waiting_for_days.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "time_months", state=NotificationStates.waiting_for_time_type)
async def process_months(callback: types.CallbackQuery, state: FSMContext):
    await send_with_auto_delete(callback.from_user.id, "📆 **Введите количество месяцев**\n📝 Например: `1`\n\n💡 Для отмены /cancel", delay=3600)
    await NotificationStates.waiting_for_months.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "time_every_hour", state=NotificationStates.waiting_for_time_type)
async def process_every_hour(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    existing_nums = [int(notif.get('num', 0)) for notif in notifications.values() if not notif.get('is_completed', False)]
    next_num = max(existing_nums) + 1 if existing_nums else 1
    notif_id = str(next_num)
    now = get_current_time()
    
    notifications[notif_id] = {
        'text': data['text'],
        'time': now.isoformat(),
        'created': now.isoformat(),
        'notified': False,
        'is_completed': False,
        'num': next_num,
        'repeat_type': 'every_hour',
        'last_trigger': now.isoformat(),
        'is_repeat': False,
        'repeat_count': 0,
        'reminder_sent': False,
        'last_reminder_time': None
    }
    save_data()
    await sync_notification_to_calendar(notif_id, 'create')
    await send_with_auto_delete(callback.from_user.id, f"✅ **Уведомление #{next_num} создано!**\n📝 {data['text']}\n🕐 Каждый час", delay=3600)
    await state.finish()
    await update_notifications_list(callback.from_user.id, persistent=True)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "time_every_day", state=NotificationStates.waiting_for_time_type)
async def process_every_day(callback: types.CallbackQuery, state: FSMContext):
    await send_with_auto_delete(callback.from_user.id, "⏰ **Введите время (ЧЧ:ММ)**\n📝 Например: `09:00`\n\n💡 Для отмены /cancel", delay=3600)
    await NotificationStates.waiting_for_every_day_time.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "time_weekdays", state=NotificationStates.waiting_for_time_type)
async def process_weekdays(callback: types.CallbackQuery, state: FSMContext):
    keyboard = InlineKeyboardMarkup(row_width=3)
    for name, day in WEEKDAYS_BUTTONS:
        keyboard.add(InlineKeyboardButton(name, callback_data=f"wd_{day}"))
    keyboard.add(InlineKeyboardButton("✅ Готово", callback_data="wd_done"))
    await send_with_auto_delete(callback.from_user.id, "📅 **Выберите дни недели**\n\nНажимайте на дни, чтобы выбрать/отменить.\nКогда закончите, нажмите «✅ Готово»", reply_markup=keyboard, delay=3600)
    await state.update_data(selected_weekdays=[])
    await NotificationStates.waiting_for_weekdays.set()
    await callback.answer()


# === ОБРАБОТЧИКИ ВВОДА ДАТЫ/ВРЕМЕНИ ===

@dp.message_handler(state=NotificationStates.waiting_for_specific_date)
async def set_specific_date_new(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    notify_time = parse_datetime(message.text)
    if notify_time is None:
        await send_with_auto_delete(message.chat.id, "❌ **Неверный формат даты/времени!**\n📝 Примеры:\n• `17.04 21:00`\n• `31.12.2025 23:59`\n• `20.04` (только дата)", delay=3600)
        return
    
    if notify_time <= get_current_time():
        await send_with_auto_delete(message.chat.id, "❌ **Дата/время должны быть в будущем!**", delay=3600)
        return
    
    data = await state.get_data()
    existing_nums = [int(notif.get('num', 0)) for notif in notifications.values() if not notif.get('is_completed', False)]
    next_num = max(existing_nums) + 1 if existing_nums else 1
    notif_id = str(next_num)
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    if notify_time.tzinfo is None:
        notify_time = tz.localize(notify_time)
    
    notifications[notif_id] = {
        'text': data['text'],
        'time': notify_time.isoformat(),
        'created': get_current_time().isoformat(),
        'notified': False,
        'is_completed': False,
        'num': next_num,
        'repeat_type': 'no',
        'is_repeat': False,
        'repeat_count': 0,
        'last_repeat_time': None,
        'reminder_sent': False,
        'last_reminder_time': None
    }
    save_data()
    await sync_notification_to_calendar(notif_id, 'create')
    await send_with_auto_delete(message.chat.id, f"✅ **Уведомление #{next_num} создано!**\n📝 {data['text']}\n⏰ {notify_time.strftime('%d.%m.%Y в %H:%M')}", delay=3600)
    await state.finish()
    await update_notifications_list(message.chat.id, persistent=True)


@dp.message_handler(state=NotificationStates.waiting_for_hours)
async def set_hours(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    try:
        hours = int(message.text)
        if hours <= 0:
            await send_with_auto_delete(message.chat.id, "❌ **Введите положительное число!**", delay=3600)
            return
        
        data = await state.get_data()
        notify_time = get_current_time() + timedelta(hours=hours)
        
        existing_nums = [int(notif.get('num', 0)) for notif in notifications.values() if not notif.get('is_completed', False)]
        next_num = max(existing_nums) + 1 if existing_nums else 1
        notif_id = str(next_num)
        tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
        if notify_time.tzinfo is None:
            notify_time = tz.localize(notify_time)
        
        notifications[notif_id] = {
            'text': data['text'],
            'time': notify_time.isoformat(),
            'created': get_current_time().isoformat(),
            'notified': False,
            'is_completed': False,
            'num': next_num,
            'repeat_type': 'no',
            'is_repeat': False,
            'repeat_count': 0,
            'last_repeat_time': None,
            'reminder_sent': False,
            'last_reminder_time': None
        }
        save_data()
        await sync_notification_to_calendar(notif_id, 'create')
        await send_with_auto_delete(message.chat.id, f"✅ **Уведомление #{next_num} создано!**\n📝 {data['text']}\n⏰ {notify_time.strftime('%d.%m.%Y в %H:%M')}", delay=3600)
        await state.finish()
        await update_notifications_list(message.chat.id, persistent=True)
    except ValueError:
        await send_with_auto_delete(message.chat.id, "❌ **Введите число!**", delay=3600)


@dp.message_handler(state=NotificationStates.waiting_for_days)
async def set_days(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    try:
        days = int(message.text)
        if days <= 0:
            await send_with_auto_delete(message.chat.id, "❌ **Введите положительное число!**", delay=3600)
            return
        
        data = await state.get_data()
        notify_time = get_current_time() + timedelta(days=days)
        
        existing_nums = [int(notif.get('num', 0)) for notif in notifications.values() if not notif.get('is_completed', False)]
        next_num = max(existing_nums) + 1 if existing_nums else 1
        notif_id = str(next_num)
        tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
        if notify_time.tzinfo is None:
            notify_time = tz.localize(notify_time)
        
        notifications[notif_id] = {
            'text': data['text'],
            'time': notify_time.isoformat(),
            'created': get_current_time().isoformat(),
            'notified': False,
            'is_completed': False,
            'num': next_num,
            'repeat_type': 'no',
            'is_repeat': False,
            'repeat_count': 0,
            'last_repeat_time': None,
            'reminder_sent': False,
            'last_reminder_time': None
        }
        save_data()
        await sync_notification_to_calendar(notif_id, 'create')
        await send_with_auto_delete(message.chat.id, f"✅ **Уведомление #{next_num} создано!**\n📝 {data['text']}\n⏰ {notify_time.strftime('%d.%m.%Y в %H:%M')}", delay=3600)
        await state.finish()
        await update_notifications_list(message.chat.id, persistent=True)
    except ValueError:
        await send_with_auto_delete(message.chat.id, "❌ **Введите число!**", delay=3600)


@dp.message_handler(state=NotificationStates.waiting_for_months)
async def set_months(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    try:
        months = int(message.text)
        if months <= 0:
            await send_with_auto_delete(message.chat.id, "❌ **Введите положительное число!**", delay=3600)
            return
        
        days = months * 30
        data = await state.get_data()
        notify_time = get_current_time() + timedelta(days=days)
        
        existing_nums = [int(notif.get('num', 0)) for notif in notifications.values() if not notif.get('is_completed', False)]
        next_num = max(existing_nums) + 1 if existing_nums else 1
        notif_id = str(next_num)
        tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
        if notify_time.tzinfo is None:
            notify_time = tz.localize(notify_time)
        
        notifications[notif_id] = {
            'text': data['text'],
            'time': notify_time.isoformat(),
            'created': get_current_time().isoformat(),
            'notified': False,
            'is_completed': False,
            'num': next_num,
            'repeat_type': 'no',
            'is_repeat': False,
            'repeat_count': 0,
            'last_repeat_time': None,
            'reminder_sent': False,
            'last_reminder_time': None
        }
        save_data()
        await sync_notification_to_calendar(notif_id, 'create')
        await send_with_auto_delete(message.chat.id, f"✅ **Уведомление #{next_num} создано!**\n📝 {data['text']}\n⏰ {notify_time.strftime('%d.%m.%Y в %H:%M')}", delay=3600)
        await state.finish()
        await update_notifications_list(message.chat.id, persistent=True)
    except ValueError:
        await send_with_auto_delete(message.chat.id, "❌ **Введите число!**", delay=3600)


@dp.message_handler(state=NotificationStates.waiting_for_every_day_time)
async def set_every_day_time(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    match = re.match(r'^(\d{1,2}):(\d{2})$', message.text.strip())
    if not match:
        await send_with_auto_delete(message.chat.id, "❌ **Неверный формат!** Используйте `ЧЧ:ММ`", delay=3600)
        return
    hour, minute = map(int, match.groups())
    if hour > 23 or minute > 59:
        await send_with_auto_delete(message.chat.id, "❌ **Некорректное время!**", delay=3600)
        return
    
    data = await state.get_data()
    existing_nums = [int(notif.get('num', 0)) for notif in notifications.values() if not notif.get('is_completed', False)]
    next_num = max(existing_nums) + 1 if existing_nums else 1
    notif_id = str(next_num)
    now = get_current_time()
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    first_time = tz.localize(datetime(now.year, now.month, now.day, hour, minute, 0))
    if first_time <= now:
        first_time += timedelta(days=1)
    
    notifications[notif_id] = {
        'text': data['text'],
        'time': first_time.isoformat(),
        'created': now.isoformat(),
        'notified': False,
        'is_completed': False,
        'num': next_num,
        'repeat_type': 'every_day',
        'repeat_hour': hour,
        'repeat_minute': minute,
        'last_trigger': (first_time - timedelta(days=1)).isoformat(),
        'next_time': first_time.isoformat(),
        'is_repeat': False,
        'repeat_count': 0,
        'reminder_sent': False,
        'last_reminder_time': None
    }
    save_data()
    await sync_notification_to_calendar(notif_id, 'create')
    await send_with_auto_delete(message.chat.id, f"✅ **Уведомление #{next_num} создано!**\n📝 {data['text']}\n📅 Ежедневно в {hour:02d}:{minute:02d}", delay=3600)
    await state.finish()
    await update_notifications_list(message.chat.id, persistent=True)


# === ОБРАБОТЧИКИ ДНЕЙ НЕДЕЛИ ===

@dp.callback_query_handler(lambda c: c.data.startswith('wd_') and c.data != 'wd_done', state=NotificationStates.waiting_for_weekdays)
async def select_weekday(callback: types.CallbackQuery, state: FSMContext):
    day = int(callback.data.replace('wd_', ''))
    data = await state.get_data()
    selected = data.get('selected_weekdays', [])
    if day in selected:
        selected.remove(day)
    else:
        selected.append(day)
    await state.update_data(selected_weekdays=selected)
    
    keyboard = InlineKeyboardMarkup(row_width=3)
    for name, d in WEEKDAYS_BUTTONS:
        text = f"✅ {name}" if d in selected else name
        keyboard.add(InlineKeyboardButton(text, callback_data=f"wd_{d}"))
    keyboard.add(InlineKeyboardButton("✅ Готово", callback_data="wd_done"))
    
    selected_names = [WEEKDAYS_NAMES[d] for d in sorted(selected)]
    status_text = f"Выбрано: {', '.join(selected_names) if selected else 'ничего'}"
    await callback.message.edit_text(f"📅 **Выберите дни недели**\n\n{status_text}", reply_markup=keyboard, parse_mode='Markdown')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "wd_done", state=NotificationStates.waiting_for_weekdays)
async def weekdays_done(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected = data.get('selected_weekdays', [])
    if not selected:
        await callback.answer("❌ Выберите хотя бы один день!")
        return
    
    await state.update_data(weekdays_list=selected)
    await send_with_auto_delete(callback.from_user.id, "⏰ **Введите время (ЧЧ:ММ)**\n📝 Например: `09:00`\n\n💡 Для отмены /cancel", delay=3600)
    await NotificationStates.waiting_for_weekday_time.set()
    await callback.answer()


@dp.message_handler(state=NotificationStates.waiting_for_weekday_time)
async def set_weekday_time(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    match = re.match(r'^(\d{1,2}):(\d{2})$', message.text.strip())
    if not match:
        await send_with_auto_delete(message.chat.id, "❌ **Неверный формат!** Используйте `ЧЧ:ММ`", delay=3600)
        return
    hour, minute = map(int, match.groups())
    if hour > 23 or minute > 59:
        await send_with_auto_delete(message.chat.id, "❌ **Некорректное время!**", delay=3600)
        return
    
    data = await state.get_data()
    weekdays_list = data.get('weekdays_list', [])
    
    if not weekdays_list:
        await send_with_auto_delete(message.chat.id, "❌ **Не выбраны дни недели!**", delay=3600)
        return
    
    first_time = get_next_weekday(weekdays_list, hour, minute)
    if not first_time:
        await send_with_auto_delete(message.chat.id, "❌ **Не удалось определить дату!**", delay=3600)
        return
    
    existing_nums = [int(notif.get('num', 0)) for notif in notifications.values() if not notif.get('is_completed', False)]
    next_num = max(existing_nums) + 1 if existing_nums else 1
    notif_id = str(next_num)
    days_names = [WEEKDAYS_NAMES[d] for d in sorted(weekdays_list)]
    
    notifications[notif_id] = {
        'text': data['text'],
        'time': first_time.isoformat(),
        'created': get_current_time().isoformat(),
        'notified': False,
        'is_completed': False,
        'num': next_num,
        'repeat_type': 'weekdays',
        'repeat_hour': hour,
        'repeat_minute': minute,
        'weekdays_list': weekdays_list,
        'last_trigger': (first_time - timedelta(days=7)).isoformat(),
        'next_time': first_time.isoformat(),
        'is_repeat': False,
        'repeat_count': 0,
        'reminder_sent': False,
        'last_reminder_time': None
    }
    save_data()
    await sync_notification_to_calendar(notif_id, 'create')
    await send_with_auto_delete(message.chat.id, f"✅ **Уведомление #{next_num} создано!**\n📝 {data['text']}\n📆 {', '.join(days_names)} в {hour:02d}:{minute:02d}", delay=3600)
    await state.finish()
    await update_notifications_list(message.chat.id, persistent=True)


# === ОБРАБОТЧИКИ ДЛЯ НЕОТМЕЧЕННЫХ ===

@dp.callback_query_handler(lambda c: c.data == "refresh_pending", state='*')
async def refresh_pending(callback: types.CallbackQuery):
    await update_pending_list(callback.from_user.id, persistent=True)
    await callback.answer("Список обновлён")


@dp.callback_query_handler(lambda c: c.data == "pend_complete_all", state='*')
async def pending_complete_all(callback: types.CallbackQuery):
    logger.info(f"Выполнение всех неотмеченных уведомлений пользователем {callback.from_user.id}")
    for notif_id in list(pending_notifications.keys()):
        notif = pending_notifications[notif_id]
        if not notif.get('is_completed', False):
            if 'calendar_event_id' in notif:
                await sync_notification_to_calendar(notif_id, 'delete')
            del pending_notifications[notif_id]
    save_data()
    await callback.message.edit_text("✅ **Все неотмеченные уведомления выполнены и удалены!**")
    await update_pending_list(callback.from_user.id, persistent=True)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "pend_edit_list", state='*')
async def pending_edit_list(callback: types.CallbackQuery, state: FSMContext):
    if not pending_notifications:
        await callback.answer("Нет неотмеченных уведомлений")
        return
    
    keyboard = InlineKeyboardMarkup(row_width=1)
    for notif_id, notif in pending_notifications.items():
        if notif.get('is_completed', False):
            continue
        keyboard.add(InlineKeyboardButton(f"{notif['text'][:40]}...", callback_data=f"pend_edit_{notif_id}"))
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_edit"))
    
    await callback.message.edit_text("✏️ **Выберите неотмеченное уведомление для редактирования:**", reply_markup=keyboard, parse_mode='Markdown')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('pend_done_'), state='*')
async def pending_done(callback: types.CallbackQuery):
    notif_id = callback.data.replace('pend_done_', '')
    logger.info(f"Отметка выполнения неотмеченного уведомления {notif_id} пользователем {callback.from_user.id}")
    
    if notif_id in pending_notifications:
        notif = pending_notifications[notif_id]
        if 'calendar_event_id' in notif:
            await sync_notification_to_calendar(notif_id, 'delete')
        del pending_notifications[notif_id]
        save_data()
        await callback.message.edit_text(f"✅ **Уведомление выполнено и удалено из неотмеченных!**")
        await update_pending_list(callback.from_user.id, persistent=True)
    else:
        await callback.answer("Уведомление не найдено или уже обработано")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('pend_edit_'), state='*')
async def pending_edit(callback: types.CallbackQuery, state: FSMContext):
    notif_id = callback.data.replace('pend_edit_', '')
    if notif_id not in pending_notifications:
        await callback.answer("Уведомление не найдено")
        return
    
    await state.update_data(edit_id=notif_id, is_pending=True)
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✏️ Изменить текст", callback_data=f"pend_chtext_{notif_id}"),
        InlineKeyboardButton("⏰ Изменить время/дату", callback_data=f"pend_chtime_{notif_id}")
    )
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_edit"))
    await callback.message.edit_text(f"✏️ **Что хотите изменить в неотмеченном уведомлении?**\n\n📝 {pending_notifications[notif_id]['text']}", reply_markup=keyboard, parse_mode='Markdown')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('pend_snooze_') and not c.data.startswith('pend_snooze_1h_') and not c.data.startswith('pend_snooze_3h_') and not c.data.startswith('pend_snooze_1d_') and not c.data.startswith('pend_snooze_7d_') and not c.data.startswith('pend_snooze_custom_'), state='*')
async def pending_snooze(callback: types.CallbackQuery, state: FSMContext):
    notif_id = callback.data.replace('pend_snooze_', '')
    if notif_id not in pending_notifications:
        await callback.answer("Уведомление не найдено")
        return
    await state.update_data(snooze_notif_id=notif_id, is_pending=True)
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("⏰ На 1 час", callback_data=f"pend_snooze_1h_{notif_id}"),
        InlineKeyboardButton("⏰ На 3 часа", callback_data=f"pend_snooze_3h_{notif_id}"),
        InlineKeyboardButton("📅 На 1 день", callback_data=f"pend_snooze_1d_{notif_id}"),
        InlineKeyboardButton("📅 На 7 дней", callback_data=f"pend_snooze_7d_{notif_id}"),
        InlineKeyboardButton("🎯 Свой вариант", callback_data=f"pend_snooze_custom_{notif_id}"),
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_snooze")
    )
    await callback.message.edit_text(f"⏰ **Отложить неотмеченное уведомление**\n\n📝 {pending_notifications[notif_id]['text']}", reply_markup=keyboard, parse_mode='Markdown')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('pend_hour_'), state='*')
async def pending_hour(callback: types.CallbackQuery):
    notif_id = callback.data.replace('pend_hour_', '')
    await process_pending_snooze(callback, notif_id, 1, "hours")


@dp.callback_query_handler(lambda c: c.data.startswith('pend_snooze_1h_'), state='*')
async def pending_snooze_1h(callback: types.CallbackQuery):
    notif_id = callback.data.replace('pend_snooze_1h_', '')
    await process_pending_snooze(callback, notif_id, 1, "hours")


@dp.callback_query_handler(lambda c: c.data.startswith('pend_snooze_3h_'), state='*')
async def pending_snooze_3h(callback: types.CallbackQuery):
    notif_id = callback.data.replace('pend_snooze_3h_', '')
    await process_pending_snooze(callback, notif_id, 3, "hours")


@dp.callback_query_handler(lambda c: c.data.startswith('pend_snooze_1d_'), state='*')
async def pending_snooze_1d(callback: types.CallbackQuery):
    notif_id = callback.data.replace('pend_snooze_1d_', '')
    await process_pending_snooze(callback, notif_id, 1, "days")


@dp.callback_query_handler(lambda c: c.data.startswith('pend_snooze_7d_'), state='*')
async def pending_snooze_7d(callback: types.CallbackQuery):
    notif_id = callback.data.replace('pend_snooze_7d_', '')
    await process_pending_snooze(callback, notif_id, 7, "days")


async def process_pending_snooze(callback: types.CallbackQuery, notif_id: str, value: int, unit: str):
    logger.info(f"Откладывание неотмеченного уведомления {notif_id} на {value} {unit}")
    if notif_id not in pending_notifications:
        await callback.answer("Уведомление не найдено")
        return
    
    now = get_current_time()
    new_time = now + timedelta(hours=value) if unit == "hours" else now + timedelta(days=value)
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    if new_time.tzinfo is None:
        new_time = tz.localize(new_time)
    
    notif = pending_notifications[notif_id]
    notif['time'] = new_time.isoformat()
    notif['reminder_sent'] = False
    notif['repeat_count'] = 0
    notif['last_reminder_time'] = None
    
    await sync_notification_to_calendar(notif_id, 'create')
    
    save_data()
    await callback.message.edit_text(f"⏰ **Уведомление отложено на {value} {unit}**\n🕐 Новое время: {new_time.strftime('%d.%m.%Y в %H:%M')}", parse_mode='Markdown')
    await update_pending_list(callback.from_user.id, persistent=True)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('pend_snooze_custom_'), state='*')
async def pending_snooze_custom(callback: types.CallbackQuery, state: FSMContext):
    notif_id = callback.data.replace('pend_snooze_custom_', '')
    await state.update_data(snooze_notif_id=notif_id, is_pending=True)
    await send_with_auto_delete(callback.from_user.id, "🗓️ **Введите новую дату и время**\n📝 Форматы:\n• `17.04 21:00`\n• `31.12.2025 23:59`\n• `20.04` (только дата)\n\n💡 Для отмены /cancel", delay=3600)
    await SnoozeStates.waiting_for_specific_date.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('pend_chtext_'), state='*')
async def pending_chtext(callback: types.CallbackQuery, state: FSMContext):
    notif_id = callback.data.replace('pend_chtext_', '')
    if notif_id not in pending_notifications:
        await callback.answer("Уведомление не найдено")
        return
    await state.update_data(edit_id=notif_id, is_pending=True)
    await send_with_auto_delete(callback.from_user.id, f"✏️ **Введите новый текст неотмеченного уведомления:**\n\n📝 Старый текст: {pending_notifications[notif_id]['text']}\n\n💡 Для отмены /cancel", delay=3600)
    await NotificationStates.waiting_for_edit_text.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('pend_chtime_'), state='*')
async def pending_chtime(callback: types.CallbackQuery, state: FSMContext):
    notif_id = callback.data.replace('pend_chtime_', '')
    if notif_id not in pending_notifications:
        await callback.answer("Уведомление не найдено")
        return
    await state.update_data(edit_id=notif_id, is_pending=True)
    await send_with_auto_delete(callback.from_user.id, "🗓️ **Введите новую дату и время**\n📝 Форматы:\n• `17.04 21:00`\n• `31.12.2025 23:59`\n• `20.04` (только дата)\n\n💡 Для отмены /cancel", delay=3600)
    await NotificationStates.waiting_for_specific_date.set()
    await callback.answer()


@dp.message_handler(state=NotificationStates.waiting_for_edit_text)
async def save_edited_text(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    data = await state.get_data()
    edit_id = data.get('edit_id')
    is_pending = data.get('is_pending', False)
    
    target_dict = pending_notifications if is_pending else notifications
    
    if not edit_id or edit_id not in target_dict:
        await send_with_auto_delete(message.chat.id, "❌ **Уведомление не найдено!**", delay=3600)
        await state.finish()
        return
    
    old_text = target_dict[edit_id]['text']
    target_dict[edit_id]['text'] = message.text
    target_dict[edit_id]['reminder_sent'] = False
    target_dict[edit_id]['repeat_count'] = 0
    target_dict[edit_id]['last_reminder_time'] = None
    
    save_data()
    await sync_notification_to_calendar(edit_id, 'create')
    
    await send_with_auto_delete(message.chat.id, f"✅ **Текст изменен!**\n\nСтарый: {old_text}\nНовый: {message.text}", delay=3600)
    
    if is_pending:
        await update_pending_list(message.chat.id, persistent=True)
    else:
        await update_notifications_list(message.chat.id, persistent=True)
    
    await state.finish()


@dp.message_handler(state=SnoozeStates.waiting_for_specific_date)
async def snooze_set_specific_date(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    notify_time = parse_datetime(message.text)
    if notify_time is None:
        await send_with_auto_delete(message.chat.id, "❌ **Неверный формат даты/времени!**\n📝 Примеры:\n• `17.04 21:00`\n• `31.12.2025 23:59`\n• `20.04` (только дата)", delay=3600)
        return
    if notify_time <= get_current_time():
        await send_with_auto_delete(message.chat.id, "❌ **Дата/время должны быть в будущем!**", delay=3600)
        return
    
    data = await state.get_data()
    notif_id = data.get('snooze_notif_id')
    is_pending = data.get('is_pending', False)
    target_dict = pending_notifications if is_pending else notifications
    
    if not notif_id or notif_id not in target_dict:
        await send_with_auto_delete(message.chat.id, "❌ **Уведомление не найдено!**", delay=3600)
        await state.finish()
        return
    
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    if notify_time.tzinfo is None:
        notify_time = tz.localize(notify_time)
    
    target_dict[notif_id]['time'] = notify_time.isoformat()
    target_dict[notif_id]['reminder_sent'] = False
    target_dict[notif_id]['repeat_count'] = 0
    target_dict[notif_id]['last_reminder_time'] = None
    save_data()
    await sync_notification_to_calendar(notif_id, 'create')
    
    await send_with_auto_delete(message.chat.id, f"⏰ **Уведомление отложено!**\n🕐 Новое время: {notify_time.strftime('%d.%m.%Y в %H:%M')}", delay=3600)
    
    if is_pending:
        await update_pending_list(message.chat.id, persistent=True)
    else:
        await update_notifications_list(message.chat.id, persistent=True)
    
    await state.finish()


@dp.message_handler(lambda m: m.text == "📋 Список", state='*')
async def list_notifications_universal(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    await state.finish()
    await update_notifications_list(message.chat.id, persistent=True)


@dp.message_handler(lambda m: m.text == "📅 События", state='*')
async def view_events_universal(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    await state.finish()
    now = get_current_time()
    await show_calendar_events(message.chat.id, now.year, now.month, persistent=True)


@dp.message_handler(lambda m: m.text == "⚠️ Неотмеченные", state='*')
async def pending_list_universal(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    await state.finish()
    await update_pending_list(message.chat.id, persistent=True)


@dp.message_handler(lambda m: m.text == "⚙️ Настройки", state='*')
async def settings_universal(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    await state.finish()
    await settings_menu_handler(message, state)


@dp.callback_query_handler(lambda c: c.data == "refresh_list", state='*')
async def refresh_list(callback: types.CallbackQuery):
    await update_notifications_list(callback.from_user.id, persistent=True)
    await callback.answer("Список обновлён")


@dp.callback_query_handler(lambda c: c.data == "edit_local", state='*')
async def edit_local_handler(callback: types.CallbackQuery, state: FSMContext):
    active_notifs = {nid: n for nid, n in notifications.items() if not n.get('is_completed', False)}
    if not active_notifs:
        await callback.answer("Нет активных уведомлений")
        return
    keyboard = InlineKeyboardMarkup(row_width=1)
    for notif_id, notif in sorted(active_notifs.items(), key=lambda x: int(x[0])):
        keyboard.add(InlineKeyboardButton(f"#{notif.get('num', notif_id)}: {notif['text'][:40]}...", callback_data=f"sel_notif_{notif_id}"))
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_edit"))
    await callback.message.edit_text("✏️ **Выберите уведомление для редактирования:**", reply_markup=keyboard, parse_mode='Markdown')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('sel_notif_'), state='*')
async def edit_selected_notification(callback: types.CallbackQuery, state: FSMContext):
    notif_id = callback.data.replace('sel_notif_', '')
    if notif_id not in notifications:
        await callback.answer("Уведомление не найдено")
        return
    await state.update_data(edit_id=notif_id, is_pending=False)
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✏️ Изменить текст", callback_data=f"chtext_{notif_id}"),
        InlineKeyboardButton("⏰ Изменить время/дату", callback_data=f"chtime_{notif_id}")
    )
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_edit"))
    await callback.message.edit_text(f"✏️ **Что хотите изменить в уведомлении #{notifications[notif_id].get('num', notif_id)}?**\n\n📝 Текст: {notifications[notif_id]['text'][:50]}...", reply_markup=keyboard, parse_mode='Markdown')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('chtext_'), state='*')
async def change_notification_text(callback: types.CallbackQuery, state: FSMContext):
    notif_id = callback.data.replace('chtext_', '')
    if notif_id not in notifications:
        await callback.answer("Уведомление не найдено")
        return
    await state.update_data(edit_id=notif_id, is_pending=False)
    await send_with_auto_delete(callback.from_user.id, f"✏️ **Введите новый текст уведомления:**\n\n📝 Старый текст: {notifications[notif_id]['text']}\n\n💡 Для отмены /cancel", delay=3600)
    await NotificationStates.waiting_for_edit_text.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('chtime_'), state='*')
async def change_notification_time(callback: types.CallbackQuery, state: FSMContext):
    notif_id = callback.data.replace('chtime_', '')
    if notif_id not in notifications:
        await callback.answer("Уведомление не найдено")
        return
    await state.update_data(edit_id=notif_id, is_pending=False)
    await send_with_auto_delete(callback.from_user.id, "🗓️ **Введите новую дату и время**\n📝 Форматы:\n• `17.04 21:00`\n• `31.12.2025 23:59`\n• `20.04` (только дата)\n\n💡 Для отмены /cancel", delay=3600)
    await NotificationStates.waiting_for_specific_date.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "cancel_edit", state='*')
async def cancel_edit_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()
    await update_notifications_list(callback.from_user.id, persistent=True)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "cancel_snooze", state='*')
async def cancel_snooze_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()
    await callback.message.edit_text("✅ **Откладывание отменено**")
    await callback.answer()


# === ОБРАБОТЧИКИ КАЛЕНДАРЯ ===

@dp.callback_query_handler(lambda c: c.data.startswith("cal_prev_"), state='*')
async def calendar_prev_month(callback: types.CallbackQuery):
    parts = callback.data.replace("cal_prev_", "").split("_")
    year = int(parts[0])
    month = int(parts[1])
    await show_calendar_events(callback.from_user.id, year, month, persistent=True)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("cal_next_"), state='*')
async def calendar_next_month(callback: types.CallbackQuery):
    parts = callback.data.replace("cal_next_", "").split("_")
    year = int(parts[0])
    month = int(parts[1])
    await show_calendar_events(callback.from_user.id, year, month, persistent=True)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("cal_refresh_"), state='*')
async def calendar_refresh(callback: types.CallbackQuery):
    parts = callback.data.replace("cal_refresh_", "").split("_")
    year = int(parts[0])
    month = int(parts[1])
    await show_calendar_events(callback.from_user.id, year, month, force_refresh=True, persistent=True)
    await callback.answer("✅ Календарь обновлён")


@dp.callback_query_handler(lambda c: c.data.startswith("cal_sync_"), state='*')
async def calendar_sync(callback: types.CallbackQuery):
    parts = callback.data.replace("cal_sync_", "").split("_")
    year = int(parts[0])
    month = int(parts[1])
    await callback.message.edit_text("🔄 **Синхронизация с календарём...**")
    if get_caldav_available():
        caldav_api = CalDAVCalendarAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
        events = await caldav_api.get_month_events(year, month)
        cache_key = f"{year}_{month}"
        calendar_events_cache[cache_key] = events
        last_calendar_update[cache_key] = get_current_time()
        await show_calendar_events(callback.from_user.id, year, month, force_refresh=True, persistent=True)
        await callback.answer("✅ Синхронизация завершена")
    else:
        await callback.answer("❌ CalDAV не настроен")
        await show_calendar_events(callback.from_user.id, year, month, persistent=True)


@dp.callback_query_handler(lambda c: c.data == "curr_month", state='*')
async def calendar_current_month(callback: types.CallbackQuery):
    now = get_current_time()
    await show_calendar_events(callback.from_user.id, now.year, now.month, persistent=True)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "edit_calendar", state='*')
async def edit_calendar_handler(callback: types.CallbackQuery, state: FSMContext):
    logger.info(f"edit_calendar_handler вызван")
    now = get_current_time()
    year = now.year
    month = now.month
    await update_calendar_events_cache(year, month)
    cache_key = f"{year}_{month}"
    events = calendar_events_cache.get(cache_key, [])
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    future_events = []
    for event in events:
        try:
            start_dt = datetime.fromisoformat(event['start'])
            if start_dt.tzinfo is None:
                start_dt = tz.localize(start_dt)
            else:
                start_dt = start_dt.astimezone(tz)
            if start_dt >= today:
                future_events.append((start_dt, event))
        except:
            continue
    if not future_events:
        await callback.answer("Нет событий для редактирования")
        return
    keyboard = InlineKeyboardMarkup(row_width=1)
    for idx, (start_dt, event) in enumerate(future_events[:20]):
        day = start_dt.day
        month_num = start_dt.month
        time_str = start_dt.strftime('%H:%M')
        short_id = hashlib.md5(event['id'].encode()).hexdigest()[:16]
        event_id_map[short_id] = event['id']
        keyboard.add(InlineKeyboardButton(f"{idx+1}. {day:02d}.{month_num:02d} {time_str} - {event['summary'][:30]}", callback_data=f"sel_cal_event_{short_id}"))
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_edit"))
    await callback.message.edit_text("✏️ **Выберите событие для редактирования:**", reply_markup=keyboard, parse_mode='Markdown')
    await EditCalendarEventStates.waiting_for_event_selection.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("sel_cal_event_"), state=EditCalendarEventStates.waiting_for_event_selection)
async def edit_calendar_select_event(callback: types.CallbackQuery, state: FSMContext):
    short_id = callback.data.replace("sel_cal_event_", "")
    full_id = event_id_map.get(short_id)
    if not full_id:
        await callback.answer("Событие не найдено")
        return
    await state.update_data(edit_event_id=full_id)
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✏️ Изменить текст", callback_data="edit_cal_text"),
        InlineKeyboardButton("⏰ Изменить дату/время", callback_data="edit_cal_time"),
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_edit")
    )
    await callback.message.edit_text("✏️ **Что хотите изменить в событии?**", reply_markup=keyboard, parse_mode='Markdown')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "edit_cal_text", state=EditCalendarEventStates.waiting_for_event_selection)
async def edit_event_text_prompt(callback: types.CallbackQuery, state: FSMContext):
    await send_with_auto_delete(callback.from_user.id, "✏️ **Введите новый текст события:**\n\n💡 Для отмены /cancel", delay=3600)
    await EditCalendarEventStates.waiting_for_new_text.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "edit_cal_time", state=EditCalendarEventStates.waiting_for_event_selection)
async def edit_event_time_prompt(callback: types.CallbackQuery, state: FSMContext):
    await send_with_auto_delete(callback.from_user.id, "🗓️ **Введите новую дату и время**\n📝 Форматы:\n• `17.04 21:00`\n• `31.12.2025 23:59`\n• `20.04` (только дата)\n\n💡 Для отмены /cancel", delay=3600)
    await EditCalendarEventStates.waiting_for_new_datetime.set()
    await callback.answer()


@dp.message_handler(state=EditCalendarEventStates.waiting_for_new_text)
async def save_edited_event_text(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    data = await state.get_data()
    event_id = data.get('edit_event_id')
    if not event_id:
        await send_with_auto_delete(message.chat.id, "❌ Ошибка: событие не найдено", delay=3600)
        await state.finish()
        return
    
    now = get_current_time()
    year = now.year
    month = now.month
    await update_calendar_events_cache(year, month)
    cache_key = f"{year}_{month}"
    events = calendar_events_cache.get(cache_key, [])
    target_event = None
    for ev in events:
        if ev['id'] == event_id:
            target_event = ev
            break
    if not target_event:
        await send_with_auto_delete(message.chat.id, "❌ Событие не найдено в кэше", delay=3600)
        await state.finish()
        return
    
    caldav_api = CalDAVCalendarAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
    start_dt = datetime.fromisoformat(target_event['start'])
    if start_dt.tzinfo is None:
        tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
        start_dt = tz.localize(start_dt)
    
    success = await caldav_api.update_event(event_id, new_summary=message.text, new_start=start_dt)
    if success:
        await send_with_auto_delete(message.chat.id, f"✅ **Текст события изменён!**\nНовый текст: {message.text}", delay=3600)
        await update_calendar_events_cache(year, month, force=True)
        await sync_calendar_to_pending()
    else:
        await send_with_auto_delete(message.chat.id, "❌ Ошибка при обновлении события", delay=3600)
    await state.finish()
    await update_notifications_list(message.chat.id, persistent=True)


@dp.message_handler(state=EditCalendarEventStates.waiting_for_new_datetime)
async def save_edited_event_datetime(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    new_dt = parse_datetime(message.text)
    if new_dt is None:
        await send_with_auto_delete(message.chat.id, "❌ **Неверный формат даты/времени!**\n📝 Примеры:\n• `17.04 21:00`\n• `31.12.2025 23:59`\n• `20.04` (только дата)", delay=3600)
        return
    
    if new_dt <= get_current_time():
        await send_with_auto_delete(message.chat.id, "❌ **Дата/время должны быть в будущем!**", delay=3600)
        return
    
    data = await state.get_data()
    event_id = data.get('edit_event_id')
    if not event_id:
        await send_with_auto_delete(message.chat.id, "❌ Ошибка: событие не найдено", delay=3600)
        await state.finish()
        return
    
    now = get_current_time()
    year = now.year
    month = now.month
    await update_calendar_events_cache(year, month)
    cache_key = f"{year}_{month}"
    events = calendar_events_cache.get(cache_key, [])
    target_event = None
    for ev in events:
        if ev['id'] == event_id:
            target_event = ev
            break
    if not target_event:
        await send_with_auto_delete(message.chat.id, "❌ Событие не найдено в кэше", delay=3600)
        await state.finish()
        return
    
    caldav_api = CalDAVCalendarAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
    success = await caldav_api.update_event(event_id, new_summary=target_event['summary'], new_start=new_dt)
    if success:
        await send_with_auto_delete(message.chat.id, f"✅ **Дата/время события изменены!**\n🕐 Новое время: {new_dt.strftime('%d.%m.%Y в %H:%M')}", delay=3600)
        await update_calendar_events_cache(year, month, force=True)
        await sync_calendar_to_pending()
    else:
        await send_with_auto_delete(message.chat.id, "❌ Ошибка при обновлении события", delay=3600)
    await state.finish()
    await update_notifications_list(message.chat.id, persistent=True)


# === НАСТРОЙКИ ===

async def settings_menu_handler(message: types.Message, state: FSMContext):
    global notifications_enabled
    status_text = "🔕 Выкл" if not notifications_enabled else "🔔 Вкл"
    status_emoji = "🔕" if not notifications_enabled else "🔔"
    calendar_sync_status = "✅ Вкл" if config.get('calendar_sync_enabled', True) else "❌ Выкл"
    if get_caldav_available():
        caldav_ok, _ = await check_caldav_connection()
        caldav_status = "✅ Доступен" if caldav_ok else "❌ Ошибка"
    else:
        caldav_status = "❌ Не настроен"
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton(f"{status_emoji} Уведомления: {status_text}", callback_data="toggle_notify"),
        InlineKeyboardButton(f"☁️ Синхр. с календарём: {calendar_sync_status}", callback_data="toggle_cal_sync"),
        InlineKeyboardButton("🕐 Время проверки", callback_data="set_check_time"),
        InlineKeyboardButton("🌍 Часовой пояс", callback_data="set_timezone"),
        InlineKeyboardButton("🔍 Проверить календарь", callback_data="check_cal"),
        InlineKeyboardButton("ℹ️ Информация", callback_data="info")
    )
    await send_with_auto_delete(message.chat.id, f"⚙️ **НАСТРОЙКИ**\n\n📧 CalDAV: {caldav_status}\n🌍 Часовой пояс: {config.get('timezone', 'Europe/Moscow')}", reply_markup=keyboard, delay=3600)


@dp.callback_query_handler(lambda c: c.data == "check_cal")
async def check_calendar_connection(callback: types.CallbackQuery):
    if not get_caldav_available():
        await callback.message.edit_text("❌ **CalDAV не настроен!**\n\nДобавьте в .env файл:\nYANDEX_EMAIL=ваш_email@yandex.ru\nYANDEX_APP_PASSWORD=пароль_приложения", parse_mode='Markdown')
        await callback.answer()
        return
    await callback.message.edit_text("🔍 **Проверка подключения к календарю...**")
    caldav_ok, caldav_message = await check_caldav_connection()
    if caldav_ok:
        await callback.message.edit_text(f"✅ **Подключение к календарю работает!**\n\n{caldav_message}", parse_mode='Markdown')
        now = get_current_time()
        await update_calendar_events_cache(now.year, now.month, force=True)
        await sync_calendar_to_pending()
    else:
        await callback.message.edit_text(f"❌ **Ошибка подключения к календарю!**\n\n📋 {caldav_message}\n\n🔧 **Что делать:**\n1. Проверьте email в .env (YANDEX_EMAIL)\n2. Получите НОВЫЙ пароль приложения:\n   • https://id.yandex.ru/security/app-passwords\n   • Создайте пароль для 'CalDAV'\n3. Обновите YANDEX_APP_PASSWORD в .env\n4. Перезапустите бота", parse_mode='Markdown')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "toggle_notify")
async def toggle_notifications(callback: types.CallbackQuery, state: FSMContext):
    global notifications_enabled
    notifications_enabled = not notifications_enabled
    config['notifications_enabled'] = notifications_enabled
    save_data()
    await callback.message.edit_text(f"✅ **Уведомления {'включены' if notifications_enabled else 'выключены'}!**")
    await settings_menu_handler(callback.message, state)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "toggle_cal_sync")
async def toggle_calendar_sync(callback: types.CallbackQuery, state: FSMContext):
    current = config.get('calendar_sync_enabled', True)
    config['calendar_sync_enabled'] = not current
    save_data()
    if config['calendar_sync_enabled'] and get_caldav_available():
        caldav_ok, _ = await check_caldav_connection()
        if caldav_ok:
            for notif_id in notifications:
                if 'calendar_event_id' not in notifications[notif_id]:
                    await sync_notification_to_calendar(notif_id, 'create')
            for notif_id in pending_notifications:
                if 'calendar_event_id' not in pending_notifications[notif_id]:
                    await sync_notification_to_calendar(notif_id, 'create')
            await callback.message.edit_text("✅ **Синхронизация с календарём включена!**")
            now = get_current_time()
            await update_calendar_events_cache(now.year, now.month, force=True)
            await sync_calendar_to_pending()
        else:
            await callback.message.edit_text("⚠️ **Синхронизация включена, но нет доступа к календарю!**\n\nПроверьте настройки CalDAV", parse_mode='Markdown')
    else:
        await callback.message.edit_text(f"✅ **Синхронизация {'включена' if config['calendar_sync_enabled'] else 'выключена'}!**")
    await settings_menu_handler(callback.message, state)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "set_check_time")
async def set_check_time(callback: types.CallbackQuery):
    await send_with_auto_delete(callback.from_user.id, f"🕐 **Текущее время проверки:** `{config.get('daily_check_time', '06:00')}`\n\nВведите новое время (ЧЧ:ММ):", delay=3600)
    await SettingsStates.waiting_for_check_time.set()
    await callback.answer()


@dp.message_handler(state=SettingsStates.waiting_for_check_time)
async def save_check_time(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    try:
        datetime.strptime(message.text, "%H:%M")
        config['daily_check_time'] = message.text
        save_data()
        await send_with_auto_delete(message.chat.id, f"✅ **Время проверки установлено:** {message.text}", delay=3600)
    except ValueError:
        await send_with_auto_delete(message.chat.id, "❌ **Неверный формат!** Используйте ЧЧ:ММ", delay=3600)
    await state.finish()


@dp.callback_query_handler(lambda c: c.data == "set_timezone")
async def set_timezone(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(row_width=2)
    for name in TIMEZONES.keys():
        keyboard.add(InlineKeyboardButton(name, callback_data=f"tz_{name}"))
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_tz"))
    await callback.message.edit_text(f"🌍 **Выберите часовой пояс**\n\nТекущий: {config.get('timezone', 'Europe/Moscow')}", reply_markup=keyboard, parse_mode='Markdown')
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("tz_"))
async def save_timezone(callback: types.CallbackQuery, state: FSMContext):
    tz_name = callback.data.replace("tz_", "")
    tz_value = TIMEZONES.get(tz_name, 'Europe/Moscow')
    config['timezone'] = tz_value
    save_data()
    await callback.message.edit_text(f"✅ **Часовой пояс установлен:** {tz_name}\n🕐 {get_current_time().strftime('%d.%m.%Y %H:%M:%S')}", parse_mode='Markdown')
    await settings_menu_handler(callback.message, state)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "cancel_tz")
async def cancel_tz(callback: types.CallbackQuery, state: FSMContext):
    await settings_menu_handler(callback.message, state)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "info")
async def show_info(callback: types.CallbackQuery):
    caldav_ok, caldav_message = await check_caldav_connection() if get_caldav_available() else (False, "Не настроен")
    caldav_status = "✅ Доступен" if caldav_ok else "❌ Ошибка"
    info = f"""
📊 **СТАТИСТИКА**

🤖 **Версия:** v{BOT_VERSION} ({BOT_VERSION_DATE})

📝 **Уведомлений:** `{len(notifications)}`
⚠️ **Неотмеченных:** `{len(pending_notifications)}`
🕐 **Проверка уведомлений:** `{config.get('daily_check_time', '06:00')}`
🌍 **Часовой пояс:** `{config.get('timezone', 'Europe/Moscow')}`
🕐 **Текущее время:** `{get_current_time().strftime('%d.%m.%Y %H:%M:%S')}`
🔔 **Уведомления:** `{'Вкл' if notifications_enabled else 'Выкл'}`
📅 **Синхр. с календарём:** `{'Вкл' if config.get('calendar_sync_enabled', True) else 'Выкл'}`
📧 **CalDAV:** `{caldav_status}`
"""
    if not caldav_ok and get_caldav_available():
        info += f"\n⚠️ **Проблема с CalDAV:**\n{caldav_message[:200]}"
    await callback.message.edit_text(info, parse_mode='Markdown')
    await callback.answer()


@dp.message_handler(commands=['version'])
async def show_version(message: types.Message):
    await delete_user_message(message)
    await send_with_auto_delete(message.chat.id, f"🤖 **Бот для уведомлений**\n📌 **Версия:** v{BOT_VERSION}\n📅 **Дата:** {BOT_VERSION_DATE}", delay=3600)


@dp.message_handler(commands=['cancel'], state='*')
async def cancel_operation(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    current_state = await state.get_state()
    if current_state is None:
        await send_with_auto_delete(message.chat.id, "❌ **Нет активных операций**", delay=3600)
        return
    await state.finish()
    await send_with_auto_delete(message.chat.id, "✅ **Операция отменена!**", delay=3600)


async def auto_update_calendar_cache():
    while True:
        try:
            now = get_current_time()
            year = now.year
            month = now.month
            await update_calendar_events_cache(year, month, force=True)
            await sync_calendar_to_pending()
            logger.info("Автообновление календаря выполнено")
        except Exception as e:
            logger.error(f"Ошибка автообновления календаря: {e}")
        await asyncio.sleep(900)


async def on_startup(dp):
    init_folders()
    load_data()
    
    # Перенумерация уведомлений и очистка дубликатов
    new_notifications = {}
    processed_ids = set()
    for i, (notif_id, notif) in enumerate(sorted(notifications.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 0), 1):
        notif_key = f"{notif['text']}_{notif.get('time', '')}"
        if notif_key not in processed_ids:
            notif['num'] = i
            if 'is_completed' not in notif:
                notif['is_completed'] = False
            if 'last_repeat_time' not in notif:
                notif['last_repeat_time'] = None
            if 'repeat_count' not in notif:
                notif['repeat_count'] = 0
            if 'is_repeat' not in notif:
                notif['is_repeat'] = False
            if 'last_trigger' not in notif:
                notif['last_trigger'] = None
            if 'reminder_sent' not in notif:
                notif['reminder_sent'] = False
            if 'last_reminder_time' not in notif:
                notif['last_reminder_time'] = None
            new_notifications[str(i)] = notif
            processed_ids.add(notif_key)
    
    notifications.clear()
    notifications.update(new_notifications)
    save_data()
    
    now = get_current_time()
    await update_calendar_events_cache(now.year, now.month, force=True)
    await sync_calendar_to_pending()
    
    logger.info(f"\n{'='*50}")
    logger.info(f"🤖 БОТ ДЛЯ УВЕДОМЛЕНИЙ v{BOT_VERSION}")
    logger.info(f"{'='*50}")
    
    if get_caldav_available():
        logger.info("🔍 Проверка подключения к CalDAV...")
        caldav_ok, caldav_message = await check_caldav_connection()
        if caldav_ok:
            logger.info(f"✅ {caldav_message}")
        else:
            logger.warning(f"⚠️ {caldav_message}")
    else:
        logger.warning("❌ CalDAV не настроен")
    
    logger.info(f"📝 Уведомлений: {len(notifications)}")
    logger.info(f"⚠️ Неотмеченных: {len(pending_notifications)}")
    logger.info(f"🔔 Уведомления: {'Вкл' if notifications_enabled else 'Выкл'}")
    logger.info(f"🌍 Часовой пояс: {config.get('timezone', 'Europe/Moscow')}")
    logger.info(f"🕐 Текущее время: {get_current_time().strftime('%d.%m.%Y %H:%M:%S')}")
    logger.info(f"{'='*50}\n")
    
    asyncio.create_task(check_regular_notifications())
    asyncio.create_task(check_pending_notifications())
    asyncio.create_task(auto_update_calendar_cache())
    asyncio.create_task(sync_calendar_task())
    logger.info("✅ Бот успешно запущен!")


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)