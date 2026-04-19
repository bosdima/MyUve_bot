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
BOT_VERSION = "1.5"
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
last_sync_time: Optional[datetime] = None

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
                return False, "Не удалось подключиться к CalDAV серверу."
            calendars = self.principal.calendars()
            if calendars:
                return True, f"CalDAV подключен, найдено {len(calendars)} календарей"
            return False, "Календари не найдены."
        except caldav.lib.error.AuthorizationError:
            return False, "Ошибка авторизации! Получите новый пароль приложения."
        except Exception as e:
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
                logger.info(f"Создано событие в календаре: {summary}")
                return str(event.url)
            return None
        except Exception as e:
            logger.error(f"Ошибка создания события: {e}")
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
                    logger.info(f"Удалено событие: {event_url}")
                    return True
            return False
        except Exception as e:
            logger.error(f"Ошибка удаления события: {e}")
            return False
    
    async def update_event(self, event_url: str, new_summary: str = None, new_start: datetime = None) -> bool:
        try:
            delete_success = await self.delete_event(event_url)
            if not delete_success:
                return False
            if new_summary and new_start:
                new_id = await self.create_event(new_summary, new_start, "Обновлено через бота")
                return new_id is not None
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
            
            # Используем старый рабочий метод date_search (несмотря на deprecation)
            events = calendar.date_search(start=from_utc, end=to_utc, expand=True)
            
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
        return False, "CalDAV не настроен."
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
        last_sync_time = now
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
    
    future_events = []
    for event in events:
        try:
            start_dt = datetime.fromisoformat(event['start'])
            if start_dt.tzinfo is None:
                start_dt = tz.localize(start_dt)
            else:
                start_dt = start_dt.astimezone(tz)
            if start_dt >= now - timedelta(hours=1):  # показываем события, которые начались не более часа назад
                future_events.append((start_dt, event))
        except Exception as e:
            logger.error(f"Ошибка обработки события: {e}")
    
    if not future_events:
        text = f"📅 **Нет предстоящих событий на {MONTHS_NAMES[month]} {year}**"
    else:
        future_events.sort(key=lambda x: x[0])
        text = f"📅 **СОБЫТИЯ КАЛЕНДАРЯ**\n📆 {MONTHS_NAMES[month]} {year}\n\n"
        for start_dt, event in future_events[:30]:
            day = start_dt.day
            month_num = start_dt.month
            year_num = start_dt.year
            time_str = start_dt.strftime('%H:%M')
            summary = event['summary']
            weekday_name = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"][start_dt.weekday()]
            
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
    waiting_for_edit_text = State()


class SnoozeStates(StatesGroup):
    waiting_for_specific_date = State()


class SettingsStates(StatesGroup):
    waiting_for_check_time = State()
    waiting_for_timezone = State()


class EditCalendarEventStates(StatesGroup):
    waiting_for_event_selection = State()
    waiting_for_new_text = State()
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
    
    notifications = {k: v for k, v in notifications.items() if not v.get('is_completed', False)}
    pending_notifications = {k: v for k, v in pending_notifications.items() if not v.get('is_completed', False)}
    
    for notif_id, notif in notifications.items():
        if 'is_completed' not in notif:
            notif['is_completed'] = False
        if 'repeat_count' not in notif:
            notif['repeat_count'] = 0
        if 'last_trigger' not in notif:
            notif['last_trigger'] = None
        if 'reminder_sent' not in notif:
            notif['reminder_sent'] = False
        if 'last_reminder_time' not in notif:
            notif['last_reminder_time'] = None
    
    for notif_id, notif in pending_notifications.items():
        if 'is_completed' not in notif:
            notif['is_completed'] = False
        if 'repeat_count' not in notif:
            notif['repeat_count'] = 0
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
    
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    for event in events:
        try:
            start_dt = datetime.fromisoformat(event['start'])
            if start_dt.tzinfo is None:
                start_dt = tz.localize(start_dt)
            else:
                start_dt = start_dt.astimezone(tz)
            
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
                    logger.info(f"Событие '{summary}' добавлено в неотмеченные")
        except Exception as e:
            logger.error(f"Ошибка синхронизации: {e}")


async def sync_notification_to_calendar(notif_id: str, action: str = 'create'):
    if not config.get('calendar_sync_enabled', True) or not get_caldav_available():
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
            
            description = f"Уведомление из бота\nТекст: {notif['text']}\nСоздано: {get_current_time().strftime('%d.%m.%Y %H:%M')}"
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
                del notif['calendar_event_id']
                save_data()
                logger.info(f"Уведомление {notif_id} удалено из календаря")
                now = get_current_time()
                await update_calendar_events_cache(now.year, now.month, force=True)
    except Exception as e:
        logger.error(f"Ошибка синхронизации: {e}")


async def auto_delete_message(chat_id: int, message_id: int, delay: int = 3600):
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id, message_id)
    except:
        pass


async def send_with_auto_delete(chat_id: int, text: str, parse_mode: str = 'Markdown', reply_markup=None, delay: int = 3600):
    try:
        msg = await bot.send_message(chat_id, text, parse_mode=parse_mode, reply_markup=reply_markup)
        asyncio.create_task(auto_delete_message(chat_id, msg.message_id, delay))
        return msg
    except Exception as e:
        logger.error(f"Ошибка отправки: {e}")
        return None


async def send_persistent_message(chat_id: int, text: str, parse_mode: str = 'Markdown', reply_markup=None):
    try:
        return await bot.send_message(chat_id, text, parse_mode=parse_mode, reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"Ошибка отправки: {e}")
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
        f"🔔 **НЕОТМЕЧЕННОЕ НАПОМИНАНИЕ!**{repeat_text}\n\n📝 {notif_text}\n\n⏰ Время истекло!\n⚠️ Напоминание будет повторяться каждый час.",
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
                
                if not notif.get('reminder_sent', False) and now >= notify_time:
                    await show_pending_notification_actions(ADMIN_ID, notif_id, notif['text'])
                    notif['reminder_sent'] = True
                    notif['last_reminder_time'] = now.isoformat()
                    notif['repeat_count'] = 1
                    save_data()
                    logger.info(f"Отправлено первое уведомление для {notif_id}")
                
                elif notif.get('reminder_sent', False) and not notif.get('is_completed', False):
                    if last_reminder_time is None:
                        last_reminder_time = notify_time
                    time_since_last = (now - last_reminder_time).total_seconds()
                    if time_since_last >= 3600:
                        repeat_count = notif.get('repeat_count', 0) + 1
                        await show_pending_notification_actions(ADMIN_ID, notif_id, notif['text'], repeat_count)
                        notif['last_reminder_time'] = now.isoformat()
                        notif['repeat_count'] = repeat_count
                        save_data()
                        logger.info(f"Отправлено повторное уведомление #{repeat_count} для {notif_id}")
        
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
                        await show_pending_notification_actions(ADMIN_ID, notif_id, notif['text'])
                        notif['last_trigger'] = now.isoformat()
                        notif['repeat_count'] = notif.get('repeat_count', 0) + 1
                        pending_notifications[notif_id] = notif.copy()
                        pending_notifications[notif_id]['reminder_sent'] = True
                        pending_notifications[notif_id]['last_reminder_time'] = now.isoformat()
                        del notifications[notif_id]
                        save_data()
                        logger.info(f"Уведомление {notif_id} перенесено в неотмеченные")
                
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
                        await show_pending_notification_actions(ADMIN_ID, notif_id, notif['text'])
                        notif['last_trigger'] = now.isoformat()
                        notif['repeat_count'] = notif.get('repeat_count', 0) + 1
                        pending_notifications[notif_id] = notif.copy()
                        pending_notifications[notif_id]['reminder_sent'] = True
                        pending_notifications[notif_id]['last_reminder_time'] = now.isoformat()
                        del notifications[notif_id]
                        save_data()
                        logger.info(f"Уведомление {notif_id} перенесено в неотмеченные (ежедневно)")
                
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
                            await show_pending_notification_actions(ADMIN_ID, notif_id, notif['text'])
                            notif['last_trigger'] = now.isoformat()
                            notif['repeat_count'] = notif.get('repeat_count', 0) + 1
                            pending_notifications[notif_id] = notif.copy()
                            pending_notifications[notif_id]['reminder_sent'] = True
                            pending_notifications[notif_id]['last_reminder_time'] = now.isoformat()
                            del notifications[notif_id]
                            save_data()
                            logger.info(f"Уведомление {notif_id} перенесено в неотмеченные (по дням недели)")
                
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
                        pending_notifications[notif_id] = notif.copy()
                        pending_notifications[notif_id]['reminder_sent'] = True
                        pending_notifications[notif_id]['last_reminder_time'] = now.isoformat()
                        del notifications[notif_id]
                        save_data()
                        logger.info(f"Уведомление {notif_id} перенесено в неотмеченные (одноразовое)")
        
        await asyncio.sleep(30)


async def sync_calendar_task():
    while True:
        try:
            await sync_calendar_to_pending()
            logger.info("Синхронизация календаря выполнена")
        except Exception as e:
            logger.error(f"Ошибка синхронизации: {e}")
        await asyncio.sleep(300)


def get_main_keyboard():
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        KeyboardButton("➕ Добавить"),
        KeyboardButton("📋 Список"),
        KeyboardButton("📅 События"),
        KeyboardButton("⚠️ Неотмеченные"),
        KeyboardButton("⚙️ Настройки")
    )
    return keyboard


async def update_notifications_list(chat_id: int, persistent: bool = False):
    if not notifications:
        msg = "📭 **Нет активных напоминаний**"
        if persistent:
            await send_persistent_message(chat_id, msg)
        else:
            await send_with_auto_delete(chat_id, msg, delay=3600)
        return
    
    now = get_current_time()
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    lines = []
    for notif_id, notif in notifications.items():
        if notif.get('is_completed'):
            continue
        repeat_type = notif.get('repeat_type', 'no')
        if repeat_type == 'every_hour':
            time_str = "🕐 Каждый час"
        elif repeat_type == 'every_day':
            hour = notif.get('repeat_hour', 0)
            minute = notif.get('repeat_minute', 0)
            time_str = f"📅 Ежедневно в {hour:02d}:{minute:02d}"
        elif repeat_type == 'weekdays':
            hour = notif.get('repeat_hour', 0)
            minute = notif.get('repeat_minute', 0)
            days = [WEEKDAYS_NAMES[d] for d in notif.get('weekdays_list', [])]
            time_str = f"📆 {', '.join(days)} в {hour:02d}:{minute:02d}"
        else:
            notify_time = datetime.fromisoformat(notif['time'])
            if notify_time.tzinfo is None:
                notify_time = tz.localize(notify_time)
            local_time = notify_time.astimezone(tz)
            time_str = f"⏰ {local_time.strftime('%d.%m.%Y %H:%M')}"
        lines.append(f"**{notif.get('num', notif_id)}. {notif['text']}** — {time_str}")
    
    if not lines:
        msg = "📭 **Нет активных напоминаний**"
    else:
        msg = "📋 **Мои напоминания:**\n\n" + "\n".join(lines) + f"\n\n📊 **Всего:** {len(lines)}"
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✏️ Ред. уведомление", callback_data="edit_local"),
        InlineKeyboardButton("🔄 Обновить", callback_data="refresh_list")
    )
    if persistent:
        await send_persistent_message(chat_id, msg, reply_markup=keyboard)
    else:
        await send_with_auto_delete(chat_id, msg, reply_markup=keyboard, delay=3600)


async def update_pending_list(chat_id: int, persistent: bool = False):
    if not pending_notifications:
        msg = "✅ **Нет неотмеченных уведомлений!**"
        if persistent:
            await send_persistent_message(chat_id, msg)
        else:
            await send_with_auto_delete(chat_id, msg, delay=3600)
        return
    
    now = get_current_time()
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    lines = []
    for notif_id, notif in pending_notifications.items():
        if notif.get('is_completed'):
            continue
        notify_time = datetime.fromisoformat(notif['time'])
        if notify_time.tzinfo is None:
            notify_time = tz.localize(notify_time)
        else:
            notify_time = notify_time.astimezone(tz)
        repeat_count = notif.get('repeat_count', 0)
        repeat_text = f" (повторений: {repeat_count})" if repeat_count > 0 else ""
        lines.append(f"• **{notif['text']}**\n  ⏰ {notify_time.strftime('%d.%m.%Y %H:%M')}{repeat_text}")
    
    if not lines:
        msg = "✅ **Нет неотмеченных уведомлений!**"
    else:
        msg = "⚠️ **НЕОТМЕЧЕННЫЕ УВЕДОМЛЕНИЯ**\n\nЭти напоминания просрочены и будут повторяться каждый час.\n\n" + "\n".join(lines) + f"\n\n📊 **Всего:** {len(lines)}"
    
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Выполнить все", callback_data="pend_complete_all"),
        InlineKeyboardButton("✏️ Редактировать", callback_data="pend_edit_list"),
        InlineKeyboardButton("🔄 Обновить", callback_data="refresh_pending")
    )
    if persistent:
        await send_persistent_message(chat_id, msg, reply_markup=keyboard)
    else:
        await send_with_auto_delete(chat_id, msg, reply_markup=keyboard, delay=3600)


# === ОСНОВНЫЕ ОБРАБОТЧИКИ ===

@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    await state.finish()
    if ADMIN_ID and message.from_user.id != ADMIN_ID:
        await message.reply("❌ У вас нет доступа")
        return
    
    caldav_ok, caldav_msg = await check_caldav_connection()
    caldav_status = "✅ Доступен" if caldav_ok else "❌ Ошибка"
    
    welcome = f"👋 Добро пожаловать!\n🤖 Версия v{BOT_VERSION}\n📧 CalDAV: {caldav_status}\n🌍 Часовой пояс: {config.get('timezone', 'Europe/Moscow')}\n\n⚠️ Неотмеченные уведомления повторяются каждый час."
    if not caldav_ok:
        welcome += f"\n\n⚠️ {caldav_msg}"
    
    await send_persistent_message(message.chat.id, welcome)
    await send_persistent_message(message.chat.id, "👋 Выберите действие:", reply_markup=get_main_keyboard())
    await update_notifications_list(message.chat.id, persistent=True)
    await update_pending_list(message.chat.id, persistent=True)
    now = get_current_time()
    await show_calendar_events(message.chat.id, now.year, now.month, persistent=True)


@dp.message_handler(commands=['menu'])
async def show_menu_command(message: types.Message):
    await delete_user_message(message)
    await send_persistent_message(message.chat.id, "👋 Главное меню:", reply_markup=get_main_keyboard())
    await update_notifications_list(message.chat.id, persistent=True)
    await update_pending_list(message.chat.id, persistent=True)
    now = get_current_time()
    await show_calendar_events(message.chat.id, now.year, now.month, persistent=True)


@dp.message_handler(lambda m: m.text == "➕ Добавить", state='*')
async def add_notification_universal(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    await state.finish()
    await send_with_auto_delete(message.chat.id, "✏️ Введите текст уведомления:", delay=3600)
    await NotificationStates.waiting_for_text.set()


@dp.message_handler(state=NotificationStates.waiting_for_text)
async def get_notification_text(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    if not message.text:
        await send_with_auto_delete(message.chat.id, "❌ Введите текст.", delay=3600)
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
    await send_with_auto_delete(message.chat.id, "⏱️ Когда уведомить?", reply_markup=keyboard, delay=3600)
    await NotificationStates.waiting_for_time_type.set()


@dp.callback_query_handler(lambda c: c.data == "time_specific", state=NotificationStates.waiting_for_time_type)
async def process_specific_time(callback: types.CallbackQuery, state: FSMContext):
    await send_with_auto_delete(callback.from_user.id, "🗓️ Введите дату и время (например: 17.04 21:00)", delay=3600)
    await NotificationStates.waiting_for_specific_date.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "time_hours", state=NotificationStates.waiting_for_time_type)
async def process_hours(callback: types.CallbackQuery, state: FSMContext):
    await send_with_auto_delete(callback.from_user.id, "⌛ Введите количество часов", delay=3600)
    await NotificationStates.waiting_for_hours.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "time_days", state=NotificationStates.waiting_for_time_type)
async def process_days(callback: types.CallbackQuery, state: FSMContext):
    await send_with_auto_delete(callback.from_user.id, "📅 Введите количество дней", delay=3600)
    await NotificationStates.waiting_for_days.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "time_months", state=NotificationStates.waiting_for_time_type)
async def process_months(callback: types.CallbackQuery, state: FSMContext):
    await send_with_auto_delete(callback.from_user.id, "📆 Введите количество месяцев", delay=3600)
    await NotificationStates.waiting_for_months.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "time_every_hour", state=NotificationStates.waiting_for_time_type)
async def process_every_hour(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    next_num = len(notifications) + 1
    notif_id = str(next_num)
    now = get_current_time()
    notifications[notif_id] = {
        'text': data['text'], 'time': now.isoformat(), 'created': now.isoformat(),
        'is_completed': False, 'num': next_num, 'repeat_type': 'every_hour',
        'last_trigger': now.isoformat(), 'repeat_count': 0, 'reminder_sent': False
    }
    save_data()
    await sync_notification_to_calendar(notif_id, 'create')
    await send_with_auto_delete(callback.from_user.id, f"✅ Уведомление #{next_num} создано! Каждый час", delay=3600)
    await state.finish()
    await update_notifications_list(callback.from_user.id, persistent=True)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "time_every_day", state=NotificationStates.waiting_for_time_type)
async def process_every_day(callback: types.CallbackQuery, state: FSMContext):
    await send_with_auto_delete(callback.from_user.id, "⏰ Введите время (ЧЧ:ММ)", delay=3600)
    await NotificationStates.waiting_for_every_day_time.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "time_weekdays", state=NotificationStates.waiting_for_time_type)
async def process_weekdays(callback: types.CallbackQuery, state: FSMContext):
    keyboard = InlineKeyboardMarkup(row_width=3)
    for name, day in WEEKDAYS_BUTTONS:
        keyboard.add(InlineKeyboardButton(name, callback_data=f"wd_{day}"))
    keyboard.add(InlineKeyboardButton("✅ Готово", callback_data="wd_done"))
    await send_with_auto_delete(callback.from_user.id, "📅 Выберите дни недели", reply_markup=keyboard, delay=3600)
    await state.update_data(selected_weekdays=[])
    await NotificationStates.waiting_for_weekdays.set()
    await callback.answer()


@dp.message_handler(state=NotificationStates.waiting_for_specific_date)
async def set_specific_date(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    notify_time = parse_datetime(message.text)
    if notify_time is None or notify_time <= get_current_time():
        await send_with_auto_delete(message.chat.id, "❌ Неверный формат или дата в прошлом!", delay=3600)
        return
    data = await state.get_data()
    next_num = len(notifications) + 1
    notif_id = str(next_num)
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    if notify_time.tzinfo is None:
        notify_time = tz.localize(notify_time)
    notifications[notif_id] = {
        'text': data['text'], 'time': notify_time.isoformat(), 'created': get_current_time().isoformat(),
        'is_completed': False, 'num': next_num, 'repeat_type': 'no',
        'repeat_count': 0, 'reminder_sent': False
    }
    save_data()
    await sync_notification_to_calendar(notif_id, 'create')
    await send_with_auto_delete(message.chat.id, f"✅ Уведомление #{next_num} создано!\n⏰ {notify_time.strftime('%d.%m.%Y %H:%M')}", delay=3600)
    await state.finish()
    await update_notifications_list(message.chat.id, persistent=True)


@dp.message_handler(state=NotificationStates.waiting_for_hours)
async def set_hours(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    try:
        hours = int(message.text)
        if hours <= 0:
            raise ValueError
        data = await state.get_data()
        notify_time = get_current_time() + timedelta(hours=hours)
        next_num = len(notifications) + 1
        notif_id = str(next_num)
        tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
        if notify_time.tzinfo is None:
            notify_time = tz.localize(notify_time)
        notifications[notif_id] = {
            'text': data['text'], 'time': notify_time.isoformat(), 'created': get_current_time().isoformat(),
            'is_completed': False, 'num': next_num, 'repeat_type': 'no',
            'repeat_count': 0, 'reminder_sent': False
        }
        save_data()
        await sync_notification_to_calendar(notif_id, 'create')
        await send_with_auto_delete(message.chat.id, f"✅ Уведомление #{next_num} создано!\n⏰ {notify_time.strftime('%d.%m.%Y %H:%M')}", delay=3600)
        await state.finish()
        await update_notifications_list(message.chat.id, persistent=True)
    except:
        await send_with_auto_delete(message.chat.id, "❌ Введите положительное число!", delay=3600)


@dp.message_handler(state=NotificationStates.waiting_for_days)
async def set_days(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    try:
        days = int(message.text)
        if days <= 0:
            raise ValueError
        data = await state.get_data()
        notify_time = get_current_time() + timedelta(days=days)
        next_num = len(notifications) + 1
        notif_id = str(next_num)
        tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
        if notify_time.tzinfo is None:
            notify_time = tz.localize(notify_time)
        notifications[notif_id] = {
            'text': data['text'], 'time': notify_time.isoformat(), 'created': get_current_time().isoformat(),
            'is_completed': False, 'num': next_num, 'repeat_type': 'no',
            'repeat_count': 0, 'reminder_sent': False
        }
        save_data()
        await sync_notification_to_calendar(notif_id, 'create')
        await send_with_auto_delete(message.chat.id, f"✅ Уведомление #{next_num} создано!\n⏰ {notify_time.strftime('%d.%m.%Y %H:%M')}", delay=3600)
        await state.finish()
        await update_notifications_list(message.chat.id, persistent=True)
    except:
        await send_with_auto_delete(message.chat.id, "❌ Введите положительное число!", delay=3600)


@dp.message_handler(state=NotificationStates.waiting_for_months)
async def set_months(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    try:
        months = int(message.text)
        if months <= 0:
            raise ValueError
        data = await state.get_data()
        notify_time = get_current_time() + timedelta(days=months*30)
        next_num = len(notifications) + 1
        notif_id = str(next_num)
        tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
        if notify_time.tzinfo is None:
            notify_time = tz.localize(notify_time)
        notifications[notif_id] = {
            'text': data['text'], 'time': notify_time.isoformat(), 'created': get_current_time().isoformat(),
            'is_completed': False, 'num': next_num, 'repeat_type': 'no',
            'repeat_count': 0, 'reminder_sent': False
        }
        save_data()
        await sync_notification_to_calendar(notif_id, 'create')
        await send_with_auto_delete(message.chat.id, f"✅ Уведомление #{next_num} создано!\n⏰ {notify_time.strftime('%d.%m.%Y %H:%M')}", delay=3600)
        await state.finish()
        await update_notifications_list(message.chat.id, persistent=True)
    except:
        await send_with_auto_delete(message.chat.id, "❌ Введите положительное число!", delay=3600)


@dp.message_handler(state=NotificationStates.waiting_for_every_day_time)
async def set_every_day_time(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    match = re.match(r'^(\d{1,2}):(\d{2})$', message.text.strip())
    if not match:
        await send_with_auto_delete(message.chat.id, "❌ Неверный формат! Используйте ЧЧ:ММ", delay=3600)
        return
    hour, minute = map(int, match.groups())
    if hour > 23 or minute > 59:
        await send_with_auto_delete(message.chat.id, "❌ Некорректное время!", delay=3600)
        return
    data = await state.get_data()
    next_num = len(notifications) + 1
    notif_id = str(next_num)
    now = get_current_time()
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    first_time = tz.localize(datetime(now.year, now.month, now.day, hour, minute, 0))
    if first_time <= now:
        first_time += timedelta(days=1)
    notifications[notif_id] = {
        'text': data['text'], 'time': first_time.isoformat(), 'created': now.isoformat(),
        'is_completed': False, 'num': next_num, 'repeat_type': 'every_day',
        'repeat_hour': hour, 'repeat_minute': minute,
        'last_trigger': (first_time - timedelta(days=1)).isoformat(),
        'repeat_count': 0, 'reminder_sent': False
    }
    save_data()
    await sync_notification_to_calendar(notif_id, 'create')
    await send_with_auto_delete(message.chat.id, f"✅ Уведомление #{next_num} создано! Ежедневно в {hour:02d}:{minute:02d}", delay=3600)
    await state.finish()
    await update_notifications_list(message.chat.id, persistent=True)


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
    await callback.message.edit_text(f"📅 Выберите дни недели\n\n{status_text}", reply_markup=keyboard)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "wd_done", state=NotificationStates.waiting_for_weekdays)
async def weekdays_done(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected = data.get('selected_weekdays', [])
    if not selected:
        await callback.answer("❌ Выберите хотя бы один день!")
        return
    await state.update_data(weekdays_list=selected)
    await send_with_auto_delete(callback.from_user.id, "⏰ Введите время (ЧЧ:ММ)", delay=3600)
    await NotificationStates.waiting_for_weekday_time.set()
    await callback.answer()


@dp.message_handler(state=NotificationStates.waiting_for_weekday_time)
async def set_weekday_time(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    match = re.match(r'^(\d{1,2}):(\d{2})$', message.text.strip())
    if not match:
        await send_with_auto_delete(message.chat.id, "❌ Неверный формат!", delay=3600)
        return
    hour, minute = map(int, match.groups())
    if hour > 23 or minute > 59:
        await send_with_auto_delete(message.chat.id, "❌ Некорректное время!", delay=3600)
        return
    data = await state.get_data()
    weekdays_list = data.get('weekdays_list', [])
    if not weekdays_list:
        await send_with_auto_delete(message.chat.id, "❌ Не выбраны дни недели!", delay=3600)
        return
    first_time = get_next_weekday(weekdays_list, hour, minute)
    if not first_time:
        await send_with_auto_delete(message.chat.id, "❌ Не удалось определить дату!", delay=3600)
        return
    next_num = len(notifications) + 1
    notif_id = str(next_num)
    days_names = [WEEKDAYS_NAMES[d] for d in sorted(weekdays_list)]
    notifications[notif_id] = {
        'text': data['text'], 'time': first_time.isoformat(), 'created': get_current_time().isoformat(),
        'is_completed': False, 'num': next_num, 'repeat_type': 'weekdays',
        'repeat_hour': hour, 'repeat_minute': minute, 'weekdays_list': weekdays_list,
        'last_trigger': (first_time - timedelta(days=7)).isoformat(),
        'repeat_count': 0, 'reminder_sent': False
    }
    save_data()
    await sync_notification_to_calendar(notif_id, 'create')
    await send_with_auto_delete(message.chat.id, f"✅ Уведомление #{next_num} создано!\n📆 {', '.join(days_names)} в {hour:02d}:{minute:02d}", delay=3600)
    await state.finish()
    await update_notifications_list(message.chat.id, persistent=True)


# === ОБРАБОТЧИКИ ДЛЯ НЕОТМЕЧЕННЫХ ===

@dp.callback_query_handler(lambda c: c.data == "refresh_pending")
async def refresh_pending(callback: types.CallbackQuery):
    await update_pending_list(callback.from_user.id, persistent=True)
    await callback.answer("Обновлено")


@dp.callback_query_handler(lambda c: c.data == "pend_complete_all")
async def pending_complete_all(callback: types.CallbackQuery):
    for notif_id in list(pending_notifications.keys()):
        notif = pending_notifications[notif_id]
        if not notif.get('is_completed'):
            if 'calendar_event_id' in notif:
                await sync_notification_to_calendar(notif_id, 'delete')
            del pending_notifications[notif_id]
    save_data()
    await callback.message.edit_text("✅ Все неотмеченные уведомления выполнены!")
    await update_pending_list(callback.from_user.id, persistent=True)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "pend_edit_list")
async def pending_edit_list(callback: types.CallbackQuery, state: FSMContext):
    if not pending_notifications:
        await callback.answer("Нет неотмеченных")
        return
    keyboard = InlineKeyboardMarkup(row_width=1)
    for notif_id, notif in pending_notifications.items():
        if notif.get('is_completed'):
            continue
        keyboard.add(InlineKeyboardButton(f"{notif['text'][:40]}...", callback_data=f"pend_edit_{notif_id}"))
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_edit"))
    await callback.message.edit_text("✏️ Выберите уведомление:", reply_markup=keyboard)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('pend_done_'))
async def pending_done(callback: types.CallbackQuery):
    notif_id = callback.data.replace('pend_done_', '')
    if notif_id in pending_notifications:
        notif = pending_notifications[notif_id]
        if 'calendar_event_id' in notif:
            await sync_notification_to_calendar(notif_id, 'delete')
        del pending_notifications[notif_id]
        save_data()
        await callback.message.edit_text("✅ Уведомление выполнено!")
        await update_pending_list(callback.from_user.id, persistent=True)
    else:
        await callback.answer("Уведомление не найдено")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('pend_edit_'))
async def pending_edit(callback: types.CallbackQuery, state: FSMContext):
    notif_id = callback.data.replace('pend_edit_', '')
    if notif_id not in pending_notifications:
        await callback.answer("Не найдено")
        return
    await state.update_data(edit_id=notif_id, is_pending=True)
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✏️ Изменить текст", callback_data=f"pend_chtext_{notif_id}"),
        InlineKeyboardButton("⏰ Изменить время", callback_data=f"pend_chtime_{notif_id}")
    )
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_edit"))
    await callback.message.edit_text(f"✏️ Что изменить?\n📝 {pending_notifications[notif_id]['text']}", reply_markup=keyboard)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('pend_snooze_') and not c.data.startswith(('pend_snooze_1h_', 'pend_snooze_3h_', 'pend_snooze_1d_', 'pend_snooze_7d_', 'pend_snooze_custom_')))
async def pending_snooze(callback: types.CallbackQuery, state: FSMContext):
    notif_id = callback.data.replace('pend_snooze_', '')
    if notif_id not in pending_notifications:
        await callback.answer("Не найдено")
        return
    await state.update_data(snooze_notif_id=notif_id)
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("⏰ На 1 час", callback_data=f"pend_snooze_1h_{notif_id}"),
        InlineKeyboardButton("⏰ На 3 часа", callback_data=f"pend_snooze_3h_{notif_id}"),
        InlineKeyboardButton("📅 На 1 день", callback_data=f"pend_snooze_1d_{notif_id}"),
        InlineKeyboardButton("📅 На 7 дней", callback_data=f"pend_snooze_7d_{notif_id}"),
        InlineKeyboardButton("🎯 Свой вариант", callback_data=f"pend_snooze_custom_{notif_id}"),
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_snooze")
    )
    await callback.message.edit_text(f"⏰ Отложить?\n📝 {pending_notifications[notif_id]['text']}", reply_markup=keyboard)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('pend_hour_'))
async def pending_hour(callback: types.CallbackQuery):
    notif_id = callback.data.replace('pend_hour_', '')
    await process_pending_snooze(callback, notif_id, 1, "hours")


@dp.callback_query_handler(lambda c: c.data.startswith('pend_snooze_1h_'))
async def pending_snooze_1h(callback: types.CallbackQuery):
    notif_id = callback.data.replace('pend_snooze_1h_', '')
    await process_pending_snooze(callback, notif_id, 1, "hours")


@dp.callback_query_handler(lambda c: c.data.startswith('pend_snooze_3h_'))
async def pending_snooze_3h(callback: types.CallbackQuery):
    notif_id = callback.data.replace('pend_snooze_3h_', '')
    await process_pending_snooze(callback, notif_id, 3, "hours")


@dp.callback_query_handler(lambda c: c.data.startswith('pend_snooze_1d_'))
async def pending_snooze_1d(callback: types.CallbackQuery):
    notif_id = callback.data.replace('pend_snooze_1d_', '')
    await process_pending_snooze(callback, notif_id, 1, "days")


@dp.callback_query_handler(lambda c: c.data.startswith('pend_snooze_7d_'))
async def pending_snooze_7d(callback: types.CallbackQuery):
    notif_id = callback.data.replace('pend_snooze_7d_', '')
    await process_pending_snooze(callback, notif_id, 7, "days")


async def process_pending_snooze(callback: types.CallbackQuery, notif_id: str, value: int, unit: str):
    if notif_id not in pending_notifications:
        await callback.answer("Не найдено")
        return
    now = get_current_time()
    new_time = now + (timedelta(hours=value) if unit == "hours" else timedelta(days=value))
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
    await callback.message.edit_text(f"⏰ Уведомление отложено на {value} {unit}\n🕐 {new_time.strftime('%d.%m.%Y %H:%M')}")
    await update_pending_list(callback.from_user.id, persistent=True)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('pend_snooze_custom_'))
async def pending_snooze_custom(callback: types.CallbackQuery, state: FSMContext):
    notif_id = callback.data.replace('pend_snooze_custom_', '')
    await state.update_data(snooze_notif_id=notif_id)
    await send_with_auto_delete(callback.from_user.id, "🗓️ Введите новую дату и время", delay=3600)
    await SnoozeStates.waiting_for_specific_date.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('pend_chtext_'))
async def pending_chtext(callback: types.CallbackQuery, state: FSMContext):
    notif_id = callback.data.replace('pend_chtext_', '')
    if notif_id not in pending_notifications:
        await callback.answer("Не найдено")
        return
    await state.update_data(edit_id=notif_id, is_pending=True)
    await send_with_auto_delete(callback.from_user.id, f"✏️ Введите новый текст:\nСтарый: {pending_notifications[notif_id]['text']}", delay=3600)
    await NotificationStates.waiting_for_edit_text.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('pend_chtime_'))
async def pending_chtime(callback: types.CallbackQuery, state: FSMContext):
    notif_id = callback.data.replace('pend_chtime_', '')
    if notif_id not in pending_notifications:
        await callback.answer("Не найдено")
        return
    await state.update_data(edit_id=notif_id, is_pending=True)
    await send_with_auto_delete(callback.from_user.id, "🗓️ Введите новую дату и время", delay=3600)
    await NotificationStates.waiting_for_specific_date.set()
    await callback.answer()


@dp.message_handler(state=NotificationStates.waiting_for_edit_text)
async def save_edited_text(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    data = await state.get_data()
    edit_id = data.get('edit_id')
    is_pending = data.get('is_pending', False)
    target = pending_notifications if is_pending else notifications
    if not edit_id or edit_id not in target:
        await send_with_auto_delete(message.chat.id, "❌ Уведомление не найдено!", delay=3600)
        await state.finish()
        return
    old_text = target[edit_id]['text']
    target[edit_id]['text'] = message.text
    target[edit_id]['reminder_sent'] = False
    target[edit_id]['repeat_count'] = 0
    target[edit_id]['last_reminder_time'] = None
    save_data()
    await sync_notification_to_calendar(edit_id, 'create')
    await send_with_auto_delete(message.chat.id, f"✅ Текст изменён!\nСтарый: {old_text}\nНовый: {message.text}", delay=3600)
    if is_pending:
        await update_pending_list(message.chat.id, persistent=True)
    else:
        await update_notifications_list(message.chat.id, persistent=True)
    await state.finish()


@dp.message_handler(state=SnoozeStates.waiting_for_specific_date)
async def snooze_set_specific_date(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    notify_time = parse_datetime(message.text)
    if notify_time is None or notify_time <= get_current_time():
        await send_with_auto_delete(message.chat.id, "❌ Неверный формат или дата в прошлом!", delay=3600)
        return
    data = await state.get_data()
    notif_id = data.get('snooze_notif_id')
    if not notif_id or notif_id not in pending_notifications:
        await send_with_auto_delete(message.chat.id, "❌ Уведомление не найдено!", delay=3600)
        await state.finish()
        return
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    if notify_time.tzinfo is None:
        notify_time = tz.localize(notify_time)
    pending_notifications[notif_id]['time'] = notify_time.isoformat()
    pending_notifications[notif_id]['reminder_sent'] = False
    pending_notifications[notif_id]['repeat_count'] = 0
    pending_notifications[notif_id]['last_reminder_time'] = None
    save_data()
    await sync_notification_to_calendar(notif_id, 'create')
    await send_with_auto_delete(message.chat.id, f"⏰ Уведомление отложено!\n🕐 {notify_time.strftime('%d.%m.%Y %H:%M')}", delay=3600)
    await update_pending_list(message.chat.id, persistent=True)
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


@dp.callback_query_handler(lambda c: c.data == "refresh_list")
async def refresh_list(callback: types.CallbackQuery):
    await update_notifications_list(callback.from_user.id, persistent=True)
    await callback.answer("Обновлено")


@dp.callback_query_handler(lambda c: c.data == "edit_local")
async def edit_local_handler(callback: types.CallbackQuery, state: FSMContext):
    active = {nid: n for nid, n in notifications.items() if not n.get('is_completed')}
    if not active:
        await callback.answer("Нет активных уведомлений")
        return
    keyboard = InlineKeyboardMarkup(row_width=1)
    for notif_id, notif in sorted(active.items(), key=lambda x: int(x[0])):
        keyboard.add(InlineKeyboardButton(f"#{notif.get('num', notif_id)}: {notif['text'][:40]}...", callback_data=f"sel_notif_{notif_id}"))
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_edit"))
    await callback.message.edit_text("✏️ Выберите уведомление для редактирования:", reply_markup=keyboard)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('sel_notif_'))
async def edit_selected_notification(callback: types.CallbackQuery, state: FSMContext):
    notif_id = callback.data.replace('sel_notif_', '')
    if notif_id not in notifications:
        await callback.answer("Не найдено")
        return
    await state.update_data(edit_id=notif_id, is_pending=False)
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✏️ Изменить текст", callback_data=f"chtext_{notif_id}"),
        InlineKeyboardButton("⏰ Изменить время", callback_data=f"chtime_{notif_id}")
    )
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_edit"))
    await callback.message.edit_text(f"✏️ Что изменить в уведомлении #{notifications[notif_id].get('num', notif_id)}?\n📝 {notifications[notif_id]['text'][:50]}...", reply_markup=keyboard)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('chtext_'))
async def change_notification_text(callback: types.CallbackQuery, state: FSMContext):
    notif_id = callback.data.replace('chtext_', '')
    if notif_id not in notifications:
        await callback.answer("Не найдено")
        return
    await state.update_data(edit_id=notif_id, is_pending=False)
    await send_with_auto_delete(callback.from_user.id, f"✏️ Введите новый текст:\nСтарый: {notifications[notif_id]['text']}", delay=3600)
    await NotificationStates.waiting_for_edit_text.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith('chtime_'))
async def change_notification_time(callback: types.CallbackQuery, state: FSMContext):
    notif_id = callback.data.replace('chtime_', '')
    if notif_id not in notifications:
        await callback.answer("Не найдено")
        return
    await state.update_data(edit_id=notif_id, is_pending=False)
    await send_with_auto_delete(callback.from_user.id, "🗓️ Введите новую дату и время", delay=3600)
    await NotificationStates.waiting_for_specific_date.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "cancel_edit")
async def cancel_edit_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()
    await update_notifications_list(callback.from_user.id, persistent=True)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "cancel_snooze")
async def cancel_snooze_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.finish()
    await callback.message.edit_text("✅ Откладывание отменено")
    await callback.answer()


# === ОБРАБОТЧИКИ КАЛЕНДАРЯ ===

@dp.callback_query_handler(lambda c: c.data.startswith("cal_prev_"))
async def calendar_prev_month(callback: types.CallbackQuery):
    parts = callback.data.replace("cal_prev_", "").split("_")
    year, month = int(parts[0]), int(parts[1])
    await show_calendar_events(callback.from_user.id, year, month, persistent=True)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("cal_next_"))
async def calendar_next_month(callback: types.CallbackQuery):
    parts = callback.data.replace("cal_next_", "").split("_")
    year, month = int(parts[0]), int(parts[1])
    await show_calendar_events(callback.from_user.id, year, month, persistent=True)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("cal_refresh_"))
async def calendar_refresh(callback: types.CallbackQuery):
    parts = callback.data.replace("cal_refresh_", "").split("_")
    year, month = int(parts[0]), int(parts[1])
    await show_calendar_events(callback.from_user.id, year, month, force_refresh=True, persistent=True)
    await callback.answer("Календарь обновлён")


@dp.callback_query_handler(lambda c: c.data.startswith("cal_sync_"))
async def calendar_sync(callback: types.CallbackQuery):
    parts = callback.data.replace("cal_sync_", "").split("_")
    year, month = int(parts[0]), int(parts[1])
    await callback.message.edit_text("🔄 Синхронизация...")
    if get_caldav_available():
        caldav_api = CalDAVCalendarAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
        events = await caldav_api.get_month_events(year, month)
        calendar_events_cache[f"{year}_{month}"] = events
        last_calendar_update[f"{year}_{month}"] = get_current_time()
        await show_calendar_events(callback.from_user.id, year, month, force_refresh=True, persistent=True)
        await callback.answer("Синхронизация завершена")
    else:
        await callback.answer("CalDAV не настроен")
        await show_calendar_events(callback.from_user.id, year, month, persistent=True)


@dp.callback_query_handler(lambda c: c.data == "curr_month")
async def calendar_current_month(callback: types.CallbackQuery):
    now = get_current_time()
    await show_calendar_events(callback.from_user.id, now.year, now.month, persistent=True)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "edit_calendar")
async def edit_calendar_handler(callback: types.CallbackQuery, state: FSMContext):
    now = get_current_time()
    year, month = now.year, now.month
    await update_calendar_events_cache(year, month)
    events = calendar_events_cache.get(f"{year}_{month}", [])
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    future_events = []
    for event in events:
        try:
            start_dt = datetime.fromisoformat(event['start'])
            if start_dt.tzinfo is None:
                start_dt = tz.localize(start_dt)
            else:
                start_dt = start_dt.astimezone(tz)
            if start_dt >= today_start:
                future_events.append((start_dt, event))
        except:
            continue
    if not future_events:
        await callback.answer("Нет событий для редактирования")
        return
    keyboard = InlineKeyboardMarkup(row_width=1)
    for idx, (start_dt, event) in enumerate(future_events[:20]):
        short_id = hashlib.md5(event['id'].encode()).hexdigest()[:16]
        event_id_map[short_id] = event['id']
        keyboard.add(InlineKeyboardButton(f"{idx+1}. {start_dt.strftime('%d.%m %H:%M')} - {event['summary'][:30]}", callback_data=f"sel_cal_event_{short_id}"))
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_edit"))
    await callback.message.edit_text("✏️ Выберите событие для редактирования:", reply_markup=keyboard)
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
    await callback.message.edit_text("✏️ Что изменить в событии?", reply_markup=keyboard)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "edit_cal_text", state=EditCalendarEventStates.waiting_for_event_selection)
async def edit_event_text_prompt(callback: types.CallbackQuery, state: FSMContext):
    await send_with_auto_delete(callback.from_user.id, "✏️ Введите новый текст события:", delay=3600)
    await EditCalendarEventStates.waiting_for_new_text.set()
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "edit_cal_time", state=EditCalendarEventStates.waiting_for_event_selection)
async def edit_event_time_prompt(callback: types.CallbackQuery, state: FSMContext):
    await send_with_auto_delete(callback.from_user.id, "🗓️ Введите новую дату и время", delay=3600)
    await EditCalendarEventStates.waiting_for_new_datetime.set()
    await callback.answer()


@dp.message_handler(state=EditCalendarEventStates.waiting_for_new_text)
async def save_edited_event_text(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    data = await state.get_data()
    event_id = data.get('edit_event_id')
    if not event_id:
        await send_with_auto_delete(message.chat.id, "❌ Ошибка", delay=3600)
        await state.finish()
        return
    now = get_current_time()
    await update_calendar_events_cache(now.year, now.month)
    events = calendar_events_cache.get(f"{now.year}_{now.month}", [])
    target_event = next((ev for ev in events if ev['id'] == event_id), None)
    if not target_event:
        await send_with_auto_delete(message.chat.id, "❌ Событие не найдено", delay=3600)
        await state.finish()
        return
    caldav_api = CalDAVCalendarAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
    start_dt = datetime.fromisoformat(target_event['start'])
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    if start_dt.tzinfo is None:
        start_dt = tz.localize(start_dt)
    success = await caldav_api.update_event(event_id, new_summary=message.text, new_start=start_dt)
    if success:
        await send_with_auto_delete(message.chat.id, f"✅ Текст изменён!\n{message.text}", delay=3600)
        await update_calendar_events_cache(now.year, now.month, force=True)
        await sync_calendar_to_pending()
    else:
        await send_with_auto_delete(message.chat.id, "❌ Ошибка обновления", delay=3600)
    await state.finish()
    await update_notifications_list(message.chat.id, persistent=True)


@dp.message_handler(state=EditCalendarEventStates.waiting_for_new_datetime)
async def save_edited_event_datetime(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    new_dt = parse_datetime(message.text)
    if new_dt is None or new_dt <= get_current_time():
        await send_with_auto_delete(message.chat.id, "❌ Неверный формат или дата в прошлом!", delay=3600)
        return
    data = await state.get_data()
    event_id = data.get('edit_event_id')
    if not event_id:
        await send_with_auto_delete(message.chat.id, "❌ Ошибка", delay=3600)
        await state.finish()
        return
    now = get_current_time()
    await update_calendar_events_cache(now.year, now.month)
    events = calendar_events_cache.get(f"{now.year}_{now.month}", [])
    target_event = next((ev for ev in events if ev['id'] == event_id), None)
    if not target_event:
        await send_with_auto_delete(message.chat.id, "❌ Событие не найдено", delay=3600)
        await state.finish()
        return
    caldav_api = CalDAVCalendarAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
    success = await caldav_api.update_event(event_id, new_summary=target_event['summary'], new_start=new_dt)
    if success:
        await send_with_auto_delete(message.chat.id, f"✅ Дата изменена!\n🕐 {new_dt.strftime('%d.%m.%Y %H:%M')}", delay=3600)
        await update_calendar_events_cache(now.year, now.month, force=True)
        await sync_calendar_to_pending()
    else:
        await send_with_auto_delete(message.chat.id, "❌ Ошибка обновления", delay=3600)
    await state.finish()
    await update_notifications_list(message.chat.id, persistent=True)


# === НАСТРОЙКИ ===

async def settings_menu_handler(message: types.Message, state: FSMContext):
    global notifications_enabled
    status = "🔔 Вкл" if notifications_enabled else "🔕 Выкл"
    cal_sync = "✅ Вкл" if config.get('calendar_sync_enabled', True) else "❌ Выкл"
    if get_caldav_available():
        ok, _ = await check_caldav_connection()
        caldav_status = "✅ Доступен" if ok else "❌ Ошибка"
    else:
        caldav_status = "❌ Не настроен"
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton(f"Уведомления: {status}", callback_data="toggle_notify"),
        InlineKeyboardButton(f"Синхр. с календарём: {cal_sync}", callback_data="toggle_cal_sync"),
        InlineKeyboardButton("🕐 Время проверки", callback_data="set_check_time"),
        InlineKeyboardButton("🌍 Часовой пояс", callback_data="set_timezone"),
        InlineKeyboardButton("🔍 Проверить календарь", callback_data="check_cal"),
        InlineKeyboardButton("ℹ️ Информация", callback_data="info")
    )
    await send_with_auto_delete(message.chat.id, f"⚙️ НАСТРОЙКИ\n\n📧 CalDAV: {caldav_status}\n🌍 Часовой пояс: {config.get('timezone', 'Europe/Moscow')}", reply_markup=keyboard, delay=3600)


@dp.callback_query_handler(lambda c: c.data == "check_cal")
async def check_calendar_connection(callback: types.CallbackQuery):
    if not get_caldav_available():
        await callback.message.edit_text("❌ CalDAV не настроен!")
        await callback.answer()
        return
    await callback.message.edit_text("🔍 Проверка...")
    ok, msg = await check_caldav_connection()
    if ok:
        await callback.message.edit_text(f"✅ {msg}")
        now = get_current_time()
        await update_calendar_events_cache(now.year, now.month, force=True)
        await sync_calendar_to_pending()
    else:
        await callback.message.edit_text(f"❌ {msg}\n\nПолучите новый пароль приложения: https://id.yandex.ru/security/app-passwords")
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "toggle_notify")
async def toggle_notifications(callback: types.CallbackQuery, state: FSMContext):
    global notifications_enabled
    notifications_enabled = not notifications_enabled
    config['notifications_enabled'] = notifications_enabled
    save_data()
    await callback.message.edit_text(f"✅ Уведомления {'включены' if notifications_enabled else 'выключены'}")
    await settings_menu_handler(callback.message, state)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "toggle_cal_sync")
async def toggle_calendar_sync(callback: types.CallbackQuery, state: FSMContext):
    current = config.get('calendar_sync_enabled', True)
    config['calendar_sync_enabled'] = not current
    save_data()
    if config['calendar_sync_enabled'] and get_caldav_available():
        ok, _ = await check_caldav_connection()
        if ok:
            for nid in notifications:
                if 'calendar_event_id' not in notifications[nid]:
                    await sync_notification_to_calendar(nid, 'create')
            for nid in pending_notifications:
                if 'calendar_event_id' not in pending_notifications[nid]:
                    await sync_notification_to_calendar(nid, 'create')
            await callback.message.edit_text("✅ Синхронизация включена")
            now = get_current_time()
            await update_calendar_events_cache(now.year, now.month, force=True)
            await sync_calendar_to_pending()
        else:
            await callback.message.edit_text("⚠️ Синхронизация включена, но нет доступа к календарю")
    else:
        await callback.message.edit_text(f"✅ Синхронизация {'включена' if config['calendar_sync_enabled'] else 'выключена'}")
    await settings_menu_handler(callback.message, state)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "set_check_time")
async def set_check_time(callback: types.CallbackQuery):
    await send_with_auto_delete(callback.from_user.id, f"🕐 Текущее время: {config.get('daily_check_time', '06:00')}\nВведите новое (ЧЧ:ММ):", delay=3600)
    await SettingsStates.waiting_for_check_time.set()
    await callback.answer()


@dp.message_handler(state=SettingsStates.waiting_for_check_time)
async def save_check_time(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    try:
        datetime.strptime(message.text, "%H:%M")
        config['daily_check_time'] = message.text
        save_data()
        await send_with_auto_delete(message.chat.id, f"✅ Время проверки: {message.text}", delay=3600)
    except:
        await send_with_auto_delete(message.chat.id, "❌ Неверный формат! Используйте ЧЧ:ММ", delay=3600)
    await state.finish()


@dp.callback_query_handler(lambda c: c.data == "set_timezone")
async def set_timezone(callback: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(row_width=2)
    for name in TIMEZONES.keys():
        keyboard.add(InlineKeyboardButton(name, callback_data=f"tz_{name}"))
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_tz"))
    await callback.message.edit_text(f"🌍 Выберите часовой пояс\nТекущий: {config.get('timezone', 'Europe/Moscow')}", reply_markup=keyboard)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("tz_"))
async def save_timezone(callback: types.CallbackQuery, state: FSMContext):
    tz_name = callback.data.replace("tz_", "")
    tz_value = TIMEZONES.get(tz_name, 'Europe/Moscow')
    config['timezone'] = tz_value
    save_data()
    await callback.message.edit_text(f"✅ Часовой пояс: {tz_name}\n🕐 {get_current_time().strftime('%d.%m.%Y %H:%M:%S')}")
    await settings_menu_handler(callback.message, state)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "cancel_tz")
async def cancel_tz(callback: types.CallbackQuery, state: FSMContext):
    await settings_menu_handler(callback.message, state)
    await callback.answer()


@dp.callback_query_handler(lambda c: c.data == "info")
async def show_info(callback: types.CallbackQuery):
    ok, _ = await check_caldav_connection() if get_caldav_available() else (False, "")
    caldav_status = "✅ Доступен" if ok else "❌ Ошибка" if get_caldav_available() else "❌ Не настроен"
    info = f"""
📊 СТАТИСТИКА
Версия: v{BOT_VERSION}
Уведомлений: {len(notifications)}
Неотмеченных: {len(pending_notifications)}
Время проверки: {config.get('daily_check_time', '06:00')}
Часовой пояс: {config.get('timezone', 'Europe/Moscow')}
Текущее время: {get_current_time().strftime('%d.%m.%Y %H:%M:%S')}
Уведомления: {'Вкл' if notifications_enabled else 'Выкл'}
Синхр. с календарём: {'Вкл' if config.get('calendar_sync_enabled', True) else 'Выкл'}
CalDAV: {caldav_status}
"""
    await callback.message.edit_text(info)
    await callback.answer()


@dp.message_handler(commands=['version'])
async def show_version(message: types.Message):
    await delete_user_message(message)
    await send_with_auto_delete(message.chat.id, f"🤖 Бот v{BOT_VERSION} ({BOT_VERSION_DATE})", delay=3600)


@dp.message_handler(commands=['cancel'], state='*')
async def cancel_operation(message: types.Message, state: FSMContext):
    await delete_user_message(message)
    if await state.get_state() is None:
        await send_with_auto_delete(message.chat.id, "❌ Нет активных операций", delay=3600)
        return
    await state.finish()
    await send_with_auto_delete(message.chat.id, "✅ Операция отменена", delay=3600)


async def auto_update_calendar_cache():
    while True:
        try:
            now = get_current_time()
            await update_calendar_events_cache(now.year, now.month, force=True)
            await sync_calendar_to_pending()
            logger.info("Автообновление календаря выполнено")
        except Exception as e:
            logger.error(f"Ошибка автообновления: {e}")
        await asyncio.sleep(900)


async def on_startup(dp):
    init_folders()
    load_data()
    # перенумерация
    new_notifications = {}
    for i, (nid, notif) in enumerate(sorted(notifications.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 0), 1):
        notif['num'] = i
        new_notifications[str(i)] = notif
    notifications.clear()
    notifications.update(new_notifications)
    save_data()
    
    now = get_current_time()
    await update_calendar_events_cache(now.year, now.month, force=True)
    await sync_calendar_to_pending()
    
    logger.info(f"\n{'='*50}\n🤖 БОТ v{BOT_VERSION} ЗАПУЩЕН\n{'='*50}")
    if get_caldav_available():
        ok, msg = await check_caldav_connection()
        logger.info(f"CalDAV: {'✅' if ok else '❌'} {msg}")
    logger.info(f"Уведомлений: {len(notifications)}, неотмеченных: {len(pending_notifications)}")
    logger.info(f"Часовой пояс: {config.get('timezone', 'Europe/Moscow')}")
    logger.info(f"Текущее время: {get_current_time().strftime('%d.%m.%Y %H:%M:%S')}")
    logger.info(f"{'='*50}\n")
    
    asyncio.create_task(check_regular_notifications())
    asyncio.create_task(check_pending_notifications())
    asyncio.create_task(auto_update_calendar_cache())
    asyncio.create_task(sync_calendar_task())
    logger.info("✅ Бот готов")


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)