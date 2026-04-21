import asyncio
import json
import os
import logging
import logging.handlers
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pytz
import caldav
import hashlib
from uuid import uuid4

try:
    from dateutil.rrule import rrulestr
    DATEUTIL_AVAILABLE = True
except ImportError:
    DATEUTIL_AVAILABLE = False
    print("WARNING: python-dateutil не установлен. Установите: pip install python-dateutil")

# Настройка логирования с ротацией (максимум 200 КБ)
LOG_FILE = 'bot.log'
LOG_MAX_BYTES = 200 * 1024  # 200 KB
LOG_BACKUP_COUNT = 1

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

file_handler = logging.handlers.RotatingFileHandler(
    LOG_FILE, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT, encoding='utf-8'
)
file_handler.setFormatter(formatter)
root_logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
root_logger.addHandler(console_handler)

logger = logging.getLogger(__name__)

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils import executor
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN')
ADMIN_ID = int(os.getenv('ADMIN_ID')) if os.getenv('ADMIN_ID') else None
YANDEX_EMAIL = os.getenv('YANDEX_EMAIL')
YANDEX_APP_PASSWORD = os.getenv('YANDEX_APP_PASSWORD')
YANDEX_CALDAV_URL = "https://caldav.yandex.ru"

BOT_VERSION = "4.0"
BOT_VERSION_DATE = "21.04.2026"

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
dp.middleware.setup(LoggingMiddleware())

CONFIG_FILE = 'config.json'

# Глобальные переменные
config: Dict = {}
notifications_enabled = True
calendar_events_cache: Dict[str, List[Dict]] = {}
last_sync_time: Optional[datetime] = None

TIMEZONES = {
    'Москва (UTC+3)': 'Europe/Moscow',
    'Калининград (UTC+2)': 'Europe/Kaliningrad',
    'Екатеринбург (UTC+5)': 'Asia/Yekaterinburg',
    'Новосибирск (UTC+7)': 'Asia/Novosibirsk',
    'Владивосток (UTC+10)': 'Asia/Vladivostok',
    'Камчатка (UTC+12)': 'Asia/Kamchatka'
}

WEEKDAYS_BUTTONS = [("Пн",0),("Вт",1),("Ср",2),("Чт",3),("Пт",4),("Сб",5),("Вс",6)]
WEEKDAYS_NAMES = {0:"Понедельник",1:"Вторник",2:"Среда",3:"Четверг",4:"Пятница",5:"Суббота",6:"Воскресенье"}
MONTHS_NAMES = {1:"января",2:"февраля",3:"марта",4:"апреля",5:"мая",6:"июня",7:"июля",8:"августа",9:"сентября",10:"октября",11:"ноября",12:"декабря"}

def get_current_time():
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    return datetime.now(tz)

def parse_datetime(date_str: str):
    now = get_current_time()
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    patterns = [
        r'^(\d{1,2})\.(\d{1,2})\.(\d{2,4})\s+(\d{1,2}):(\d{2})$',
        r'^(\d{1,2})\.(\d{1,2})\s+(\d{1,2}):(\d{2})$',
        r'^(\d{1,2})\.(\d{1,2})\.(\d{2,4})$',
        r'^(\d{1,2})\.(\d{1,2})$'
    ]
    for pat in patterns:
        m = re.match(pat, date_str.strip())
        if m:
            groups = m.groups()
            if len(groups) == 5:
                d, mth, y, h, minute = groups
            elif len(groups) == 4:
                d, mth, h, minute = groups
                y = now.year
            elif len(groups) == 3:
                d, mth, y = groups
                h, minute = now.hour, now.minute
            else:
                d, mth = groups
                y = now.year
                h, minute = now.hour, now.minute
            y = int(y)
            if y < 100:
                y += 2000
            try:
                dt = tz.localize(datetime(y, int(mth), int(d), int(h), int(minute)))
                if dt < now and len(groups) in (4,2):
                    dt = tz.localize(datetime(y+1, int(mth), int(d), int(h), int(minute)))
                return dt
            except:
                return None
    return None

def expand_recurring_event(start_dt, rrule_str, until_date=None, max_count=150):
    if not DATEUTIL_AVAILABLE or not rrule_str:
        return [start_dt]
    
    try:
        tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
        
        if start_dt.tzinfo is None:
            start_dt = tz.localize(start_dt)
        
        start_dt_utc = start_dt.astimezone(pytz.UTC)
        start_dt_naive = start_dt_utc.replace(tzinfo=None)
        
        rrule_clean = rrule_str.strip()
        until_match = re.search(r'UNTIL=(\d{8}T\d{6}Z)', rrule_clean)
        if until_match:
            until_val = until_match.group(1).replace('Z', '')
            rrule_clean = re.sub(r'UNTIL=\d{8}T\d{6}Z', f'UNTIL={until_val}', rrule_clean)
        
        logger.info(f"Раскрытие RRULE: {rrule_clean}")
        
        rule = rrulestr(f"DTSTART:{start_dt_naive.strftime('%Y%m%dT%H%M%S')}\nRRULE:{rrule_clean}", dtstart=start_dt_naive)
        
        now_utc = get_current_time().astimezone(pytz.UTC)
        now_naive = now_utc.replace(tzinfo=None)
        
        if until_date:
            if until_date.tzinfo is None:
                until_date = tz.localize(until_date)
            until_utc = until_date.astimezone(pytz.UTC)
            end_date = until_utc.replace(tzinfo=None)
        else:
            end_date = now_naive + timedelta(days=120)
        
        occurrences_naive = list(rule.between(start_dt_naive, end_date, inc=True))
        
        if len(occurrences_naive) > max_count:
            occurrences_naive = occurrences_naive[:max_count]
        
        occurrences = []
        for occ_naive in occurrences_naive:
            occ_utc = pytz.UTC.localize(occ_naive)
            occ_local = occ_utc.astimezone(tz)
            occurrences.append(occ_local)
        
        logger.info(f"Раскрыто {len(occurrences)} вхождений")
        return occurrences if occurrences else [start_dt]
    except Exception as e:
        logger.error(f"expand_recurring_event error: {e}")
        return [start_dt]

class CalDAVCalendarAPI:
    def __init__(self, email, pwd):
        self.email = email
        self.pwd = pwd
        self.client = None
        self.principal = None

    def _connect(self):
        try:
            self.client = caldav.DAVClient(url=YANDEX_CALDAV_URL, username=self.email, password=self.pwd)
            self.principal = self.client.principal()
            return True
        except Exception as e:
            logger.error(f"Connect error: {e}")
            return False

    def get_default_calendar(self):
        if not self._connect():
            return None
        try:
            calendars = self.principal.calendars()
            return calendars[0] if calendars else None
        except Exception as e:
            logger.error(f"Get calendar error: {e}")
            return None

    async def test_connection(self):
        try:
            if not self._connect():
                return False, "Ошибка подключения"
            calendars = self.principal.calendars()
            if calendars:
                return True, f"Найдено {len(calendars)} календарей"
            return False, "Нет календарей"
        except Exception as e:
            return False, str(e)[:100]

    async def create_event(self, summary, start_time, description=""):
        try:
            cal = self.get_default_calendar()
            if not cal:
                return None
            end_time = start_time + timedelta(hours=1)
            tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
            if start_time.tzinfo is None:
                start_time = tz.localize(start_time)
            if end_time.tzinfo is None:
                end_time = tz.localize(end_time)
            tzid = config.get('timezone', 'Europe/Moscow')
            uid = f"{uuid4()}@myuved.bot"
            ical = f"""BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:{uid}
DTSTAMP:{datetime.now().strftime('%Y%m%dT%H%M%SZ')}
DTSTART;TZID={tzid}:{start_time.strftime('%Y%m%dT%H%M%S')}
DTEND;TZID={tzid}:{end_time.strftime('%Y%m%dT%H%M%S')}
SUMMARY:{summary[:255]}
DESCRIPTION:{description[:500]}
END:VEVENT
END:VCALENDAR"""
            event = cal.save_event(ical)
            return str(event.url) if event else None
        except Exception as e:
            logger.error(f"create_event error: {e}")
            return None

    async def delete_event(self, event_url):
        try:
            cal = self.get_default_calendar()
            if not cal:
                return False
            for event in cal.events():
                if str(event.url) == event_url:
                    event.delete()
                    return True
            return False
        except Exception as e:
            logger.error(f"delete_event error: {e}")
            return False

    async def add_exception_to_recurring(self, event_url, exception_date):
        """Добавляет EXDATE к повторяющемуся событию"""
        try:
            cal = self.get_default_calendar()
            if not cal:
                return False
            
            target_event = None
            for event in cal.events():
                if str(event.url) == event_url:
                    target_event = event
                    break
            
            if not target_event:
                return False
            
            ical_data = target_event.data
            
            # Преобразуем дату в UTC для EXDATE
            tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
            if exception_date.tzinfo is None:
                exception_date = tz.localize(exception_date)
            exception_utc = exception_date.astimezone(pytz.UTC)
            exdate_str = exception_utc.strftime('%Y%m%dT%H%M%SZ')
            
            # Проверяем и добавляем EXDATE
            if 'EXDATE' in ical_data:
                # Обновляем существующий EXDATE
                lines = ical_data.split('\n')
                new_lines = []
                for line in lines:
                    if line.startswith('EXDATE'):
                        if exdate_str not in line:
                            new_lines.append(line.rstrip('\r') + ',' + exdate_str)
                        else:
                            new_lines.append(line)
                    else:
                        new_lines.append(line)
                ical_data = '\n'.join(new_lines)
            else:
                # Добавляем новый EXDATE после RRULE или перед END:VEVENT
                lines = ical_data.split('\n')
                new_lines = []
                added = False
                for line in lines:
                    new_lines.append(line)
                    if not added and line.startswith('RRULE'):
                        new_lines.append(f'EXDATE:{exdate_str}')
                        added = True
                    elif not added and line.startswith('END:VEVENT'):
                        new_lines.insert(-1, f'EXDATE:{exdate_str}')
                        added = True
                ical_data = '\n'.join(new_lines)
            
            target_event.data = ical_data
            target_event.save()
            logger.info(f"Добавлено исключение для {event_url} на {exdate_str}")
            return True
        except Exception as e:
            logger.error(f"add_exception_to_recurring error: {e}")
            return False

    async def get_all_events_raw(self, from_date, to_date):
        try:
            cal = self.get_default_calendar()
            if not cal:
                return []
            tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
            if from_date.tzinfo is None:
                from_date = tz.localize(from_date)
            if to_date.tzinfo is None:
                to_date = tz.localize(to_date)
            from_utc = from_date.astimezone(pytz.UTC)
            to_utc = to_date.astimezone(pytz.UTC)
            events = cal.date_search(start=from_utc, end=to_utc, expand=False)
            result = []
            for ev in events:
                try:
                    vevent = ev.vobject_instance.vevent
                    dt = vevent.dtstart.value
                    if hasattr(dt, 'dt'):
                        dt = dt.dt
                    if dt.tzinfo is None:
                        dt = tz.localize(dt)
                    else:
                        dt = dt.astimezone(tz)
                    rrule = None
                    if hasattr(vevent, 'rrule') and vevent.rrule.value:
                        rrule = str(vevent.rrule.value)
                    result.append({
                        'id': str(ev.url),
                        'summary': str(vevent.summary.value) if hasattr(vevent, 'summary') else 'Без названия',
                        'start': dt.isoformat(),
                        'rrule': rrule,
                        'is_recurring': rrule is not None
                    })
                except Exception as e:
                    logger.error(f"parse event error: {e}")
                    continue
            return result
        except Exception as e:
            logger.error(f"get_all_events_raw error: {e}")
            return []

    async def get_expanded_events(self, from_date, to_date):
        raw_events = await self.get_all_events_raw(from_date, to_date)
        tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
        if from_date.tzinfo is None:
            from_date = tz.localize(from_date)
        if to_date.tzinfo is None:
            to_date = tz.localize(to_date)
        expanded = []
        for ev in raw_events:
            start_dt = datetime.fromisoformat(ev['start'])
            if start_dt.tzinfo is None:
                start_dt = tz.localize(start_dt)
            else:
                start_dt = start_dt.astimezone(tz)
            if ev.get('is_recurring') and ev.get('rrule') and DATEUTIL_AVAILABLE:
                end_date = get_current_time() + timedelta(days=120)
                occurrences = expand_recurring_event(start_dt, ev['rrule'], end_date, max_count=150)
                for occ_dt in occurrences:
                    if occ_dt >= from_date - timedelta(days=1):
                        expanded.append({
                            'id': ev['id'],
                            'summary': ev['summary'],
                            'start': occ_dt.isoformat(),
                            'is_recurring': True,
                            'rrule': ev['rrule']
                        })
            else:
                if start_dt >= from_date - timedelta(days=1):
                    expanded.append({
                        'id': ev['id'],
                        'summary': ev['summary'],
                        'start': ev['start'],
                        'is_recurring': False,
                        'rrule': None
                    })
        # Убираем дубликаты
        seen = set()
        unique_expanded = []
        for ev in expanded:
            key = f"{ev['id']}_{ev['start']}"
            if key not in seen:
                seen.add(key)
                unique_expanded.append(ev)
        unique_expanded.sort(key=lambda x: x['start'])
        return unique_expanded

    async def get_month_events(self, year, month):
        tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
        start = tz.localize(datetime(year, month, 1, 0, 0, 0))
        if month == 12:
            end = tz.localize(datetime(year+1, 1, 1, 0, 0, 0)) - timedelta(seconds=1)
        else:
            end = tz.localize(datetime(year, month+1, 1, 0, 0, 0)) - timedelta(seconds=1)
        start = start - timedelta(days=7)
        return await self.get_expanded_events(start, end)

def get_caldav_available():
    return bool(YANDEX_EMAIL and YANDEX_APP_PASSWORD)

async def check_caldav_connection():
    if not get_caldav_available():
        return False, "CalDAV не настроен"
    api = CalDAVCalendarAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
    return await api.test_connection()

async def update_calendar_cache(year, month, force=False):
    global calendar_events_cache, last_sync_time
    key = f"{year}_{month}"
    now = get_current_time()
    if not force and key in calendar_events_cache and last_sync_time and (now - last_sync_time).total_seconds() < 300:
        return
    if not get_caldav_available():
        return
    try:
        api = CalDAVCalendarAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
        events = await api.get_month_events(year, month)
        calendar_events_cache[key] = events
        last_sync_time = now
        logger.info(f"Обновлён кэш календаря {year}.{month}: {len(events)} событий")
    except Exception as e:
        logger.error(f"update_cache error: {e}")

async def get_pending_notifications():
    """Получает просроченные уведомления из календаря"""
    now = get_current_time()
    today = now.date()
    await update_calendar_cache(now.year, now.month)
    events = calendar_events_cache.get(f"{now.year}_{now.month}", [])
    
    pending = []
    for ev in events:
        try:
            dt = datetime.fromisoformat(ev['start'])
            tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
            if dt.tzinfo is None:
                dt = tz.localize(dt)
            else:
                dt = dt.astimezone(tz)
            
            # Показываем только просроченные (время уже прошло)
            if dt <= now:
                pending.append({
                    'id': ev['id'],
                    'text': ev['summary'],
                    'time': dt,
                    'is_recurring': ev.get('is_recurring', False),
                    'rrule': ev.get('rrule')
                })
        except Exception as e:
            logger.error(f"get_pending error: {e}")
            continue
    
    pending.sort(key=lambda x: x['time'])
    return pending

async def get_upcoming_events(year, month):
    """Получает предстоящие события для отображения в календаре"""
    await update_calendar_cache(year, month)
    events = calendar_events_cache.get(f"{year}_{month}", [])
    now = get_current_time()
    today = now.date()
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    
    future = []
    for ev in events:
        try:
            dt = datetime.fromisoformat(ev['start'])
            if dt.tzinfo is None:
                dt = tz.localize(dt)
            else:
                dt = dt.astimezone(tz)
            
            if dt.date() < today:
                continue
            
            # Для повторяющихся показываем только сегодня и завтра
            if ev.get('is_recurring', False):
                if dt.date() <= today + timedelta(days=1):
                    future.append((dt, ev))
            else:
                future.append((dt, ev))
        except Exception as e:
            logger.error(f"upcoming events error: {e}")
            continue
    
    future.sort(key=lambda x: x[0])
    return future

async def get_formatted_calendar_events(year, month, force_refresh=False):
    if force_refresh:
        await update_calendar_cache(year, month, force=True)
    
    future = await get_upcoming_events(year, month)
    
    if not future:
        return f"📅 **Нет предстоящих событий на {MONTHS_NAMES[month]} {year}**"
    
    now = get_current_time()
    today = now.date()
    text = f"📅 **СОБЫТИЯ КАЛЕНДАРЯ**\n📆 {MONTHS_NAMES[month]} {year}\n\n"
    
    for dt, ev in future[:100]:
        weekday = ["пн","вт","ср","чт","пт","сб","вс"][dt.weekday()]
        if dt.date() == today:
            prefix = "🔴 СЕГОДНЯ"
        elif dt.date() == today + timedelta(days=1):
            prefix = "🟠 ЗАВТРА"
        else:
            prefix = "📌"
        recurring_mark = " 🔁" if ev.get('is_recurring', False) else ""
        text += f"{prefix} {dt.day:02d}.{dt.month:02d}.{dt.year} ({weekday}) в {dt.strftime('%H:%M')} — **{ev['summary']}**{recurring_mark}\n"
    
    if last_sync_time:
        text += f"\n🔄 *Последняя синхронизация:* {last_sync_time.strftime('%d.%m.%Y %H:%M:%S')}"
    return text

async def show_calendar_events(chat_id, year=None, month=None, force_refresh=False, persistent=False):
    if year is None or month is None:
        now = get_current_time()
        year, month = now.year, now.month
    text = await get_formatted_calendar_events(year, month, force_refresh)
    kb = InlineKeyboardMarkup(row_width=3)
    pm, py = (month-1, year) if month > 1 else (12, year-1)
    nm, ny = (month+1, year) if month < 12 else (1, year+1)
    kb.add(
        InlineKeyboardButton("◀️", callback_data=f"cal_prev_{py}_{pm}"),
        InlineKeyboardButton(f"{MONTHS_NAMES[month]} {year}", callback_data="curr_month"),
        InlineKeyboardButton("▶️", callback_data=f"cal_next_{ny}_{nm}")
    )
    kb.add(
        InlineKeyboardButton("🔄 Обновить", callback_data=f"cal_refresh_{year}_{month}"),
        InlineKeyboardButton("📥 Синхр.", callback_data=f"cal_sync_{year}_{month}")
    )
    if persistent:
        await send_persistent_message(chat_id, text, reply_markup=kb)
    else:
        await send_with_auto_delete(chat_id, text, reply_markup=kb, delay=3600)

class NotificationStates(StatesGroup):
    waiting_for_text = State()
    waiting_for_specific_date = State()

class SettingsStates(StatesGroup):
    waiting_for_check_time = State()
    waiting_for_timezone = State()

def init_config():
    if not os.path.exists(CONFIG_FILE):
        default_config = {
            'daily_check_time': '06:00',
            'notifications_enabled': True,
            'timezone': 'Europe/Moscow',
            'calendar_sync_enabled': True
        }
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(default_config, f)

def load_config():
    global config, notifications_enabled
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        config = json.load(f)
        notifications_enabled = config.get('notifications_enabled', True)

def save_config():
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

async def auto_delete_message(chat_id, msg_id, delay=3600):
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id, msg_id)
    except:
        pass

async def send_with_auto_delete(chat_id, text, parse_mode='Markdown', reply_markup=None, delay=3600):
    msg = await bot.send_message(chat_id, text, parse_mode=parse_mode, reply_markup=reply_markup)
    asyncio.create_task(auto_delete_message(chat_id, msg.message_id, delay))
    return msg

async def send_persistent_message(chat_id, text, parse_mode='Markdown', reply_markup=None):
    return await bot.send_message(chat_id, text, parse_mode=parse_mode, reply_markup=reply_markup)

async def delete_user_message(msg, delay=3600):
    asyncio.create_task(auto_delete_message(msg.chat.id, msg.message_id, delay))

async def show_pending_actions(chat_id, event_id, text, event_time, is_recurring=False):
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Выполнено", callback_data=f"done_{event_id}"),
        InlineKeyboardButton("📅 Отложить", callback_data=f"snooze_{event_id}"),
        InlineKeyboardButton("❌ Отложить на час", callback_data=f"hour_{event_id}")
    )
    recurring_text = " (повторяющееся)" if is_recurring else ""
    await bot.send_message(
        chat_id,
        f"🔔 **НЕОТМЕЧЕННОЕ НАПОМИНАНИЕ!**{recurring_text}\n\n📝 {text}\n⏰ {event_time.strftime('%d.%m.%Y %H:%M')}\n\n⚠️ Время истекло!",
        reply_markup=kb,
        parse_mode='Markdown'
    )

async def check_pending():
    global notifications_enabled
    while True:
        if notifications_enabled:
            pending = await get_pending_notifications()
            for p in pending:
                if p['time'] <= get_current_time():
                    await show_pending_actions(ADMIN_ID, p['id'], p['text'], p['time'], p['is_recurring'])
                    # Ждем 5 минут перед следующим уведомлением о том же событии
                    await asyncio.sleep(300)
        await asyncio.sleep(60)

async def snooze_event(event_id, hours=1):
    """Откладывает событие на указанное количество часов"""
    try:
        api = CalDAVCalendarAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
        # Получаем событие
        cal = api.get_default_calendar()
        if not cal:
            return False
        
        for event in cal.events():
            if str(event.url) == event_id:
                # Парсим текущее время
                vevent = event.vobject_instance.vevent
                old_start = vevent.dtstart.value
                if hasattr(old_start, 'dt'):
                    old_start = old_start.dt
                
                tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
                if old_start.tzinfo is None:
                    old_start = tz.localize(old_start)
                
                # Вычисляем новое время
                new_start = old_start + timedelta(hours=hours)
                new_end = new_start + timedelta(hours=1)
                
                # Создаем новое событие
                tzid = config.get('timezone', 'Europe/Moscow')
                uid = f"{uuid4()}@myuved.bot"
                summary = str(vevent.summary.value) if hasattr(vevent, 'summary') else 'Без названия'
                
                ical = f"""BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:{uid}
DTSTAMP:{datetime.now().strftime('%Y%m%dT%H%M%SZ')}
DTSTART;TZID={tzid}:{new_start.strftime('%Y%m%dT%H%M%S')}
DTEND;TZID={tzid}:{new_end.strftime('%Y%m%dT%H%M%S')}
SUMMARY:{summary[:255]}
END:VEVENT
END:VCALENDAR"""
                
                # Удаляем старое и создаем новое
                event.delete()
                cal.save_event(ical)
                logger.info(f"Событие {event_id} отложено на {hours} час(ов)")
                return True
        return False
    except Exception as e:
        logger.error(f"snooze_event error: {e}")
        return False

async def mark_done(event_id, is_recurring=False, event_time=None):
    """Отмечает событие как выполненное"""
    try:
        api = CalDAVCalendarAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
        
        if is_recurring and event_time:
            # Для повторяющихся - добавляем исключение
            return await api.add_exception_to_recurring(event_id, event_time)
        else:
            # Для обычных - удаляем
            return await api.delete_event(event_id)
    except Exception as e:
        logger.error(f"mark_done error: {e}")
        return False

def get_main_keyboard():
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(
        KeyboardButton("➕ Добавить"),
        KeyboardButton("📅 Календарь"),
        KeyboardButton("⚠️ Просроченные"),
        KeyboardButton("⚙️ Настройки")
    )
    return kb

async def show_pending_list(chat_id, persistent=False):
    pending = await get_pending_notifications()
    
    if not pending:
        msg = "✅ **Нет просроченных уведомлений!**\n\nВсе напоминания выполнены."
        if persistent:
            await send_persistent_message(chat_id, msg)
        else:
            await send_with_auto_delete(chat_id, msg, delay=3600)
        return
    
    text = "⚠️ **ПРОСРОЧЕННЫЕ УВЕДОМЛЕНИЯ**\n\n"
    for idx, p in enumerate(pending, 1):
        recurring_mark = " 🔁" if p['is_recurring'] else ""
        text += f"{idx}. **{p['text']}**{recurring_mark}\n   ⏰ {p['time'].strftime('%d.%m.%Y %H:%M')}\n\n"
    text += f"📊 **Всего:** {len(pending)}"
    
    await send_persistent_message(chat_id, text)

# ---------- ОСНОВНЫЕ ОБРАБОТЧИКИ ----------
@dp.message_handler(commands=['start'])
async def cmd_start(msg, state):
    await delete_user_message(msg)
    await state.finish()
    if ADMIN_ID and msg.from_user.id != ADMIN_ID:
        return await msg.reply("❌ Нет доступа")
    
    ok, _ = await check_caldav_connection()
    welcome = f"""👋 **Добро пожаловать!**
🤖 Версия v{BOT_VERSION}
📧 CalDAV: {'✅ Доступен' if ok else '❌ Ошибка'}
🌍 Часовой пояс: {config.get('timezone', 'Europe/Moscow')}

📌 **Как это работает:**
• Все уведомления берутся ТОЛЬКО из Яндекс.Календаря
• При отметке "Выполнено" событие удаляется из календаря
• Для повторяющихся событий удаляется только текущее вхождение

📅 **В календаре показываются только предстоящие события**"""
    
    if not ok and get_caldav_available():
        welcome += "\n\n⚠️ Проблема с CalDAV. Проверьте пароль приложения."
    
    await send_persistent_message(msg.chat.id, welcome)
    await send_persistent_message(msg.chat.id, "👋 **Выберите действие:**", reply_markup=get_main_keyboard())
    
    now = get_current_time()
    await show_calendar_events(msg.chat.id, now.year, now.month, persistent=True)
    await show_pending_list(msg.chat.id, persistent=True)

@dp.message_handler(commands=['menu'])
async def show_menu(msg):
    await delete_user_message(msg)
    await send_persistent_message(msg.chat.id, "👋 **Главное меню:**", reply_markup=get_main_keyboard())
    now = get_current_time()
    await show_calendar_events(msg.chat.id, now.year, now.month, persistent=True)
    await show_pending_list(msg.chat.id, persistent=True)

@dp.message_handler(lambda m: m.text == "➕ Добавить", state='*')
async def add_start(msg, state):
    await delete_user_message(msg)
    await state.finish()
    await send_with_auto_delete(msg.chat.id, "✏️ **Введите текст уведомления:**\n\n💡 Для отмены /cancel", delay=3600)
    await NotificationStates.waiting_for_text.set()

@dp.message_handler(state=NotificationStates.waiting_for_text)
async def get_text(msg, state):
    await delete_user_message(msg)
    if not msg.text:
        return await send_with_auto_delete(msg.chat.id, "❌ Введите текст.", delay=3600)
    await state.update_data(text=msg.text)
    await send_with_auto_delete(msg.chat.id, "🗓️ **Введите дату и время**\n📝 Форматы:\n• `21.04 14:00`\n• `31.12.2025 23:59`", delay=3600)
    await NotificationStates.waiting_for_specific_date.set()

@dp.message_handler(state=NotificationStates.waiting_for_specific_date)
async def set_specific_date(msg, state):
    await delete_user_message(msg)
    dt = parse_datetime(msg.text)
    if dt is None or dt <= get_current_time():
        return await send_with_auto_delete(msg.chat.id, "❌ **Неверный формат или дата в прошлом!**", delay=3600)
    
    data = await state.get_data()
    api = CalDAVCalendarAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
    ev_id = await api.create_event(data['text'], dt, f"Создано через бота {get_current_time().strftime('%d.%m.%Y %H:%M')}")
    
    if ev_id:
        await send_with_auto_delete(msg.chat.id, f"✅ **Уведомление создано!**\n📝 {data['text']}\n⏰ {dt.strftime('%d.%m.%Y %H:%M')}", delay=3600)
        # Обновляем кэш
        now = get_current_time()
        await update_calendar_cache(now.year, now.month, force=True)
    else:
        await send_with_auto_delete(msg.chat.id, "❌ **Ошибка при создании уведомления!**", delay=3600)
    
    await state.finish()
    await show_calendar_events(msg.chat.id, persistent=True)
    await show_pending_list(msg.chat.id, persistent=True)

# ---------- ОБРАБОТЧИКИ ДЛЯ ПРОСРОЧЕННЫХ ----------
@dp.callback_query_handler(lambda c: c.data and c.data.startswith('done_'), state='*')
async def handle_done(cb):
    event_id = cb.data.replace('done_', '')
    
    # Определяем, повторяющееся ли событие и получаем время
    is_recurring = False
    event_time = None
    pending = await get_pending_notifications()
    for p in pending:
        if p['id'] == event_id:
            is_recurring = p['is_recurring']
            event_time = p['time']
            break
    
    success = await mark_done(event_id, is_recurring, event_time)
    
    if success:
        await cb.answer("✅ Событие отмечено как выполненное!")
        # Обновляем кэш
        now = get_current_time()
        await update_calendar_cache(now.year, now.month, force=True)
        await show_calendar_events(cb.from_user.id, now.year, now.month, force_refresh=True, persistent=True)
        await show_pending_list(cb.from_user.id, persistent=True)
    else:
        await cb.answer("❌ Ошибка при удалении события!")

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('snooze_'), state='*')
async def handle_snooze(cb):
    event_id = cb.data.replace('snooze_', '')
    await cb.message.edit_text("⏰ **На сколько отложить?**")
    
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("1 час", callback_data=f"snooze_1h_{event_id}"),
        InlineKeyboardButton("3 часа", callback_data=f"snooze_3h_{event_id}"),
        InlineKeyboardButton("1 день", callback_data=f"snooze_1d_{event_id}"),
        InlineKeyboardButton("7 дней", callback_data=f"snooze_7d_{event_id}")
    )
    await cb.message.edit_reply_markup(reply_markup=kb)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('snooze_1h_'), state='*')
async def snooze_1h(cb):
    event_id = cb.data.replace('snooze_1h_', '')
    await process_snooze(cb, event_id, 1)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('snooze_3h_'), state='*')
async def snooze_3h(cb):
    event_id = cb.data.replace('snooze_3h_', '')
    await process_snooze(cb, event_id, 3)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('snooze_1d_'), state='*')
async def snooze_1d(cb):
    event_id = cb.data.replace('snooze_1d_', '')
    await process_snooze(cb, event_id, 24)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('snooze_7d_'), state='*')
async def snooze_7d(cb):
    event_id = cb.data.replace('snooze_7d_', '')
    await process_snooze(cb, event_id, 168)

async def process_snooze(cb, event_id, hours):
    success = await snooze_event(event_id, hours)
    if success:
        await cb.answer(f"✅ Событие отложено на {hours} час(ов)!")
        now = get_current_time()
        await update_calendar_cache(now.year, now.month, force=True)
        await show_calendar_events(cb.from_user.id, now.year, now.month, force_refresh=True, persistent=True)
        await show_pending_list(cb.from_user.id, persistent=True)
    else:
        await cb.answer("❌ Ошибка при откладывании события!")

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('hour_'), state='*')
async def handle_hour(cb):
    event_id = cb.data.replace('hour_', '')
    await process_snooze(cb, event_id, 1)

@dp.message_handler(lambda m: m.text == "⚠️ Просроченные", state='*')
async def view_pending(msg, state):
    await delete_user_message(msg)
    await state.finish()
    await show_pending_list(msg.chat.id, persistent=True)

# ---------- ОБРАБОТЧИКИ КАЛЕНДАРЯ ----------
@dp.message_handler(lambda m: m.text == "📅 Календарь", state='*')
async def view_calendar(msg, state):
    await delete_user_message(msg)
    await state.finish()
    now = get_current_time()
    await show_calendar_events(msg.chat.id, now.year, now.month, persistent=True)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("cal_prev_"), state='*')
async def cal_prev(cb):
    parts = cb.data.replace("cal_prev_", "").split("_")
    await show_calendar_events(cb.from_user.id, int(parts[0]), int(parts[1]), persistent=True)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("cal_next_"), state='*')
async def cal_next(cb):
    parts = cb.data.replace("cal_next_", "").split("_")
    await show_calendar_events(cb.from_user.id, int(parts[0]), int(parts[1]), persistent=True)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("cal_refresh_"), state='*')
async def cal_refresh(cb):
    parts = cb.data.replace("cal_refresh_", "").split("_")
    await show_calendar_events(cb.from_user.id, int(parts[0]), int(parts[1]), force_refresh=True, persistent=True)
    await cb.answer("✅ Календарь обновлён")

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("cal_sync_"), state='*')
async def cal_sync(cb):
    parts = cb.data.replace("cal_sync_", "").split("_")
    y, m = int(parts[0]), int(parts[1])
    await cb.message.edit_text("🔄 **Синхронизация с календарём...**")
    await update_calendar_cache(y, m, force=True)
    await show_calendar_events(cb.from_user.id, y, m, force_refresh=True, persistent=True)
    await cb.answer("✅ Синхронизация завершена")

@dp.callback_query_handler(lambda c: c.data == "curr_month", state='*')
async def curr_month(cb):
    now = get_current_time()
    await show_calendar_events(cb.from_user.id, now.year, now.month, persistent=True)
    await cb.answer()

# ---------- НАСТРОЙКИ ----------
@dp.message_handler(lambda m: m.text == "⚙️ Настройки", state='*')
async def settings_menu(msg, state):
    await delete_user_message(msg)
    await state.finish()
    await settings_menu_handler(msg)

async def settings_menu_handler(msg):
    global notifications_enabled
    status = "🔔 Вкл" if notifications_enabled else "🔕 Выкл"
    if get_caldav_available():
        ok, _ = await check_caldav_connection()
        caldav_status = "✅ Доступен" if ok else "❌ Ошибка"
    else:
        caldav_status = "❌ Не настроен"
    
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton(f"Уведомления: {status}", callback_data="toggle_notify"),
        InlineKeyboardButton("🌍 Часовой пояс", callback_data="set_timezone"),
        InlineKeyboardButton("🔍 Проверить календарь", callback_data="check_cal"),
        InlineKeyboardButton("ℹ️ Информация", callback_data="info")
    )
    await send_with_auto_delete(msg.chat.id, f"⚙️ **НАСТРОЙКИ**\n\n📧 CalDAV: {caldav_status}\n🌍 Часовой пояс: {config.get('timezone', 'Europe/Moscow')}", reply_markup=kb, delay=3600)

@dp.callback_query_handler(lambda c: c.data == "check_cal", state='*')
async def check_cal(cb):
    if not get_caldav_available():
        return await cb.message.edit_text("❌ **CalDAV не настроен!**")
    await cb.message.edit_text("🔍 **Проверка подключения...**")
    ok, msg = await check_caldav_connection()
    if ok:
        await cb.message.edit_text(f"✅ **{msg}**")
        now = get_current_time()
        await update_calendar_cache(now.year, now.month, force=True)
    else:
        await cb.message.edit_text(f"❌ **{msg}**\n\n🔧 Получите новый пароль приложения: https://id.yandex.ru/security/app-passwords")
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data == "toggle_notify", state='*')
async def toggle_notify(cb, state):
    global notifications_enabled
    notifications_enabled = not notifications_enabled
    config['notifications_enabled'] = notifications_enabled
    save_config()
    await cb.message.edit_text(f"✅ **Уведомления {'включены' if notifications_enabled else 'выключены'}!**")
    await settings_menu_handler(cb.message)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data == "set_timezone", state='*')
async def set_timezone(cb):
    kb = InlineKeyboardMarkup(row_width=2)
    for name in TIMEZONES:
        kb.add(InlineKeyboardButton(name, callback_data=f"tz_{name}"))
    kb.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_tz"))
    await cb.message.edit_text(f"🌍 **Выберите часовой пояс**\n\nТекущий: {config.get('timezone', 'Europe/Moscow')}", reply_markup=kb)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("tz_"), state='*')
async def save_tz(cb, state):
    name = cb.data.replace("tz_", "")
    tz = TIMEZONES.get(name, 'Europe/Moscow')
    config['timezone'] = tz
    save_config()
    await cb.message.edit_text(f"✅ **Часовой пояс установлен:** {name}\n🕐 {get_current_time().strftime('%d.%m.%Y %H:%M:%S')}")
    await settings_menu_handler(cb.message)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data == "cancel_tz", state='*')
async def cancel_tz(cb, state):
    await settings_menu_handler(cb.message)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data == "info", state='*')
async def info(cb):
    ok, _ = await check_caldav_connection() if get_caldav_available() else (False, "")
    caldav_status = "✅ Доступен" if ok else "❌ Ошибка" if get_caldav_available() else "❌ Не настроен"
    info_text = f"""📊 **СТАТИСТИКА**

🤖 **Версия:** v{BOT_VERSION} ({BOT_VERSION_DATE})

🌍 **Часовой пояс:** `{config.get('timezone', 'Europe/Moscow')}`
🕐 **Текущее время:** `{get_current_time().strftime('%d.%m.%Y %H:%M:%S')}`
🔔 **Уведомления:** `{'Вкл' if notifications_enabled else 'Выкл'}`
📧 **CalDAV:** `{caldav_status}`

📌 Бот работает ТОЛЬКО с данными из Яндекс.Календаря"""
    await cb.message.edit_text(info_text)
    await cb.answer()

@dp.message_handler(commands=['version'])
async def show_version(msg):
    await delete_user_message(msg)
    await send_with_auto_delete(msg.chat.id, f"🤖 **Бот для уведомлений**\n📌 **Версия:** v{BOT_VERSION}\n📅 **Дата:** {BOT_VERSION_DATE}", delay=3600)

@dp.message_handler(commands=['cancel'], state='*')
async def cancel(msg, state):
    await delete_user_message(msg)
    if await state.get_state() is None:
        return await send_with_auto_delete(msg.chat.id, "❌ **Нет активных операций**", delay=3600)
    await state.finish()
    await send_with_auto_delete(msg.chat.id, "✅ **Операция отменена!**", delay=3600)

async def auto_update_cache():
    while True:
        try:
            now = get_current_time()
            await update_calendar_cache(now.year, now.month, force=True)
            logger.info("Автообновление календаря выполнено")
        except Exception as e:
            logger.error(f"auto_update error: {e}")
        await asyncio.sleep(900)

async def on_startup(dp):
    init_config()
    load_config()
    
    # Очищаем старые файлы с уведомлениями, если они есть
    old_data_file = 'notifications.json'
    if os.path.exists(old_data_file):
        os.remove(old_data_file)
        logger.info("Удалён старый файл notifications.json")
    
    logger.info(f"\n{'='*50}\n🤖 БОТ v{BOT_VERSION} ЗАПУЩЕН\n{'='*50}")
    if get_caldav_available():
        ok, msg = await check_caldav_connection()
        logger.info(f"CalDAV: {'✅' if ok else '❌'} {msg}")
    logger.info(f"Часовой пояс: {config.get('timezone', 'Europe/Moscow')}")
    logger.info(f"Текущее время: {get_current_time().strftime('%d.%m.%Y %H:%M:%S')}")
    logger.info(f"Логирование настроено: файл {LOG_FILE}, макс. размер {LOG_MAX_BYTES} байт")
    logger.info(f"{'='*50}\n")
    
    asyncio.create_task(check_pending())
    asyncio.create_task(auto_update_cache())
    logger.info("✅ Бот готов")

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)