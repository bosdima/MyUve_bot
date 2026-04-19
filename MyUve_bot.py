import asyncio
import json
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pytz
import re
import caldav
import hashlib
from uuid import uuid4

# Для работы с повторяющимися событиями
try:
    from dateutil.rrule import rrulestr
    from dateutil.parser import parse as parse_date
    DATEUTIL_AVAILABLE = True
except ImportError:
    DATEUTIL_AVAILABLE = False
    logging.warning("python-dateutil не установлен. Повторяющиеся события могут работать некорректно.")

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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BOT_VERSION = "2.0"
BOT_VERSION_DATE = "19.04.2026"

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
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

def get_next_weekday(target_weekdays, hour, minute, from_date=None):
    now = from_date or get_current_time()
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    if now.tzinfo is None:
        now = tz.localize(now)
    if now.weekday() in target_weekdays:
        today = tz.localize(datetime(now.year, now.month, now.day, hour, minute))
        if today > now:
            return today
    for i in range(1, 15):
        nxt = now + timedelta(days=i)
        if nxt.weekday() in target_weekdays:
            return tz.localize(datetime(nxt.year, nxt.month, nxt.day, hour, minute))
    return None

def expand_recurring_event(start_dt, rrule_str, until_date=None, max_count=90):
    """Разворачивает повторяющееся событие в список дат"""
    if not DATEUTIL_AVAILABLE or not rrule_str:
        return [start_dt]
    
    try:
        # Очищаем RRULE от лишних пробелов
        rrule_str = rrule_str.strip()
        
        # Парсим правило
        rule = rrulestr(f"DTSTART:{start_dt.strftime('%Y%m%dT%H%M%S')}\nRRULE:{rrule_str}", dtstart=start_dt)
        
        # Получаем вхождения
        if until_date:
            occurrences = list(rule.between(start_dt, until_date, inc=True))
        else:
            occurrences = list(rule[:max_count])
        
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
        self.calendar = None
    
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
    
    async def create_event(self, summary, start_time, description="", recurrence=None):
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
DESCRIPTION:{description[:500]}"""
            
            if recurrence:
                ical += f"\nRRULE:{recurrence}"
            
            ical += "\nEND:VEVENT\nEND:VCALENDAR"
            
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
    
    async def update_event(self, url, new_summary=None, new_start=None):
        try:
            if not await self.delete_event(url):
                return False
            if new_summary and new_start:
                return await self.create_event(new_summary, new_start, "Обновлено через бота") is not None
            return False
        except Exception as e:
            logger.error(f"update_event error: {e}")
            return False
    
    async def get_all_events_raw(self, from_date, to_date):
        """Получает сырые события (без раскрытия повторений)"""
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
            
            # Получаем события (без expand, чтобы получить RRULE)
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
                    
                    # Проверяем наличие RRULE
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
        """Получает события с раскрытием повторяющихся вхождений"""
        raw_events = await self.get_all_events_raw(from_date, to_date)
        tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
        
        # Корректируем часовой пояс
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
                # Раскрываем повторяющееся событие
                occurrences = expand_recurring_event(start_dt, ev['rrule'], to_date, max_count=90)
                for occ_dt in occurrences:
                    if from_date <= occ_dt <= to_date:
                        expanded.append({
                            'id': ev['id'],
                            'summary': ev['summary'],
                            'start': occ_dt.isoformat(),
                            'is_recurring': True,
                            'rrule': ev['rrule'],
                            'original_start': ev['start']
                        })
            else:
                # Обычное событие
                if from_date <= start_dt <= to_date:
                    expanded.append({
                        'id': ev['id'],
                        'summary': ev['summary'],
                        'start': ev['start'],
                        'is_recurring': False,
                        'rrule': None
                    })
        
        return expanded
    
    async def get_month_events(self, year, month):
        tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
        start = tz.localize(datetime(year, month, 1, 0, 0, 0))
        if month == 12:
            end = tz.localize(datetime(year+1, 1, 1, 0, 0, 0)) - timedelta(seconds=1)
        else:
            end = tz.localize(datetime(year, month+1, 1, 0, 0, 0)) - timedelta(seconds=1)
        return await self.get_expanded_events(start, end)

def get_caldav_available():
    return bool(YANDEX_EMAIL and YANDEX_APP_PASSWORD)

async def check_caldav_connection():
    if not get_caldav_available():
        return False, "CalDAV не настроен"
    api = CalDAVCalendarAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
    return await api.test_connection()

async def update_calendar_events_cache(year, month, force=False):
    global calendar_events_cache, last_calendar_update, last_sync_time
    key = f"{year}_{month}"
    now = get_current_time()
    if not force and key in last_calendar_update and (now - last_calendar_update[key]).total_seconds() < 300:
        return
    if not get_caldav_available():
        return
    try:
        api = CalDAVCalendarAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
        events = await api.get_month_events(year, month)
        events.sort(key=lambda x: x['start'])
        calendar_events_cache[key] = events
        last_calendar_update[key] = now
        last_sync_time = now
        logger.info(f"Обновлён кэш календаря {year}.{month}: {len(events)} событий")
        # Логируем для отладки
        for ev in events[:10]:
            recurring = " 🔁" if ev.get('is_recurring') else ""
            logger.info(f"  - {ev['start']}: {ev['summary']}{recurring}")
    except Exception as e:
        logger.error(f"update_cache error: {e}")

async def get_formatted_calendar_events(year, month, force_refresh=False):
    if force_refresh:
        await update_calendar_events_cache(year, month, force=True)
    else:
        await update_calendar_events_cache(year, month)
    events = calendar_events_cache.get(f"{year}_{month}", [])
    now = get_current_time()
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    future = []
    for ev in events:
        try:
            dt = datetime.fromisoformat(ev['start'])
            if dt.tzinfo is None:
                dt = tz.localize(dt)
            else:
                dt = dt.astimezone(tz)
            if dt >= now - timedelta(hours=1):
                future.append((dt, ev))
        except Exception as e:
            logger.error(f"format event error: {e}")
            continue
    if not future:
        return f"📅 **Нет событий на {MONTHS_NAMES[month]} {year}**"
    future.sort(key=lambda x: x[0])
    text = f"📅 **СОБЫТИЯ КАЛЕНДАРЯ**\n📆 {MONTHS_NAMES[month]} {year}\n\n"
    for dt, ev in future[:50]:
        weekday = ["пн","вт","ср","чт","пт","сб","вс"][dt.weekday()]
        if dt.date() == now.date():
            prefix = "🔴 СЕГОДНЯ"
        elif dt.date() == now.date() + timedelta(days=1):
            prefix = "🟠 ЗАВТРА"
        elif dt.date() == now.date() + timedelta(days=2):
            prefix = "🟡 ПОСЛЕЗАВТРА"
        else:
            prefix = "📌"
        recurring_mark = " 🔁" if ev.get('is_recurring', False) else ""
        text += f"{prefix} {dt.day:02d}.{dt.month:02d}.{dt.year} ({weekday}) в {dt.strftime('%H:%M')} — **{ev['summary']}**{recurring_mark}\n"
    if len(future) > 50:
        text += f"\n... и еще {len(future)-50} событий"
    if last_sync_time:
        text += f"\n\n🔄 *Последняя синхронизация:* {last_sync_time.strftime('%d.%m.%Y %H:%M:%S')}"
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
        InlineKeyboardButton("📥 Синхр.", callback_data=f"cal_sync_{year}_{month}"),
        InlineKeyboardButton("✏️ Ред.", callback_data="edit_calendar")
    )
    if persistent:
        await send_persistent_message(chat_id, text, reply_markup=kb)
    else:
        await send_with_auto_delete(chat_id, text, reply_markup=kb, delay=3600)

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
    waiting_for_edit_time = State()

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
            'calendar_sync_enabled': True
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
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        config = json.load(f)
        notifications_enabled = config.get('notifications_enabled', True)
    logger.info(f"Загружено: {len(notifications)} уведомлений, {len(pending_notifications)} неотмеченных")

def save_data():
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump({
            'notifications': notifications,
            'pending_notifications': pending_notifications
        }, f, indent=2, ensure_ascii=False)
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

async def sync_calendar_to_pending():
    """Синхронизация - добавляем ВСЕ просроченные события (включая повторяющиеся)"""
    if not get_caldav_available() or not config.get('calendar_sync_enabled', True):
        return
    now = get_current_time()
    await update_calendar_events_cache(now.year, now.month)
    events = calendar_events_cache.get(f"{now.year}_{now.month}", [])
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    
    added_count = 0
    for ev in events:
        try:
            dt = datetime.fromisoformat(ev['start'])
            if dt.tzinfo is None:
                dt = tz.localize(dt)
            else:
                dt = dt.astimezone(tz)
            
            # Добавляем ТОЛЬКО просроченные события (время наступило)
            if dt <= now:
                # Уникальный ключ: ID + дата (для повторяющихся) или просто ID
                event_key = f"{ev['id']}_{dt.strftime('%Y%m%d')}" if ev.get('is_recurring') else ev['id']
                exists = False
                for n in pending_notifications.values():
                    if n.get('calendar_event_key') == event_key:
                        exists = True
                        break
                if not exists:
                    nid = f"pending_{int(dt.timestamp())}_{hashlib.md5(event_key.encode()).hexdigest()[:8]}"
                    pending_notifications[nid] = {
                        'text': ev['summary'],
                        'time': dt.isoformat(),
                        'created': get_current_time().isoformat(),
                        'calendar_event_id': ev['id'],
                        'calendar_event_key': event_key,
                        'is_completed': False,
                        'reminder_sent': False,
                        'repeat_count': 0,
                        'last_reminder_time': None,
                        'is_recurring': ev.get('is_recurring', False),
                        'event_date': dt.strftime('%Y%m%d')
                    }
                    added_count += 1
                    logger.info(f"Добавлено просроченное событие: {ev['summary']} на {dt.strftime('%d.%m.%Y %H:%M')} (повтор: {ev.get('is_recurring', False)})")
        except Exception as e:
            logger.error(f"sync_calendar error for {ev.get('summary', 'unknown')}: {e}")
    
    if added_count > 0:
        save_data()
        logger.info(f"Синхронизация: добавлено {added_count} просроченных событий")

async def sync_notification_to_calendar(notif_id, action='create'):
    if not config.get('calendar_sync_enabled', True) or not get_caldav_available():
        return
    notif = notifications.get(notif_id) or pending_notifications.get(notif_id)
    if not notif:
        return
    api = CalDAVCalendarAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
    try:
        if action == 'create':
            t = datetime.fromisoformat(notif['time'])
            if t.tzinfo is None:
                t = pytz.timezone(config.get('timezone', 'Europe/Moscow')).localize(t)
            desc = f"Уведомление из бота\nТекст: {notif['text']}\nСоздано: {get_current_time().strftime('%d.%m.%Y %H:%M')}"
            ev_id = await api.create_event(notif['text'][:100], t, desc)
            if ev_id:
                notif['calendar_event_id'] = ev_id
                save_data()
                now = get_current_time()
                await update_calendar_events_cache(now.year, now.month, force=True)
        elif action == 'delete':
            if 'calendar_event_id' in notif:
                await api.delete_event(notif['calendar_event_id'])
                del notif['calendar_event_id']
                save_data()
                now = get_current_time()
                await update_calendar_events_cache(now.year, now.month, force=True)
    except Exception as e:
        logger.error(f"sync_notif error: {e}")

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

async def show_pending_notification_actions(chat_id, nid, text, repeat=0):
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Выполнено", callback_data=f"pend_done_{nid}"),
        InlineKeyboardButton("✏️ Изменить время", callback_data=f"pend_edit_{nid}"),
        InlineKeyboardButton("📅 Отложить", callback_data=f"pend_snooze_{nid}"),
        InlineKeyboardButton("❌ Отложить на час", callback_data=f"pend_hour_{nid}")
    )
    repeat_text = f" (повтор #{repeat})" if repeat > 0 else ""
    await bot.send_message(
        chat_id,
        f"🔔 **НЕОТМЕЧЕННОЕ НАПОМИНАНИЕ!**{repeat_text}\n\n📝 {text}\n\n⏰ Время истекло!\n⚠️ Будет повторяться каждый час.",
        reply_markup=kb,
        parse_mode='Markdown'
    )

async def check_pending_notifications():
    global notifications_enabled
    while True:
        if notifications_enabled:
            now = get_current_time()
            tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
            for nid, n in list(pending_notifications.items()):
                if n.get('is_completed', False):
                    continue
                nt = datetime.fromisoformat(n['time'])
                if nt.tzinfo is None:
                    nt = tz.localize(nt)
                else:
                    nt = nt.astimezone(tz)
                last = n.get('last_reminder_time')
                last_t = datetime.fromisoformat(last) if last else None
                if not n.get('reminder_sent', False) and now >= nt:
                    await show_pending_notification_actions(ADMIN_ID, nid, n['text'])
                    n['reminder_sent'] = True
                    n['last_reminder_time'] = now.isoformat()
                    n['repeat_count'] = 1
                    save_data()
                    logger.info(f"Отправлено первое уведомление для {nid}")
                elif n.get('reminder_sent', False) and not n.get('is_completed', False):
                    if last_t is None:
                        last_t = nt
                    if (now - last_t).total_seconds() >= 3600:
                        cnt = n.get('repeat_count', 0) + 1
                        await show_pending_notification_actions(ADMIN_ID, nid, n['text'], cnt)
                        n['last_reminder_time'] = now.isoformat()
                        n['repeat_count'] = cnt
                        save_data()
                        logger.info(f"Отправлено повторное уведомление #{cnt} для {nid}")
        await asyncio.sleep(30)

async def check_regular_notifications():
    global notifications_enabled
    while True:
        if notifications_enabled:
            now = get_current_time()
            tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
            for nid, n in list(notifications.items()):
                if n.get('is_completed', False):
                    continue
                rt = n.get('repeat_type', 'no')
                if rt == 'every_hour':
                    lt = n.get('last_trigger')
                    if lt:
                        lt_t = datetime.fromisoformat(lt)
                        lt_t = lt_t if lt_t.tzinfo else tz.localize(lt_t)
                    else:
                        lt_t = now - timedelta(hours=1)
                    if (now - lt_t).total_seconds() >= 3600:
                        await show_pending_notification_actions(ADMIN_ID, nid, n['text'])
                        n['last_trigger'] = now.isoformat()
                        n['repeat_count'] = n.get('repeat_count', 0) + 1
                        pending_notifications[nid] = n.copy()
                        pending_notifications[nid]['reminder_sent'] = True
                        pending_notifications[nid]['last_reminder_time'] = now.isoformat()
                        del notifications[nid]
                        save_data()
                        logger.info(f"Уведомление {nid} перенесено в неотмеченные (каждый час)")
                elif rt == 'every_day':
                    h, m = n.get('repeat_hour', 0), n.get('repeat_minute', 0)
                    today = tz.localize(datetime(now.year, now.month, now.day, h, m, 0))
                    lt = n.get('last_trigger')
                    if lt:
                        lt_t = datetime.fromisoformat(lt)
                        lt_t = lt_t if lt_t.tzinfo else tz.localize(lt_t)
                    else:
                        lt_t = None
                    if (lt_t is None and now >= today) or (lt_t and lt_t.date() < now.date() and now >= today):
                        await show_pending_notification_actions(ADMIN_ID, nid, n['text'])
                        n['last_trigger'] = now.isoformat()
                        n['repeat_count'] = n.get('repeat_count', 0) + 1
                        pending_notifications[nid] = n.copy()
                        pending_notifications[nid]['reminder_sent'] = True
                        pending_notifications[nid]['last_reminder_time'] = now.isoformat()
                        del notifications[nid]
                        save_data()
                        logger.info(f"Уведомление {nid} перенесено в неотмеченные (ежедневно)")
                elif rt == 'weekdays':
                    h, m = n.get('repeat_hour', 0), n.get('repeat_minute', 0)
                    wl = n.get('weekdays_list', [])
                    if now.weekday() in wl:
                        today = tz.localize(datetime(now.year, now.month, now.day, h, m, 0))
                        lt = n.get('last_trigger')
                        if lt:
                            lt_t = datetime.fromisoformat(lt)
                            lt_t = lt_t if lt_t.tzinfo else tz.localize(lt_t)
                        else:
                            lt_t = None
                        if now >= today and not (lt_t and lt_t.date() == now.date()):
                            await show_pending_notification_actions(ADMIN_ID, nid, n['text'])
                            n['last_trigger'] = now.isoformat()
                            n['repeat_count'] = n.get('repeat_count', 0) + 1
                            pending_notifications[nid] = n.copy()
                            pending_notifications[nid]['reminder_sent'] = True
                            pending_notifications[nid]['last_reminder_time'] = now.isoformat()
                            del notifications[nid]
                            save_data()
                            logger.info(f"Уведомление {nid} перенесено в неотмеченные (по дням недели)")
                elif rt == 'no' and n.get('time') and not n.get('reminder_sent', False):
                    nt = datetime.fromisoformat(n['time'])
                    if nt.tzinfo is None:
                        nt = tz.localize(nt)
                    else:
                        nt = nt.astimezone(tz)
                    if now >= nt:
                        await show_pending_notification_actions(ADMIN_ID, nid, n['text'])
                        n['reminder_sent'] = True
                        n['last_reminder_time'] = now.isoformat()
                        n['repeat_count'] = 1
                        pending_notifications[nid] = n.copy()
                        pending_notifications[nid]['reminder_sent'] = True
                        pending_notifications[nid]['last_reminder_time'] = now.isoformat()
                        del notifications[nid]
                        save_data()
                        logger.info(f"Уведомление {nid} перенесено в неотмеченные (одноразовое)")
        await asyncio.sleep(30)

async def sync_calendar_task():
    while True:
        try:
            await sync_calendar_to_pending()
            logger.info("Синхронизация календаря выполнена")
        except Exception as e:
            logger.error(f"sync_task error: {e}")
        await asyncio.sleep(300)

def get_main_keyboard():
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(
        KeyboardButton("➕ Добавить"),
        KeyboardButton("📅 Календарь"),
        KeyboardButton("⚠️ Неотмеченные"),
        KeyboardButton("⚙️ Настройки")
    )
    return kb

async def update_pending_list(chat_id, persistent=False):
    """Показываем список неотмеченных уведомлений с возможностью выбора"""
    if not pending_notifications:
        msg = "✅ **Нет неотмеченных уведомлений!**\n\nВсе напоминания выполнены."
        if persistent:
            await send_persistent_message(chat_id, msg)
        else:
            await send_with_auto_delete(chat_id, msg, delay=3600)
        return
    
    now = get_current_time()
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    pending_list = []
    
    for nid, n in pending_notifications.items():
        if n.get('is_completed', False):
            continue
        dt = datetime.fromisoformat(n['time'])
        if dt.tzinfo is None:
            dt = tz.localize(dt)
        else:
            dt = dt.astimezone(tz)
        cnt = n.get('repeat_count', 0)
        recurring_mark = " 🔁" if n.get('is_recurring', False) else ""
        pending_list.append((nid, n['text'] + recurring_mark, dt, cnt, n.get('is_recurring', False)))
    
    if not pending_list:
        msg = "✅ **Нет неотмеченных уведомлений!**"
        if persistent:
            await send_persistent_message(chat_id, msg)
        else:
            await send_with_auto_delete(chat_id, msg, delay=3600)
        return
    
    # Сортируем по времени
    pending_list.sort(key=lambda x: x[2])
    
    text = "⚠️ **НЕОТМЕЧЕННЫЕ УВЕДОМЛЕНИЯ**\n\n"
    text += "Эти напоминания просрочены и будут повторяться каждый час.\n"
    text += "Выберите уведомление для отметки выполнения или редактирования.\n\n"
    
    for idx, (nid, txt, dt, cnt, is_rec) in enumerate(pending_list, 1):
        repeat_text = f" (повторений: {cnt})" if cnt > 0 else ""
        text += f"{idx}. **{txt}**\n   ⏰ {dt.strftime('%d.%m.%Y %H:%M')}{repeat_text}\n\n"
    
    text += f"📊 **Всего:** {len(pending_list)}"
    
    # Клавиатура с кнопками для каждого уведомления
    kb = InlineKeyboardMarkup(row_width=2)
    for idx, (nid, txt, dt, cnt, is_rec) in enumerate(pending_list, 1):
        kb.add(InlineKeyboardButton(f"{idx}. {txt[:30]}...", callback_data=f"pend_select_{nid}"))
    kb.add(InlineKeyboardButton("🔄 Обновить", callback_data="refresh_pending"))
    
    if persistent:
        await send_persistent_message(chat_id, text, reply_markup=kb)
    else:
        await send_with_auto_delete(chat_id, text, reply_markup=kb, delay=3600)

# ---------- ОСНОВНЫЕ ОБРАБОТЧИКИ ----------
@dp.message_handler(commands=['start'])
async def cmd_start(msg, state):
    await delete_user_message(msg)
    await state.finish()
    if ADMIN_ID and msg.from_user.id != ADMIN_ID:
        return await msg.reply("❌ Нет доступа")
    ok, _ = await check_caldav_connection()
    welcome = f"👋 **Добро пожаловать!**\n🤖 Версия v{BOT_VERSION}\n📧 CalDAV: {'✅ Доступен' if ok else '❌ Ошибка'}\n🌍 Часовой пояс: {config.get('timezone', 'Europe/Moscow')}\n\n⚠️ **Неотмеченные уведомления** — это напоминания, время которых уже истекло. Они будут повторяться каждый час, пока вы не отметите их как выполненные.\n\n🔄 **Для повторяющихся событий:** при отметке \"Выполнено\" удаляется только сегодняшнее вхождение, следующее остаётся в календаре."
    if not ok and get_caldav_available():
        welcome += "\n\n⚠️ Проблема с CalDAV. Проверьте пароль приложения."
    if not DATEUTIL_AVAILABLE:
        welcome += "\n\n⚠️ **Внимание:** Библиотека python-dateutil не установлена. Повторяющиеся события могут отображаться некорректно. Установите: pip install python-dateutil"
    await send_persistent_message(msg.chat.id, welcome)
    await send_persistent_message(msg.chat.id, "👋 **Выберите действие:**", reply_markup=get_main_keyboard())
    await update_pending_list(msg.chat.id, persistent=True)
    now = get_current_time()
    await show_calendar_events(msg.chat.id, now.year, now.month, persistent=True)

@dp.message_handler(commands=['menu'])
async def show_menu(msg):
    await delete_user_message(msg)
    await send_persistent_message(msg.chat.id, "👋 **Главное меню:**", reply_markup=get_main_keyboard())
    await update_pending_list(msg.chat.id, persistent=True)
    now = get_current_time()
    await show_calendar_events(msg.chat.id, now.year, now.month, persistent=True)

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
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("⏰ В часах", callback_data="time_hours"),
        InlineKeyboardButton("📅 В днях", callback_data="time_days"),
        InlineKeyboardButton("📆 В месяцах", callback_data="time_months"),
        InlineKeyboardButton("🗓️ Конкретная дата/время", callback_data="time_specific"),
        InlineKeyboardButton("🕐 Каждый час", callback_data="time_every_hour"),
        InlineKeyboardButton("📅 Каждый день", callback_data="time_every_day"),
        InlineKeyboardButton("📆 По дням недели", callback_data="time_weekdays")
    )
    await send_with_auto_delete(msg.chat.id, "⏱️ **Когда уведомить?**", reply_markup=kb, delay=3600)
    await NotificationStates.waiting_for_time_type.set()

@dp.callback_query_handler(lambda c: c.data in ("time_hours","time_days","time_months","time_specific","time_every_hour","time_every_day","time_weekdays"), state=NotificationStates.waiting_for_time_type)
async def time_type(cb, state):
    data = cb.data
    if data == "time_specific":
        await send_with_auto_delete(cb.from_user.id, "🗓️ **Введите дату и время**\n📝 Форматы:\n• `17.04 21:00`\n• `31.12.2025 23:59`", delay=3600)
        await NotificationStates.waiting_for_specific_date.set()
    elif data == "time_hours":
        await send_with_auto_delete(cb.from_user.id, "⌛ **Введите количество часов**\n📝 Например: `5`", delay=3600)
        await NotificationStates.waiting_for_hours.set()
    elif data == "time_days":
        await send_with_auto_delete(cb.from_user.id, "📅 **Введите количество дней**\n📝 Например: `7`", delay=3600)
        await NotificationStates.waiting_for_days.set()
    elif data == "time_months":
        await send_with_auto_delete(cb.from_user.id, "📆 **Введите количество месяцев**\n📝 Например: `1`", delay=3600)
        await NotificationStates.waiting_for_months.set()
    elif data == "time_every_hour":
        d = await state.get_data()
        nxt = len(notifications) + 1
        notif = {
            'text': d['text'], 'time': get_current_time().isoformat(), 'created': get_current_time().isoformat(),
            'is_completed': False, 'num': nxt, 'repeat_type': 'every_hour',
            'last_trigger': get_current_time().isoformat(), 'repeat_count': 0, 'reminder_sent': False
        }
        notifications[str(nxt)] = notif
        save_data()
        await sync_notification_to_calendar(str(nxt), 'create')
        await send_with_auto_delete(cb.from_user.id, f"✅ **Уведомление #{nxt} создано!**\n🕐 Каждый час", delay=3600)
        await state.finish()
        await update_pending_list(cb.from_user.id, persistent=True)
    elif data == "time_every_day":
        await send_with_auto_delete(cb.from_user.id, "⏰ **Введите время (ЧЧ:ММ)**\n📝 Например: `09:00`", delay=3600)
        await NotificationStates.waiting_for_every_day_time.set()
    elif data == "time_weekdays":
        kb = InlineKeyboardMarkup(row_width=3)
        for name, day in WEEKDAYS_BUTTONS:
            kb.add(InlineKeyboardButton(name, callback_data=f"wd_{day}"))
        kb.add(InlineKeyboardButton("✅ Готово", callback_data="wd_done"))
        await send_with_auto_delete(cb.from_user.id, "📅 **Выберите дни недели**", reply_markup=kb, delay=3600)
        await state.update_data(selected_weekdays=[])
        await NotificationStates.waiting_for_weekdays.set()
    await cb.answer()

@dp.message_handler(state=NotificationStates.waiting_for_specific_date)
async def set_specific_date(msg, state):
    await delete_user_message(msg)
    dt = parse_datetime(msg.text)
    if dt is None or dt <= get_current_time():
        return await send_with_auto_delete(msg.chat.id, "❌ **Неверный формат или дата в прошлом!**", delay=3600)
    d = await state.get_data()
    if 'text' not in d:
        return await send_with_auto_delete(msg.chat.id, "❌ **Ошибка: текст уведомления не найден. Начните заново.**", delay=3600)
    nxt = len(notifications) + 1
    notifications[str(nxt)] = {
        'text': d['text'], 'time': dt.isoformat(), 'created': get_current_time().isoformat(),
        'is_completed': False, 'num': nxt, 'repeat_type': 'no',
        'repeat_count': 0, 'reminder_sent': False
    }
    save_data()
    await sync_notification_to_calendar(str(nxt), 'create')
    await send_with_auto_delete(msg.chat.id, f"✅ **Уведомление #{nxt} создано!**\n⏰ {dt.strftime('%d.%m.%Y %H:%M')}", delay=3600)
    await state.finish()
    await update_pending_list(msg.chat.id, persistent=True)

@dp.message_handler(state=NotificationStates.waiting_for_hours)
async def set_hours(msg, state):
    await delete_user_message(msg)
    try:
        h = int(msg.text)
        if h <= 0:
            raise ValueError
        d = await state.get_data()
        if 'text' not in d:
            return await send_with_auto_delete(msg.chat.id, "❌ Ошибка. Начните заново.", delay=3600)
        dt = get_current_time() + timedelta(hours=h)
        nxt = len(notifications) + 1
        notifications[str(nxt)] = {
            'text': d['text'], 'time': dt.isoformat(), 'created': get_current_time().isoformat(),
            'is_completed': False, 'num': nxt, 'repeat_type': 'no',
            'repeat_count': 0, 'reminder_sent': False
        }
        save_data()
        await sync_notification_to_calendar(str(nxt), 'create')
        await send_with_auto_delete(msg.chat.id, f"✅ **Уведомление #{nxt} создано!**\n⏰ {dt.strftime('%d.%m.%Y %H:%M')}", delay=3600)
        await state.finish()
        await update_pending_list(msg.chat.id, persistent=True)
    except:
        await send_with_auto_delete(msg.chat.id, "❌ **Введите положительное число!**", delay=3600)

@dp.message_handler(state=NotificationStates.waiting_for_days)
async def set_days(msg, state):
    await delete_user_message(msg)
    try:
        d = int(msg.text)
        if d <= 0:
            raise ValueError
        data = await state.get_data()
        if 'text' not in data:
            return await send_with_auto_delete(msg.chat.id, "❌ Ошибка. Начните заново.", delay=3600)
        dt = get_current_time() + timedelta(days=d)
        nxt = len(notifications) + 1
        notifications[str(nxt)] = {
            'text': data['text'], 'time': dt.isoformat(), 'created': get_current_time().isoformat(),
            'is_completed': False, 'num': nxt, 'repeat_type': 'no',
            'repeat_count': 0, 'reminder_sent': False
        }
        save_data()
        await sync_notification_to_calendar(str(nxt), 'create')
        await send_with_auto_delete(msg.chat.id, f"✅ **Уведомление #{nxt} создано!**\n⏰ {dt.strftime('%d.%m.%Y %H:%M')}", delay=3600)
        await state.finish()
        await update_pending_list(msg.chat.id, persistent=True)
    except:
        await send_with_auto_delete(msg.chat.id, "❌ **Введите положительное число!**", delay=3600)

@dp.message_handler(state=NotificationStates.waiting_for_months)
async def set_months(msg, state):
    await delete_user_message(msg)
    try:
        m = int(msg.text)
        if m <= 0:
            raise ValueError
        data = await state.get_data()
        if 'text' not in data:
            return await send_with_auto_delete(msg.chat.id, "❌ Ошибка. Начните заново.", delay=3600)
        dt = get_current_time() + timedelta(days=m*30)
        nxt = len(notifications) + 1
        notifications[str(nxt)] = {
            'text': data['text'], 'time': dt.isoformat(), 'created': get_current_time().isoformat(),
            'is_completed': False, 'num': nxt, 'repeat_type': 'no',
            'repeat_count': 0, 'reminder_sent': False
        }
        save_data()
        await sync_notification_to_calendar(str(nxt), 'create')
        await send_with_auto_delete(msg.chat.id, f"✅ **Уведомление #{nxt} создано!**\n⏰ {dt.strftime('%d.%m.%Y %H:%M')}", delay=3600)
        await state.finish()
        await update_pending_list(msg.chat.id, persistent=True)
    except:
        await send_with_auto_delete(msg.chat.id, "❌ **Введите положительное число!**", delay=3600)

@dp.message_handler(state=NotificationStates.waiting_for_every_day_time)
async def set_every_day_time(msg, state):
    await delete_user_message(msg)
    m = re.match(r'^(\d{1,2}):(\d{2})$', msg.text.strip())
    if not m:
        return await send_with_auto_delete(msg.chat.id, "❌ **Формат ЧЧ:ММ**", delay=3600)
    h, mn = map(int, m.groups())
    if h > 23 or mn > 59:
        return await send_with_auto_delete(msg.chat.id, "❌ **Некорректное время**", delay=3600)
    data = await state.get_data()
    if 'text' not in data:
        return await send_with_auto_delete(msg.chat.id, "❌ Ошибка. Начните заново.", delay=3600)
    nxt = len(notifications) + 1
    now = get_current_time()
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    first = tz.localize(datetime(now.year, now.month, now.day, h, mn, 0))
    if first <= now:
        first += timedelta(days=1)
    notifications[str(nxt)] = {
        'text': data['text'], 'time': first.isoformat(), 'created': now.isoformat(),
        'is_completed': False, 'num': nxt, 'repeat_type': 'every_day',
        'repeat_hour': h, 'repeat_minute': mn,
        'last_trigger': (first - timedelta(days=1)).isoformat(),
        'repeat_count': 0, 'reminder_sent': False
    }
    save_data()
    await sync_notification_to_calendar(str(nxt), 'create')
    await send_with_auto_delete(msg.chat.id, f"✅ **Уведомление #{nxt} создано!**\n📅 Ежедневно в {h:02d}:{mn:02d}", delay=3600)
    await state.finish()
    await update_pending_list(msg.chat.id, persistent=True)

@dp.callback_query_handler(lambda c: c.data.startswith('wd_') and c.data != 'wd_done', state=NotificationStates.waiting_for_weekdays)
async def select_weekday(cb, state):
    day = int(cb.data.replace('wd_', ''))
    data = await state.get_data()
    sel = data.get('selected_weekdays', [])
    if day in sel:
        sel.remove(day)
    else:
        sel.append(day)
    await state.update_data(selected_weekdays=sel)
    kb = InlineKeyboardMarkup(row_width=3)
    for name, d in WEEKDAYS_BUTTONS:
        text = f"✅ {name}" if d in sel else name
        kb.add(InlineKeyboardButton(text, callback_data=f"wd_{d}"))
    kb.add(InlineKeyboardButton("✅ Готово", callback_data="wd_done"))
    names = [WEEKDAYS_NAMES[d] for d in sorted(sel)]
    status = f"Выбрано: {', '.join(names) if sel else 'ничего'}"
    await cb.message.edit_text(f"📅 **Выберите дни недели**\n\n{status}", reply_markup=kb)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data == 'wd_done', state=NotificationStates.waiting_for_weekdays)
async def weekdays_done(cb, state):
    data = await state.get_data()
    sel = data.get('selected_weekdays', [])
    if not sel:
        return await cb.answer("❌ Выберите хотя бы один день!")
    await state.update_data(weekdays_list=sel)
    await send_with_auto_delete(cb.from_user.id, "⏰ **Введите время (ЧЧ:ММ)**\n📝 Например: `09:00`", delay=3600)
    await NotificationStates.waiting_for_weekday_time.set()
    await cb.answer()

@dp.message_handler(state=NotificationStates.waiting_for_weekday_time)
async def set_weekday_time(msg, state):
    await delete_user_message(msg)
    m = re.match(r'^(\d{1,2}):(\d{2})$', msg.text.strip())
    if not m:
        return await send_with_auto_delete(msg.chat.id, "❌ **Формат ЧЧ:ММ**", delay=3600)
    h, mn = map(int, m.groups())
    if h > 23 or mn > 59:
        return await send_with_auto_delete(msg.chat.id, "❌ **Некорректное время**", delay=3600)
    data = await state.get_data()
    wl = data.get('weekdays_list', [])
    if not wl:
        return await send_with_auto_delete(msg.chat.id, "❌ **Не выбраны дни недели**", delay=3600)
    if 'text' not in data:
        return await send_with_auto_delete(msg.chat.id, "❌ Ошибка. Начните заново.", delay=3600)
    first = get_next_weekday(wl, h, mn)
    if not first:
        return await send_with_auto_delete(msg.chat.id, "❌ **Не удалось определить дату**", delay=3600)
    nxt = len(notifications) + 1
    days_names = [WEEKDAYS_NAMES[d] for d in sorted(wl)]
    notifications[str(nxt)] = {
        'text': data['text'], 'time': first.isoformat(), 'created': get_current_time().isoformat(),
        'is_completed': False, 'num': nxt, 'repeat_type': 'weekdays',
        'repeat_hour': h, 'repeat_minute': mn, 'weekdays_list': wl,
        'last_trigger': (first - timedelta(days=7)).isoformat(),
        'repeat_count': 0, 'reminder_sent': False
    }
    save_data()
    await sync_notification_to_calendar(str(nxt), 'create')
    await send_with_auto_delete(msg.chat.id, f"✅ **Уведомление #{nxt} создано!**\n📆 {', '.join(days_names)} в {h:02d}:{mn:02d}", delay=3600)
    await state.finish()
    await update_pending_list(msg.chat.id, persistent=True)

# ---------- ОБРАБОТЧИКИ НЕОТМЕЧЕННЫХ ----------
@dp.callback_query_handler(lambda c: c.data == "refresh_pending", state='*')
async def refresh_pending(cb):
    await update_pending_list(cb.from_user.id, persistent=True)
    await cb.answer("Обновлено")

@dp.callback_query_handler(lambda c: c.data.startswith('pend_select_'), state='*')
async def pend_select(cb, state):
    """Выбор конкретного уведомления из списка"""
    nid = cb.data.replace('pend_select_', '')
    if nid not in pending_notifications:
        return await cb.answer("Уведомление не найдено")
    n = pending_notifications[nid]
    
    # Показываем действия для выбранного уведомления
    kb = InlineKeyboardMarkup(row_width=2)
    recurring_mark = " 🔁" if n.get('is_recurring', False) else ""
    kb.add(
        InlineKeyboardButton("✅ Выполнено", callback_data=f"pend_done_{nid}"),
        InlineKeyboardButton("✏️ Изменить текст", callback_data=f"pend_edit_text_{nid}"),
        InlineKeyboardButton("⏰ Изменить время", callback_data=f"pend_edit_time_{nid}"),
        InlineKeyboardButton("📅 Отложить", callback_data=f"pend_snooze_{nid}"),
        InlineKeyboardButton("❌ Отложить на час", callback_data=f"pend_hour_{nid}"),
        InlineKeyboardButton("◀️ Назад", callback_data="refresh_pending")
    )
    
    dt = datetime.fromisoformat(n['time'])
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    if dt.tzinfo is None:
        dt = tz.localize(dt)
    else:
        dt = dt.astimezone(tz)
    
    recurring_info = "\n🔄 **Это повторяющееся событие** — при отметке \"Выполнено\" удалится только сегодняшнее вхождение." if n.get('is_recurring', False) else ""
    
    text = f"📝 **Уведомление:**\n{n['text']}{recurring_mark}\n\n⏰ **Время:** {dt.strftime('%d.%m.%Y %H:%M')}\n🔄 **Повторов:** {n.get('repeat_count', 0)}{recurring_info}\n\nВыберите действие:"
    await cb.message.edit_text(text, reply_markup=kb)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('pend_done_'), state='*')
async def pend_done(cb):
    nid = cb.data.replace('pend_done_', '')
    if nid in pending_notifications:
        notif = pending_notifications[nid]
        # Просто удаляем из списка неотмеченных
        # Исходное событие в календаре остаётся (для повторяющихся)
        del pending_notifications[nid]
        save_data()
        
        if notif.get('is_recurring', False):
            await cb.answer("✅ Текущее вхождение повторяющегося события отмечено как выполненное!")
        else:
            await cb.answer("✅ Уведомление выполнено и удалено!")
        await update_pending_list(cb.from_user.id, persistent=True)
    else:
        await cb.answer("Уведомление не найдено")
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('pend_edit_text_'), state='*')
async def pend_edit_text(cb, state):
    nid = cb.data.replace('pend_edit_text_', '')
    if nid not in pending_notifications:
        return await cb.answer("Уведомление не найдено")
    await state.update_data(edit_id=nid, is_pending=True)
    await send_with_auto_delete(cb.from_user.id, f"✏️ **Введите новый текст:**\n\n📝 Старый: {pending_notifications[nid]['text']}\n\n💡 Для отмены /cancel", delay=3600)
    await NotificationStates.waiting_for_edit_text.set()
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('pend_edit_time_'), state='*')
async def pend_edit_time(cb, state):
    nid = cb.data.replace('pend_edit_time_', '')
    if nid not in pending_notifications:
        return await cb.answer("Уведомление не найдено")
    await state.update_data(edit_id=nid, is_pending=True)
    await send_with_auto_delete(cb.from_user.id, "🗓️ **Введите новую дату и время**\n📝 Форматы:\n• `17.04 21:00`\n• `31.12.2025 23:59`", delay=3600)
    await NotificationStates.waiting_for_edit_time.set()
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('pend_snooze_') and not c.data.startswith(('pend_snooze_1h_','pend_snooze_3h_','pend_snooze_1d_','pend_snooze_7d_','pend_snooze_custom_')), state='*')
async def pend_snooze(cb, state):
    nid = cb.data.replace('pend_snooze_', '')
    if nid not in pending_notifications:
        return await cb.answer("Уведомление не найдено")
    await state.update_data(snooze_notif_id=nid)
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("⏰ 1 час", callback_data=f"pend_snooze_1h_{nid}"),
        InlineKeyboardButton("⏰ 3 часа", callback_data=f"pend_snooze_3h_{nid}"),
        InlineKeyboardButton("📅 1 день", callback_data=f"pend_snooze_1d_{nid}"),
        InlineKeyboardButton("📅 7 дней", callback_data=f"pend_snooze_7d_{nid}"),
        InlineKeyboardButton("🎯 Свой вариант", callback_data=f"pend_snooze_custom_{nid}"),
        InlineKeyboardButton("◀️ Назад", callback_data=f"pend_select_{nid}")
    )
    await cb.message.edit_text(f"⏰ **Отложить уведомление?**\n\n📝 {pending_notifications[nid]['text']}", reply_markup=kb)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('pend_hour_'), state='*')
async def pend_hour(cb):
    nid = cb.data.replace('pend_hour_', '')
    await process_pending_snooze(cb, nid, 1, "hours")

@dp.callback_query_handler(lambda c: c.data.startswith('pend_snooze_1h_'), state='*')
async def snooze_1h(cb):
    nid = cb.data.replace('pend_snooze_1h_', '')
    await process_pending_snooze(cb, nid, 1, "hours")

@dp.callback_query_handler(lambda c: c.data.startswith('pend_snooze_3h_'), state='*')
async def snooze_3h(cb):
    nid = cb.data.replace('pend_snooze_3h_', '')
    await process_pending_snooze(cb, nid, 3, "hours")

@dp.callback_query_handler(lambda c: c.data.startswith('pend_snooze_1d_'), state='*')
async def snooze_1d(cb):
    nid = cb.data.replace('pend_snooze_1d_', '')
    await process_pending_snooze(cb, nid, 1, "days")

@dp.callback_query_handler(lambda c: c.data.startswith('pend_snooze_7d_'), state='*')
async def snooze_7d(cb):
    nid = cb.data.replace('pend_snooze_7d_', '')
    await process_pending_snooze(cb, nid, 7, "days")

async def process_pending_snooze(cb, nid, val, unit):
    if nid not in pending_notifications:
        return await cb.answer("Уведомление не найдено")
    now = get_current_time()
    new_time = now + (timedelta(hours=val) if unit == "hours" else timedelta(days=val))
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    if new_time.tzinfo is None:
        new_time = tz.localize(new_time)
    n = pending_notifications[nid]
    n['time'] = new_time.isoformat()
    n['reminder_sent'] = False
    n['repeat_count'] = 0
    n['last_reminder_time'] = None
    await sync_notification_to_calendar(nid, 'create')
    save_data()
    await cb.message.edit_text(f"⏰ **Уведомление отложено на {val} {unit}**\n🕐 Новое время: {new_time.strftime('%d.%m.%Y %H:%M')}")
    await update_pending_list(cb.from_user.id, persistent=True)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('pend_snooze_custom_'), state='*')
async def snooze_custom(cb, state):
    nid = cb.data.replace('pend_snooze_custom_', '')
    await state.update_data(snooze_notif_id=nid)
    await send_with_auto_delete(cb.from_user.id, "🗓️ **Введите новую дату и время**\n📝 Форматы:\n• `17.04 21:00`\n• `31.12.2025 23:59`", delay=3600)
    await SnoozeStates.waiting_for_specific_date.set()
    await cb.answer()

@dp.message_handler(state=NotificationStates.waiting_for_edit_text)
async def save_edit_text(msg, state):
    await delete_user_message(msg)
    data = await state.get_data()
    nid = data.get('edit_id')
    is_pending = data.get('is_pending', False)
    target = pending_notifications if is_pending else notifications
    if not nid or nid not in target:
        return await send_with_auto_delete(msg.chat.id, "❌ **Уведомление не найдено!**", delay=3600)
    old = target[nid]['text']
    target[nid]['text'] = msg.text
    target[nid]['reminder_sent'] = False
    target[nid]['repeat_count'] = 0
    target[nid]['last_reminder_time'] = None
    save_data()
    await sync_notification_to_calendar(nid, 'create')
    await send_with_auto_delete(msg.chat.id, f"✅ **Текст изменён!**\n\nСтарый: {old}\nНовый: {msg.text}", delay=3600)
    await update_pending_list(msg.chat.id, persistent=True)
    await state.finish()

@dp.message_handler(state=NotificationStates.waiting_for_edit_time)
async def save_edit_time(msg, state):
    await delete_user_message(msg)
    dt = parse_datetime(msg.text)
    if dt is None or dt <= get_current_time():
        return await send_with_auto_delete(msg.chat.id, "❌ **Неверный формат или дата в прошлом!**", delay=3600)
    data = await state.get_data()
    nid = data.get('edit_id')
    is_pending = data.get('is_pending', False)
    target = pending_notifications if is_pending else notifications
    if not nid or nid not in target:
        return await send_with_auto_delete(msg.chat.id, "❌ **Уведомление не найдено!**", delay=3600)
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    if dt.tzinfo is None:
        dt = tz.localize(dt)
    target[nid]['time'] = dt.isoformat()
    target[nid]['reminder_sent'] = False
    target[nid]['repeat_count'] = 0
    target[nid]['last_reminder_time'] = None
    save_data()
    await sync_notification_to_calendar(nid, 'create')
    await send_with_auto_delete(msg.chat.id, f"✅ **Время изменено!**\n🕐 Новое время: {dt.strftime('%d.%m.%Y %H:%M')}", delay=3600)
    await update_pending_list(msg.chat.id, persistent=True)
    await state.finish()

@dp.message_handler(state=SnoozeStates.waiting_for_specific_date)
async def snooze_set_date(msg, state):
    await delete_user_message(msg)
    dt = parse_datetime(msg.text)
    if dt is None or dt <= get_current_time():
        return await send_with_auto_delete(msg.chat.id, "❌ **Неверный формат или дата в прошлом!**", delay=3600)
    data = await state.get_data()
    nid = data.get('snooze_notif_id')
    if not nid or nid not in pending_notifications:
        return await send_with_auto_delete(msg.chat.id, "❌ **Уведомление не найдено!**", delay=3600)
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    if dt.tzinfo is None:
        dt = tz.localize(dt)
    pending_notifications[nid]['time'] = dt.isoformat()
    pending_notifications[nid]['reminder_sent'] = False
    pending_notifications[nid]['repeat_count'] = 0
    pending_notifications[nid]['last_reminder_time'] = None
    save_data()
    await sync_notification_to_calendar(nid, 'create')
    await send_with_auto_delete(msg.chat.id, f"⏰ **Уведомление отложено!**\n🕐 Новое время: {dt.strftime('%d.%m.%Y %H:%M')}", delay=3600)
    await update_pending_list(msg.chat.id, persistent=True)
    await state.finish()

@dp.callback_query_handler(lambda c: c.data == "cancel_edit", state='*')
async def cancel_edit(cb, state):
    await state.finish()
    await update_pending_list(cb.from_user.id, persistent=True)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data == "cancel_snooze", state='*')
async def cancel_snooze(cb, state):
    await state.finish()
    await update_pending_list(cb.from_user.id, persistent=True)
    await cb.answer()

# ---------- ОБРАБОТЧИКИ КАЛЕНДАРЯ ----------
@dp.message_handler(lambda m: m.text == "📅 Календарь", state='*')
async def view_calendar(msg, state):
    await delete_user_message(msg)
    await state.finish()
    now = get_current_time()
    await show_calendar_events(msg.chat.id, now.year, now.month, persistent=True)

@dp.message_handler(lambda m: m.text == "⚠️ Неотмеченные", state='*')
async def view_pending(msg, state):
    await delete_user_message(msg)
    await state.finish()
    await update_pending_list(msg.chat.id, persistent=True)

@dp.message_handler(lambda m: m.text == "⚙️ Настройки", state='*')
async def settings_menu(msg, state):
    await delete_user_message(msg)
    await state.finish()
    await settings_menu_handler(msg, state)

@dp.callback_query_handler(lambda c: c.data.startswith("cal_prev_"), state='*')
async def cal_prev(cb):
    parts = cb.data.replace("cal_prev_", "").split("_")
    await show_calendar_events(cb.from_user.id, int(parts[0]), int(parts[1]), persistent=True)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("cal_next_"), state='*')
async def cal_next(cb):
    parts = cb.data.replace("cal_next_", "").split("_")
    await show_calendar_events(cb.from_user.id, int(parts[0]), int(parts[1]), persistent=True)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("cal_refresh_"), state='*')
async def cal_refresh(cb):
    parts = cb.data.replace("cal_refresh_", "").split("_")
    await show_calendar_events(cb.from_user.id, int(parts[0]), int(parts[1]), force_refresh=True, persistent=True)
    await cb.answer("✅ Календарь обновлён")

@dp.callback_query_handler(lambda c: c.data.startswith("cal_sync_"), state='*')
async def cal_sync(cb):
    parts = cb.data.replace("cal_sync_", "").split("_")
    y, m = int(parts[0]), int(parts[1])
    await cb.message.edit_text("🔄 **Синхронизация с календарём...**")
    if get_caldav_available():
        api = CalDAVCalendarAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
        events = await api.get_month_events(y, m)
        calendar_events_cache[f"{y}_{m}"] = events
        last_calendar_update[f"{y}_{m}"] = get_current_time()
        await show_calendar_events(cb.from_user.id, y, m, force_refresh=True, persistent=True)
        await cb.answer("✅ Синхронизация завершена")
    else:
        await cb.answer("❌ CalDAV не настроен")

@dp.callback_query_handler(lambda c: c.data == "curr_month", state='*')
async def curr_month(cb):
    now = get_current_time()
    await show_calendar_events(cb.from_user.id, now.year, now.month, persistent=True)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data == "edit_calendar", state='*')
async def edit_calendar(cb, state):
    now = get_current_time()
    await update_calendar_events_cache(now.year, now.month)
    events = calendar_events_cache.get(f"{now.year}_{now.month}", [])
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    future = []
    for ev in events:
        try:
            dt = datetime.fromisoformat(ev['start'])
            if dt.tzinfo is None:
                dt = tz.localize(dt)
            else:
                dt = dt.astimezone(tz)
            if dt >= today_start:
                future.append((dt, ev))
        except:
            continue
    if not future:
        return await cb.answer("Нет событий для редактирования")
    kb = InlineKeyboardMarkup(row_width=1)
    for idx, (dt, ev) in enumerate(future[:20]):
        sid = hashlib.md5(ev['id'].encode()).hexdigest()[:16]
        event_id_map[sid] = ev['id']
        recurring_mark = " 🔁" if ev.get('is_recurring', False) else ""
        kb.add(InlineKeyboardButton(f"{idx+1}. {dt.strftime('%d.%m %H:%M')} - {ev['summary'][:30]}{recurring_mark}", callback_data=f"sel_cal_event_{sid}"))
    kb.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_edit"))
    await cb.message.edit_text("✏️ **Выберите событие для редактирования:**", reply_markup=kb)
    await EditCalendarEventStates.waiting_for_event_selection.set()
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("sel_cal_event_"), state=EditCalendarEventStates.waiting_for_event_selection)
async def sel_cal_event(cb, state):
    sid = cb.data.replace("sel_cal_event_", "")
    full = event_id_map.get(sid)
    if not full:
        return await cb.answer("Событие не найдено")
    await state.update_data(edit_event_id=full)
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✏️ Изменить текст", callback_data="edit_cal_text"),
        InlineKeyboardButton("⏰ Изменить дату/время", callback_data="edit_cal_time"),
        InlineKeyboardButton("❌ Отмена", callback_data="cancel_edit")
    )
    await cb.message.edit_text("✏️ **Что изменить в событии?**", reply_markup=kb)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data == "edit_cal_text", state=EditCalendarEventStates.waiting_for_event_selection)
async def edit_cal_text_prompt(cb, state):
    await send_with_auto_delete(cb.from_user.id, "✏️ **Введите новый текст события:**\n\n💡 Для отмены /cancel", delay=3600)
    await EditCalendarEventStates.waiting_for_new_text.set()
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data == "edit_cal_time", state=EditCalendarEventStates.waiting_for_event_selection)
async def edit_cal_time_prompt(cb, state):
    await send_with_auto_delete(cb.from_user.id, "🗓️ **Введите новую дату и время**\n📝 Форматы:\n• `17.04 21:00`\n• `31.12.2025 23:59`", delay=3600)
    await EditCalendarEventStates.waiting_for_new_datetime.set()
    await cb.answer()

@dp.message_handler(state=EditCalendarEventStates.waiting_for_new_text)
async def save_cal_text(msg, state):
    await delete_user_message(msg)
    data = await state.get_data()
    eid = data.get('edit_event_id')
    if not eid:
        return await send_with_auto_delete(msg.chat.id, "❌ Ошибка", delay=3600)
    now = get_current_time()
    await update_calendar_events_cache(now.year, now.month)
    events = calendar_events_cache.get(f"{now.year}_{now.month}", [])
    ev = next((x for x in events if x['id'] == eid), None)
    if not ev:
        return await send_with_auto_delete(msg.chat.id, "❌ Событие не найдено", delay=3600)
    api = CalDAVCalendarAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
    dt = datetime.fromisoformat(ev['start'])
    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
    if dt.tzinfo is None:
        dt = tz.localize(dt)
    ok = await api.update_event(eid, new_summary=msg.text, new_start=dt)
    if ok:
        await send_with_auto_delete(msg.chat.id, f"✅ **Текст события изменён!**\n{msg.text}", delay=3600)
        await update_calendar_events_cache(now.year, now.month, force=True)
        await sync_calendar_to_pending()
    else:
        await send_with_auto_delete(msg.chat.id, "❌ Ошибка при обновлении события", delay=3600)
    await state.finish()
    await update_pending_list(msg.chat.id, persistent=True)

@dp.message_handler(state=EditCalendarEventStates.waiting_for_new_datetime)
async def save_cal_datetime(msg, state):
    await delete_user_message(msg)
    dt = parse_datetime(msg.text)
    if dt is None or dt <= get_current_time():
        return await send_with_auto_delete(msg.chat.id, "❌ **Неверный формат или дата в прошлом!**", delay=3600)
    data = await state.get_data()
    eid = data.get('edit_event_id')
    if not eid:
        return await send_with_auto_delete(msg.chat.id, "❌ Ошибка", delay=3600)
    now = get_current_time()
    await update_calendar_events_cache(now.year, now.month)
    events = calendar_events_cache.get(f"{now.year}_{now.month}", [])
    ev = next((x for x in events if x['id'] == eid), None)
    if not ev:
        return await send_with_auto_delete(msg.chat.id, "❌ Событие не найдено", delay=3600)
    api = CalDAVCalendarAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
    ok = await api.update_event(eid, new_summary=ev['summary'], new_start=dt)
    if ok:
        await send_with_auto_delete(msg.chat.id, f"✅ **Дата/время события изменены!**\n🕐 {dt.strftime('%d.%m.%Y %H:%M')}", delay=3600)
        await update_calendar_events_cache(now.year, now.month, force=True)
        await sync_calendar_to_pending()
    else:
        await send_with_auto_delete(msg.chat.id, "❌ Ошибка при обновлении события", delay=3600)
    await state.finish()
    await update_pending_list(msg.chat.id, persistent=True)

# ---------- НАСТРОЙКИ ----------
async def settings_menu_handler(msg, state):
    global notifications_enabled
    status = "🔔 Вкл" if notifications_enabled else "🔕 Выкл"
    cal_sync = "✅ Вкл" if config.get('calendar_sync_enabled', True) else "❌ Выкл"
    if get_caldav_available():
        ok, _ = await check_caldav_connection()
        caldav_status = "✅ Доступен" if ok else "❌ Ошибка"
    else:
        caldav_status = "❌ Не настроен"
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton(f"Уведомления: {status}", callback_data="toggle_notify"),
        InlineKeyboardButton(f"Синхр. с календарём: {cal_sync}", callback_data="toggle_cal_sync"),
        InlineKeyboardButton("🕐 Время проверки", callback_data="set_check_time"),
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
        await update_calendar_events_cache(now.year, now.month, force=True)
        await sync_calendar_to_pending()
    else:
        await cb.message.edit_text(f"❌ **{msg}**\n\n🔧 Получите новый пароль приложения: https://id.yandex.ru/security/app-passwords")
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data == "toggle_notify", state='*')
async def toggle_notify(cb, state):
    global notifications_enabled
    notifications_enabled = not notifications_enabled
    config['notifications_enabled'] = notifications_enabled
    save_data()
    await cb.message.edit_text(f"✅ **Уведомления {'включены' if notifications_enabled else 'выключены'}!**")
    await settings_menu_handler(cb.message, state)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data == "toggle_cal_sync", state='*')
async def toggle_cal_sync(cb, state):
    cur = config.get('calendar_sync_enabled', True)
    config['calendar_sync_enabled'] = not cur
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
            await cb.message.edit_text("✅ **Синхронизация с календарём включена!**")
            now = get_current_time()
            await update_calendar_events_cache(now.year, now.month, force=True)
            await sync_calendar_to_pending()
        else:
            await cb.message.edit_text("⚠️ **Синхронизация включена, но нет доступа к календарю!**")
    else:
        await cb.message.edit_text(f"✅ **Синхронизация {'включена' if config['calendar_sync_enabled'] else 'выключена'}!**")
    await settings_menu_handler(cb.message, state)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data == "set_check_time", state='*')
async def set_check_time(cb):
    await send_with_auto_delete(cb.from_user.id, f"🕐 **Текущее время проверки:** `{config.get('daily_check_time', '06:00')}`\n\nВведите новое время (ЧЧ:ММ):", delay=3600)
    await SettingsStates.waiting_for_check_time.set()
    await cb.answer()

@dp.message_handler(state=SettingsStates.waiting_for_check_time)
async def save_check_time(msg, state):
    await delete_user_message(msg)
    try:
        datetime.strptime(msg.text, "%H:%M")
        config['daily_check_time'] = msg.text
        save_data()
        await send_with_auto_delete(msg.chat.id, f"✅ **Время проверки установлено:** {msg.text}", delay=3600)
    except:
        await send_with_auto_delete(msg.chat.id, "❌ **Неверный формат!** Используйте ЧЧ:ММ", delay=3600)
    await state.finish()

@dp.callback_query_handler(lambda c: c.data == "set_timezone", state='*')
async def set_timezone(cb):
    kb = InlineKeyboardMarkup(row_width=2)
    for name in TIMEZONES:
        kb.add(InlineKeyboardButton(name, callback_data=f"tz_{name}"))
    kb.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_tz"))
    await cb.message.edit_text(f"🌍 **Выберите часовой пояс**\n\nТекущий: {config.get('timezone', 'Europe/Moscow')}", reply_markup=kb)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("tz_"), state='*')
async def save_tz(cb, state):
    name = cb.data.replace("tz_", "")
    tz = TIMEZONES.get(name, 'Europe/Moscow')
    config['timezone'] = tz
    save_data()
    await cb.message.edit_text(f"✅ **Часовой пояс установлен:** {name}\n🕐 {get_current_time().strftime('%d.%m.%Y %H:%M:%S')}")
    await settings_menu_handler(cb.message, state)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data == "cancel_tz", state='*')
async def cancel_tz(cb, state):
    await settings_menu_handler(cb.message, state)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data == "info", state='*')
async def info(cb):
    ok, _ = await check_caldav_connection() if get_caldav_available() else (False, "")
    caldav_status = "✅ Доступен" if ok else "❌ Ошибка" if get_caldav_available() else "❌ Не настроен"
    info_text = f"""📊 **СТАТИСТИКА**

🤖 **Версия:** v{BOT_VERSION} ({BOT_VERSION_DATE})

⚠️ **Неотмеченных:** `{len(pending_notifications)}`
🕐 **Проверка уведомлений:** `{config.get('daily_check_time', '06:00')}`
🌍 **Часовой пояс:** `{config.get('timezone', 'Europe/Moscow')}`
🕐 **Текущее время:** `{get_current_time().strftime('%d.%m.%Y %H:%M:%S')}`
🔔 **Уведомления:** `{'Вкл' if notifications_enabled else 'Выкл'}`
📅 **Синхр. с календарём:** `{'Вкл' if config.get('calendar_sync_enabled', True) else 'Выкл'}`
📧 **CalDAV:** `{caldav_status}`"""
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

async def auto_update_calendar_cache():
    while True:
        try:
            now = get_current_time()
            await update_calendar_events_cache(now.year, now.month, force=True)
            await sync_calendar_to_pending()
            logger.info("Автообновление календаря выполнено")
        except Exception as e:
            logger.error(f"auto_update error: {e}")
        await asyncio.sleep(900)

async def on_startup(dp):
    init_folders()
    load_data()
    # перенумерация
    new_notifications = {}
    for i, (nid, n) in enumerate(sorted(notifications.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 0), 1):
        n['num'] = i
        new_notifications[str(i)] = n
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
    logger.info(f"Неотмеченных: {len(pending_notifications)}")
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