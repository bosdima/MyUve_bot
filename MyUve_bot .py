import asyncio
import json
import os
import logging
import logging.handlers
import re
import hashlib
import requests
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple
import pytz
import caldav
from uuid import uuid4
from xml.etree import ElementTree as ET

try:
    from dateutil.rrule import rrulestr
    DATEUTIL_AVAILABLE = True
except ImportError:
    DATEUTIL_AVAILABLE = False
    print("WARNING: python-dateutil not installed. Run: pip install python-dateutil")

# ===== НАСТРОЙКИ ЛОГИРОВАНИЯ =====
LOG_FILE = 'bot.log'
LOG_MAX_BYTES = 200 * 1024
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

# ===== AIORAM IMPORTS =====
from aiogram import Bot, Dispatcher
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils import executor
from dotenv import load_dotenv

load_dotenv()

# ===== КОНФИГУРАЦИЯ =====
BOT_TOKEN = os.getenv('BOT_TOKEN')
ADMIN_ID = int(os.getenv('ADMIN_ID')) if os.getenv('ADMIN_ID') else None
YANDEX_EMAIL = os.getenv('YANDEX_EMAIL')
YANDEX_APP_PASSWORD = os.getenv('YANDEX_APP_PASSWORD')
YANDEX_CALDAV_URL = "https://caldav.yandex.ru"

BOT_VERSION = "5.0.1-FIX"
BOT_VERSION_DATE = "25.04.2026"

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
dp.middleware.setup(LoggingMiddleware())

CONFIG_FILE = 'config.json'
config: Dict = {}
notifications_enabled = True
last_sync_time: Optional[datetime] = None

# 🔑 Глобальный кэш событий: {email: {'data': [...], 'timestamp': float}}
events_cache = {}
CACHE_TTL = 60  # Кэш живет 60 секунд, чтобы не спамить Яндекс при частых обновлениях

pending_notifications: Dict[str, Dict] = {}
last_notification_hour: Dict[str, int] = {}

TIMEZONES = {
    'Москва (UTC+3)': 'Europe/Moscow',
    'Калининград (UTC+2)': 'Europe/Kaliningrad',
    'Екатеринбург (UTC+5)': 'Asia/Yekaterinburg',
    'Новосибирск (UTC+7)': 'Asia/Novosibirsk',
    'Владивосток (UTC+10)': 'Asia/Vladivostok',
    'Камчатка (UTC+12)': 'Asia/Kamchatka'
}

# ===== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====

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
            if y < 100: y += 2000
            try:
                dt = tz.localize(datetime(y, int(mth), int(d), int(h), int(minute)))
                if dt < now and len(groups) in (4, 2):
                    dt = tz.localize(datetime(y+1, int(mth), int(d), int(h), int(minute)))
                return dt
            except Exception:
                return None
    return None

# ===== CALDAV API С ПРЯМЫМИ ЗАПРОСАМИ ДЛЯ АКТУАЛЬНОСТИ =====

class FreshCalDAVAPI:
    """
    Класс для работы с CalDAV, который гарантирует получение свежих данных,
    игнорируя кэш библиотеки caldav и используя прямые HTTP-запросы с заголовками No-Cache.
    """
    def __init__(self, email: str, password: str):
        self.email = email
        self.password = password
        self.session = requests.Session()
        self.session.auth = (email, password)
        # 🔑 Важные заголовки для обхода кэша
        self.session.headers.update({
            'User-Agent': 'MyUveBot/5.0.1',
            'Cache-Control': 'no-cache, no-store, must-revalidate',
            'Pragma': 'no-cache',
            'Expires': '0'
        })

    def _get_calendar_url(self):
        # Обычно календарь по умолчанию находится по этому пути
        return f"{YANDEX_CALDAV_URL}/{self.email}/"

    def fetch_events_raw(self, start_dt: datetime, end_dt: datetime) -> List[str]:
        """
        Делает прямой REPORT запрос к CalDAV серверу с заголовками No-Cache.
        Возвращает список сырых iCal строк.
        """
        url = self._get_calendar_url()
        
        # Формируем временные метки в формате UTC для запроса
        start_utc = start_dt.astimezone(pytz.UTC).strftime('%Y%m%dT%H%M%SZ')
        end_utc = end_dt.astimezone(pytz.UTC).strftime('%Y%m%dT%H%M%SZ')

        # XML тело запроса CalDAV REPORT
        report_xml = f"""<?xml version="1.0" encoding="utf-8" ?>
<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
    <D:prop>
        <C:calendar-data/>
    </D:prop>
    <C:filter>
        <C:comp-filter name="VCALENDAR">
            <C:comp-filter name="VEVENT">
                <C:time-range start="{start_utc}" end="{end_utc}"/>
            </C:comp-filter>
        </C:comp-filter>
    </C:filter>
</C:calendar-query>"""

        try:
            # 🔑 Прямой POST запрос с заголовками против кэша
            response = self.session.request(
                "REPORT", 
                url, 
                data=report_xml.encode('utf-8'),
                headers={'Content-Type': 'application/xml; charset=utf-8', 'Depth': '1'}
            )
            response.raise_for_status()
            
            # Парсим ответ XML
            root = ET.fromstring(response.content)
            ns = {'D': 'DAV:', 'C': 'urn:ietf:params:xml:ns:caldav'}
            
            events_data = []
            for resp in root.findall('.//D:response', ns):
                propstat = resp.find('.//D:propstat', ns)
                if propstat is not None:
                    prop = propstat.find('.//D:prop', ns)
                    if prop is not None:
                        cal_data = prop.find('.//C:calendar-data', ns)
                        if cal_data is not None and cal_data.text:
                            events_data.append(cal_data.text)
            
            logger.info(f"Fetched {len(events_data)} raw events from Yandex directly.")
            return events_data

        except Exception as e:
            logger.error(f"Error fetching raw events: {e}")
            # Фоллбэк на стандартный метод caldav, если прямой запрос не сработал
            return self._fallback_fetch(start_dt, end_dt)

    def _fallback_fetch(self, start_dt: datetime, end_dt: datetime) -> List[str]:
        """Резервный метод через библиотеку caldav"""
        try:
            client = caldav.DAVClient(url=YANDEX_CALDAV_URL, username=self.email, password=self.password)
            principal = client.principal()
            calendars = principal.calendars()
            if not calendars: return []
            cal = calendars[0]
            events = cal.date_search(start=start_dt, end=end_dt, expand=False)
            return [ev.data for ev in events]
        except Exception as e:
            logger.error(f"Fallback fetch error: {e}")
            return []

    def parse_ical_events(self, raw_events: List[str]) -> List[Dict]:
        """Парсит сырые iCal строки в удобную структуру"""
        parsed_events = []
        tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
        
        for ical_str in raw_events:
            try:
                # Используем vobject через caldav для парсинга
                import vobject
                vobj = vobject.readOne(ical_str)
                vevent = vobj.vevent
                
                # Дата начала
                dtstart_raw = vevent.dtstart.value
                if isinstance(dtstart_raw, datetime):
                    dtstart = dtstart_raw if dtstart_raw.tzinfo else tz.localize(dtstart_raw)
                elif isinstance(dtstart_raw, date):
                    dtstart = tz.localize(datetime.combine(dtstart_raw, datetime.min.time()))
                else:
                    continue

                summary = str(vevent.summary.value) if hasattr(vevent, 'summary') else 'Без названия'
                
                # UID и URL (URL генерируем искусственно для идентификации, так как в raw data его нет)
                uid = str(vevent.uid.value) if hasattr(vevent, 'uid') else hashlib.md5(ical_str.encode()).hexdigest()
                # Для Яндекс Календаря URL обычно можно восстановить, но для удаления нам нужен UID или полный объект
                # Мы будем искать событие по UID в полном списке позже, если потребуется удаление
                
                is_recurring = hasattr(vevent, 'rrule') and vevent.rrule.value is not None
                rrule_str = vevent.rrule.to_ical().decode() if is_recurring else None

                # EXDATE
                exdates = []
                if hasattr(vevent, 'exdate'):
                    for exd in vevent.exdate_list:
                        for val in exd.vals:
                            if isinstance(val, datetime):
                                utc_val = val.astimezone(pytz.UTC) if val.tzinfo else tz.localize(val).astimezone(pytz.UTC)
                                exdates.append(utc_val.strftime('%Y%m%dT%H%M%SZ'))
                            elif isinstance(val, date):
                                # Для all-day событий
                                exdates.append(val.strftime('%Y%m%d'))

                parsed_events.append({
                    'uid': uid,
                    'summary': summary,
                    'start': dtstart,
                    'is_recurring': is_recurring,
                    'rrule': rrule_str,
                    'exdates': exdates,
                    'all_day': not isinstance(dtstart_raw, datetime),
                    'raw_ical': ical_str # Сохраняем для возможного анализа
                })
            except Exception as e:
                logger.warning(f"Failed to parse one event: {e}")
                continue
        
        return parsed_events

    async def get_fresh_events(self, days_ahead: int = 2) -> List[Dict]:
        """
        Главный метод: получает свежие события за период.
        """
        now = get_current_time()
        start_dt = now - timedelta(days=1) # Берем с вчера, чтобы захватить прошедшие сегодня
        end_dt = now + timedelta(days=days_ahead)
        
        # 🔑 Получаем сырые данные напрямую с сервера
        raw_events = self.fetch_events_raw(start_dt, end_dt)
        
        # Парсим их
        events = self.parse_ical_events(raw_events)
        
        logger.info(f"Parsed {len(events)} fresh events.")
        return events

    async def delete_event_by_uid(self, uid: str):
        """Удаление события по UID через прямой DELETE запрос"""
        url = f"{self._get_calendar_url()}{uid}.ics"
        try:
            resp = self.session.delete(url)
            if resp.status_code in [200, 204, 404]: # 404 значит уже удалено
                logger.info(f"Event {uid} deleted via direct DELETE.")
                return True
            else:
                logger.error(f"Failed to delete {uid}: {resp.status_code}")
                return False
        except Exception as e:
            logger.error(f"Delete error: {e}")
            return False

    async def add_exdate_by_uid(self, uid: str, exception_dt: datetime):
        """
        Добавление EXDATE через модификацию события.
        1. Скачиваем текущее событие.
        2. Добавляем EXDATE.
        3. Загружаем обратно (PUT).
        """
        url = f"{self._get_calendar_url()}{uid}.ics"
        try:
            # 1. Получаем текущее событие
            resp = self.session.get(url)
            if resp.status_code != 200:
                logger.error(f"Could not fetch event {uid} for modification: {resp.status_code}")
                return False
            
            ical_text = resp.text
            import vobject
            vobj = vobject.readOne(ical_text)
            vevent = vobj.vevent
            
            # 2. Добавляем EXDATE
            tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
            if exception_dt.tzinfo is None:
                exception_dt = tz.localize(exception_dt)
            exdate_utc = exception_dt.astimezone(pytz.UTC)
            
            # Создаем свойство EXDATE
            exdate_prop = vobject.base.Property('EXDATE')
            exdate_prop.value = exdate_utc
            
            # Добавляем в событие
            vevent.add(exdate_prop)
            
            # 3. Отправляем обновленное событие (PUT)
            new_ical = vobj.serialize()
            put_resp = self.session.put(url, data=new_ical, headers={'Content-Type': 'text/calendar'})
            
            if put_resp.status_code in [200, 204, 201]:
                logger.info(f"EXDATE added to {uid} for {exdate_utc}")
                return True
            else:
                logger.error(f"Failed to update event {uid}: {put_resp.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Add EXDATE error: {e}", exc_info=True)
            return False

# ===== ФУНКЦИИ РАБОТЫ С СОБЫТИЯМИ =====

def get_caldav_available():
    return bool(YANDEX_EMAIL and YANDEX_APP_PASSWORD)

async def check_caldav_connection() -> Tuple[bool, str]:
    if not get_caldav_available():
        return False, "CalDAV не настроен"
    api = FreshCalDAVAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
    try:
        # Пробуем получить события за сегодня, чтобы проверить связь
        events = await api.get_fresh_events(days_ahead=0)
        return True, "Подключено"
    except Exception as e:
        return False, f"Ошибка: {str(e)}"

async def get_today_tomorrow_events(force_refresh: bool = False) -> List[Tuple[datetime, Dict]]:
    now = get_current_time()
    api = FreshCalDAVAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
    today = now.date()
    tomorrow = today + timedelta(days=1)
    
    # 🔑 Всегда получаем свежие данные, если force_refresh или кэш устарел
    current_time_ts = datetime.now().timestamp()
    cache_key = YANDEX_EMAIL
    
    if not force_refresh and cache_key in events_cache:
        if current_time_ts - events_cache[cache_key]['timestamp'] < CACHE_TTL:
            logger.debug("Using cached events")
            all_events = events_cache[cache_key]['data']
        else:
            all_events = await api.get_fresh_events(days_ahead=2)
            events_cache[cache_key] = {'data': all_events, 'timestamp': current_time_ts}
    else:
        all_events = await api.get_fresh_events(days_ahead=2)
        events_cache[cache_key] = {'data': all_events, 'timestamp': current_time_ts}

    result = []
    for ev in all_events:
        if not ev.get('is_recurring'):
            event_date = ev['start'].date()
            if event_date == today or event_date == tomorrow:
                result.append((ev['start'], ev))
        else:
            # Для повторяющихся нужно развернуть
            # Используем простую логику разворачивания, так как у нас есть rrule
            if DATEUTIL_AVAILABLE and ev.get('rrule'):
                try:
                    rule = rrulestr(ev['rrule'], dtstart=ev['start'])
                    # Генерируем вхождения на 2 дня вперед
                    occurrences = rule.between(now - timedelta(days=1), now + timedelta(days=2), inc=True)
                    for occ in occurrences:
                        if occ.tzinfo is None:
                            occ = pytz.timezone(config.get('timezone', 'Europe/Moscow')).localize(occ)
                        
                        # Проверка EXDATE
                        occ_utc = occ.astimezone(pytz.UTC).strftime('%Y%m%dT%H%M%SZ')
                        if occ_utc in ev.get('exdates', []):
                            continue
                        
                        occ_date = occ.date()
                        if occ_date == today or occ_date == tomorrow:
                            ev_copy = ev.copy()
                            ev_copy['start'] = occ
                            result.append((occ, ev_copy))
                except Exception as e:
                    logger.error(f"Error expanding recurrence: {e}")
            else:
                # Если нет dateutil, просто добавляем как есть (может быть неточно для сложных правил)
                event_date = ev['start'].date()
                if event_date == today or event_date == tomorrow:
                    result.append((ev['start'], ev))

    result.sort(key=lambda x: x[0])
    unique = {}
    for dt, ev in result:
        key = f"{ev.get('uid', '')}_{dt.strftime('%Y%m%d%H%M')}"
        if key not in unique:
            unique[key] = (dt, ev)
    
    final = list(unique.values())
    logger.info(f"Found {len(final)} events for today/tomorrow")
    return final

async def get_formatted_calendar_events(force_refresh: bool = False) -> str:
    events = await get_today_tomorrow_events(force_refresh=force_refresh)
    now = get_current_time()
    
    if not events:
        return "📅 Нет событий на сегодня и завтра"
    
    today = now.date()
    tomorrow = today + timedelta(days=1)
    
    text = "📅 **СОБЫТИЯ НА СЕГОДНЯ И ЗАВТРА**\n\n"
    today_events, today_passed, tomorrow_events = [], [], []
    
    for dt, ev in events:
        if dt.date() == today:
            (today_passed if dt < now else today_events).append((dt, ev))
        elif dt.date() == tomorrow:
            tomorrow_events.append((dt, ev))
    
    if today_passed:
        text += "🔴 **СЕГОДНЯ (ПРОШЕДШИЕ)**\n"
        for dt, ev in today_passed:
            rec = " 🔁" if ev.get('is_recurring') else ""
            text += f"   • {dt.strftime('%H:%M')} — **{ev['summary']}**{rec} ⚠️\n"
        text += "\n"
    
    if today_events:
        text += "🟢 **СЕГОДНЯ (ПРЕДСТОЯЩИЕ)**\n"
        for dt, ev in today_events:
            rec = " 🔁" if ev.get('is_recurring') else ""
            text += f"   • {dt.strftime('%H:%M')} — **{ev['summary']}**{rec}\n"
        text += "\n"
    
    if tomorrow_events:
        text += "🟠 **ЗАВТРА**\n"
        for dt, ev in tomorrow_events:
            rec = " 🔁" if ev.get('is_recurring') else ""
            text += f"   • {dt.strftime('%H:%M')} — **{ev['summary']}**{rec}\n"
        text += "\n"
    
    if last_sync_time:
        text += f"\n🔄 *Последняя синхронизация:* {last_sync_time.strftime('%d.%m.%Y %H:%M:%S')}"
    
    return text

async def get_pending_notifications() -> List[Dict]:
    now = get_current_time()
    today = now.date()
    
    # Очистка старых уведомлений
    cutoff = now - timedelta(hours=24)
    for k in list(pending_notifications.keys()):
        if pending_notifications[k]['time'] < cutoff:
            del pending_notifications[k]
    
    # Получаем свежие события
    events = await get_today_tomorrow_events(force_refresh=False) # Используем кэш, если свежий
    pending = []
    
    for dt, ev in events:
        if dt.date() == today and dt <= now:
            uid = ev.get('uid', '')
            short_id = hashlib.md5(f"{uid}_{dt.strftime('%Y%m%d%H%M')}".encode()).hexdigest()[:12]
            
            if short_id not in pending_notifications:
                pending_notifications[short_id] = {
                    'uid': uid,
                    'short_id': short_id,
                    'text': ev['summary'],
                    'time': dt,
                    'is_recurring': ev.get('is_recurring', False)
                }
                pending.append(pending_notifications[short_id])
    
    pending.sort(key=lambda x: x['time'])
    logger.info(f"Found {len(pending)} pending notifications")
    return pending

# ===== ОТПРАВКА СООБЩЕНИЙ =====

async def auto_delete_message(chat_id: int, msg_id: int, delay: int = 3600):
    await asyncio.sleep(delay)
    try: await bot.delete_message(chat_id, msg_id)
    except: pass

async def send_with_auto_delete(chat_id: int, text: str, parse_mode: str = 'Markdown', reply_markup=None, delay: int = 3600):
    msg = await bot.send_message(chat_id, text, parse_mode=parse_mode, reply_markup=reply_markup)
    asyncio.create_task(auto_delete_message(chat_id, msg.message_id, delay))
    return msg

async def send_persistent_message(chat_id: int, text: str, parse_mode: str = 'Markdown', reply_markup=None):
    return await bot.send_message(chat_id, text, parse_mode=parse_mode, reply_markup=reply_markup)

async def delete_user_message(msg, delay: int = 3600):
    asyncio.create_task(auto_delete_message(msg.chat.id, msg.message_id, delay))

# ===== ФОНОВЫЕ ЗАДАЧИ =====

async def check_pending():
    global notifications_enabled, last_notification_hour
    while True:
        if notifications_enabled:
            pending = await get_pending_notifications()
            current_hour = get_current_time().hour
            for p in pending:
                key = f"{p['short_id']}_{p['time'].strftime('%Y%m%d')}"
                last = last_notification_hour.get(key)
                if last != current_hour:
                    kb = InlineKeyboardMarkup(row_width=2)
                    kb.add(
                        InlineKeyboardButton("✅ Выполнено", callback_data=f"done_{p['short_id']}"),
                        InlineKeyboardButton("📅 Отложить", callback_data=f"snooze_{p['short_id']}")
                    )
                    await bot.send_message(
                        ADMIN_ID,
                        f"🔔 ПРОСРОЧЕНО: {p['text']}\n⏰ {p['time'].strftime('%H:%M')}",
                        reply_markup=kb
                    )
                    last_notification_hour[key] = current_hour
        await asyncio.sleep(60)

async def auto_cache_refresh():
    while True:
        await asyncio.sleep(300) # Обновляем кэш каждые 5 минут
        try:
            api = FreshCalDAVAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
            events = await api.get_fresh_events(days_ahead=2)
            events_cache[YANDEX_EMAIL] = {'data': events, 'timestamp': datetime.now().timestamp()}
            logger.info("Cache auto-refreshed")
        except Exception as e:
            logger.error(f"Auto-refresh error: {e}")

async def update_sync_time():
    global last_sync_time
    while True:
        last_sync_time = get_current_time()
        await asyncio.sleep(60)

# ===== УПРАВЛЕНИЕ СОБЫТИЯМИ =====

async def mark_done(short_id: str) -> bool:
    event_data = pending_notifications.get(short_id)
    if not event_data:
        return False
    
    api = FreshCalDAVAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
    uid = event_data['uid']
    
    try:
        if event_data.get('is_recurring'):
            # Для повторяющихся добавляем EXDATE
            success = await api.add_exdate_by_uid(uid, event_data['time'])
        else:
            # Для обычных удаляем
            success = await api.delete_event_by_uid(uid)
        
        if success:
            if short_id in pending_notifications:
                del pending_notifications[short_id]
            # Сбрасываем кэш событий, чтобы сразу увидеть изменения
            if YANDEX_EMAIL in events_cache:
                del events_cache[YANDEX_EMAIL]
        return success
    except Exception as e:
        logger.error(f"mark_done error: {e}")
        return False

async def snooze_event(short_id: str, hours: int = 1) -> bool:
    event_data = pending_notifications.get(short_id)
    if not event_data:
        return False
    
    api = FreshCalDAVAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
    new_start = event_data['time'] + timedelta(hours=hours)
    
    # Создаем новое событие
    try:
        await api.fetch_events_raw(new_start, new_start) # Dummy call to init session if needed
        # Используем стандартный метод создания через caldav для простоты, так как PUT сложнее
        client = caldav.DAVClient(url=YANDEX_CALDAV_URL, username=YANDEX_EMAIL, password=YANDEX_APP_PASSWORD)
        cal = client.principal().calendars()[0]
        end_time = new_start + timedelta(hours=1)
        tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
        if new_start.tzinfo is None: new_start = tz.localize(new_start)
        if end_time.tzinfo is None: end_time = tz.localize(end_time)
        
        event = cal.save_event(
            f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//MyUveBot//NONSGML v1.0//EN
BEGIN:VEVENT
UID:{uuid4()}@myuved.bot
DTSTAMP:{datetime.now(pytz.UTC).strftime('%Y%m%dT%H%M%SZ')}
DTSTART;TZID={config.get('timezone', 'Europe/Moscow')}:{new_start.strftime('%Y%m%dT%H%M%S')}
DTEND;TZID={config.get('timezone', 'Europe/Moscow')}:{end_time.strftime('%Y%m%dT%H%M%S')}
SUMMARY:{event_data['text']} (Отложено)
END:VEVENT
END:VCALENDAR"""
        )
        if short_id in pending_notifications:
            del pending_notifications[short_id]
        # Сброс кэша
        if YANDEX_EMAIL in events_cache:
            del events_cache[YANDEX_EMAIL]
        return True
    except Exception as e:
        logger.error(f"snooze error: {e}")
        return False

# ===== КОНФИГУРАЦИЯ И КЛАВИАТУРЫ =====

def init_config():
    if not os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump({'notifications_enabled': True, 'timezone': 'Europe/Moscow'}, f)

def load_config():
    global config, notifications_enabled
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
        notifications_enabled = config.get('notifications_enabled', True)
    except Exception:
        config = {'notifications_enabled': True, 'timezone': 'Europe/Moscow'}
        notifications_enabled = True

def save_config():
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

def get_main_keyboard():
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add(
        KeyboardButton("➕ Добавить"), KeyboardButton("📅 Календарь"),
        KeyboardButton("⚠️ Просроченные"), KeyboardButton("⚙️ Настройки")
    )
    return kb

class NotificationStates(StatesGroup):
    waiting_for_text = State()
    waiting_for_specific_date = State()

# ===== ОБРАБОТЧИКИ =====

@dp.message_handler(commands=['start'])
async def cmd_start(msg, state):
    await delete_user_message(msg); await state.finish()
    if ADMIN_ID and msg.from_user.id != ADMIN_ID: return await msg.reply("❌ Нет доступа")
    
    ok, msg_text = await check_caldav_connection()
    welcome = f"""👋 Добро пожаловать!
🤖 Версия v{BOT_VERSION}
📧 CalDAV: {'✅ Доступен' if ok else '❌ Ошибка'}
🌍 Часовой пояс: {config.get('timezone', 'Europe/Moscow')}

📌 Как это работает:
• Все уведомления берутся ТОЛЬКО из Яндекс.Календаря
• При отметке "Выполнено" событие удаляется из календаря
• Для повторяющихся событий удаляется только текущее вхождение"""
    
    await send_persistent_message(msg.chat.id, welcome)
    await send_persistent_message(msg.chat.id, "👋 Выберите действие:", reply_markup=get_main_keyboard())
    await show_calendar_events(msg.chat.id, persistent=True, force_refresh=True)
    await show_pending_list(msg.chat.id, persistent=True)

@dp.message_handler(lambda m: m.text == "📅 Календарь", state='*')
async def view_calendar(msg, state):
    await delete_user_message(msg); await state.finish()
    await show_calendar_events(msg.chat.id, persistent=True, force_refresh=True)

@dp.message_handler(lambda m: m.text == "⚠️ Просроченные", state='*')
async def view_pending(msg, state):
    await delete_user_message(msg); await state.finish()
    await show_pending_list(msg.chat.id, persistent=True)

@dp.callback_query_handler(lambda c: c.data == "refresh_calendar", state='*')
async def refresh_calendar(cb):
    await cb.answer("Обновляю...")
    await show_calendar_events(cb.from_user.id, persistent=True, force_refresh=True)

@dp.callback_query_handler(lambda c: c.data.startswith('done_'), state='*')
async def handle_done(cb):
    short_id = cb.data.replace('done_', '')
    await cb.answer("Удаляю...")
    success = await mark_done(short_id)
    if success:
        await bot.send_message(cb.from_user.id, "✅ Событие удалено!")
        await show_calendar_events(cb.from_user.id, persistent=True, force_refresh=True)
        await show_pending_list(cb.from_user.id, persistent=True)
    else:
        await bot.send_message(cb.from_user.id, "❌ Ошибка удаления.")

async def show_pending_list(chat_id: int, persistent: bool = False):
    pending = await get_pending_notifications()
    if not pending:
        await send_persistent_message(chat_id, "✅ Нет просроченных уведомлений!")
        return
    # ... (остальной код отображения списка аналогичен предыдущему)
    await send_persistent_message(chat_id, f"⚠️ ПРОСРОЧЕННЫЕ ({len(pending)}):\n" + "\n".join([f"- {p['time'].strftime('%H:%M')} {p['text']}" for p in pending]))

async def show_calendar_events(chat_id: int, persistent: bool = False, force_refresh: bool = False):
    text = await get_formatted_calendar_events(force_refresh=force_refresh)
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🔄 Обновить", callback_data="refresh_calendar"),
        InlineKeyboardButton("📥 Синхр.", callback_data="sync_calendar")
    )
    if persistent:
        await send_persistent_message(chat_id, text, reply_markup=kb)
    else:
        await send_with_auto_delete(chat_id, text, reply_markup=kb, delay=3600)

# ===== ЗАПУСК =====

async def on_startup(dp):
    init_config(); load_config()
    logger.info(f"🤖 БОТ v{BOT_VERSION} ЗАПУЩЕН")
    asyncio.create_task(check_pending())
    asyncio.create_task(auto_cache_refresh())
    asyncio.create_task(update_sync_time())

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)