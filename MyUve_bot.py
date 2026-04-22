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
    from dateutil.rrule import rrule as rrrule
    DATEUTIL_AVAILABLE = True
except ImportError:
    DATEUTIL_AVAILABLE = False
    print("WARNING: python-dateutil не установлен. Установите: pip install python-dateutil")

# Настройка логирования
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

BOT_VERSION = "5.4"
BOT_VERSION_DATE = "22.04.2026"

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
dp.middleware.setup(LoggingMiddleware())

CONFIG_FILE = 'config.json'

config: Dict = {}
notifications_enabled = True
calendar_events_cache: Dict[str, List[Dict]] = {}
last_sync_time: Optional[datetime] = None

# Хранилище времени последнего уведомления для каждого события
last_notification_time: Dict[str, datetime] = {}

TIMEZONES = {
    'Москва (UTC+3)': 'Europe/Moscow',
    'Калининград (UTC+2)': 'Europe/Kaliningrad',
    'Екатеринбург (UTC+5)': 'Asia/Yekaterinburg',
    'Новосибирск (UTC+7)': 'Asia/Novosibirsk',
    'Владивосток (UTC+10)': 'Asia/Vladivostok',
    'Камчатка (UTC+12)': 'Asia/Kamchatka'
}

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

class CalDAVCalendarAPI:
    def __init__(self, email, pwd):
        self.email = email
        self.pwd = pwd

    def get_calendar(self):
        try:
            client = caldav.DAVClient(url=YANDEX_CALDAV_URL, username=self.email, password=self.pwd)
            principal = client.principal()
            calendars = principal.calendars()
            return calendars[0] if calendars else None
        except Exception as e:
            logger.error(f"Get calendar error: {e}")
            return None

    async def create_event(self, summary, start_time):
        try:
            cal = self.get_calendar()
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
END:VEVENT
END:VCALENDAR"""
            event = cal.save_event(ical)
            return str(event.url) if event else None
        except Exception as e:
            logger.error(f"create_event error: {e}")
            return None

    async def delete_event(self, event_url):
        try:
            cal = self.get_calendar()
            if not cal:
                return False
            for event in cal.events():
                if str(event.url) == event_url:
                    event.delete()
                    logger.info(f"Событие удалено: {event_url}")
                    return True
            return False
        except Exception as e:
            logger.error(f"delete_event error: {e}")
            return False

    async def add_exception_to_recurring(self, event_url, exception_date):
        """Добавляет EXDATE к повторяющемуся событию для отмены конкретного вхождения"""
        try:
            cal = self.get_calendar()
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
            
            tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
            if exception_date.tzinfo is None:
                exception_date = tz.localize(exception_date)
            
            # Форматируем дату для EXDATE (без времени)
            exdate_str = exception_date.strftime('%Y%m%d')
            
            # Проверяем, есть ли уже EXDATE в событии
            if 'EXDATE' in ical_data:
                lines = ical_data.split('\n')
                new_lines = []
                for line in lines:
                    if line.startswith('EXDATE'):
                        current_exdates = line.replace('EXDATE:', '').replace('\r', '')
                        if exdate_str not in current_exdates:
                            new_lines.append(f'EXDATE:{current_exdates},{exdate_str}')
                        else:
                            new_lines.append(line)
                    else:
                        new_lines.append(line)
                ical_data = '\n'.join(new_lines)
            else:
                lines = ical_data.split('\n')
                new_lines = []
                for line in lines:
                    new_lines.append(line)
                    if line.startswith('RRULE'):
                        new_lines.append(f'EXDATE:{exdate_str}')
                ical_data = '\n'.join(new_lines)
            
            target_event.data = ical_data
            target_event.save()
            logger.info(f"Добавлено исключение для {event_url} на {exdate_str}")
            return True
        except Exception as e:
            logger.error(f"add_exception_to_recurring error: {e}")
            return False

    async def get_recurring_occurrences(self, event, start_date, end_date, tz, exdates):
        """Вычисляет вхождения повторяющегося события в заданном диапазоне"""
        try:
            vevent = event.vobject_instance.vevent
            
            if not hasattr(vevent, 'rrule') or vevent.rrule.value is None:
                return []
            
            # Получаем время начала
            dtstart = vevent.dtstart.value
            if hasattr(dtstart, 'dt'):
                dtstart = dtstart.dt
            
            if dtstart.tzinfo is None:
                dtstart = tz.localize(dtstart)
            
            # Получаем RRULE строку
            rrule_str = str(vevent.rrule.value)
            
            if not DATEUTIL_AVAILABLE:
                return []
            
            # Парсим RRULE
            rule = rrulestr(rrule_str, dtstart=dtstart)
            
            # Получаем все вхождения до end_date
            occurrences = []
            for occ in rule:
                # Приводим к часовому поясу
                if occ.tzinfo is None:
                    occ = tz.localize(occ)
                else:
                    occ = occ.astimezone(tz)
                
                # Проверяем, что вхождение в нашем диапазоне
                if occ < start_date:
                    continue
                if occ > end_date:
                    break
                
                # Проверяем, не исключено ли это вхождение
                occ_date_str = occ.strftime('%Y%m%d')
                if occ_date_str in exdates:
                    continue
                
                occurrences.append(occ)
            
            return occurrences
        except Exception as e:
            logger.error(f"get_recurring_occurrences error: {e}")
            return []

    async def get_events(self, from_date: datetime, to_date: datetime) -> List[Dict]:
        """Получает события из календаря за указанный период, включая вхождения повторяющихся"""
        try:
            cal = self.get_calendar()
            if not cal:
                return []
            
            tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
            if from_date.tzinfo is None:
                from_date = tz.localize(from_date)
            if to_date.tzinfo is None:
                to_date = tz.localize(to_date)
            
            # Получаем все события (не разворачивая повторяющиеся)
            all_events = cal.events()
            
            result = []
            
            for ev in all_events:
                try:
                    vevent = ev.vobject_instance.vevent
                    
                    # Получаем время начала
                    dtstart = vevent.dtstart.value
                    if hasattr(dtstart, 'dt'):
                        dtstart = dtstart.dt
                    
                    # Приводим к часовому поясу
                    if dtstart.tzinfo is None:
                        dtstart = tz.localize(dtstart)
                    else:
                        dtstart = dtstart.astimezone(tz)
                    
                    # Проверяем, является ли событие повторяющимся
                    is_recurring = hasattr(vevent, 'rrule') and vevent.rrule.value is not None
                    
                    # Получаем EXDATE если есть
                    exdates = []
                    if hasattr(vevent, 'exdate'):
                        for exdate in vevent.exdate:
                            exdate_val = str(exdate.value)
                            # Извлекаем только дату
                            if len(exdate_val) >= 8:
                                exdates.append(exdate_val[:8])
                    
                    summary = str(vevent.summary.value) if hasattr(vevent, 'summary') else 'Без названия'
                    event_url = str(ev.url)
                    
                    if is_recurring:
                        # Для повторяющихся событий вычисляем вхождения в нужном диапазоне
                        occurrences = await self.get_recurring_occurrences(ev, from_date, to_date, tz, exdates)
                        for occ_time in occurrences:
                            # Сохраняем информацию о родительском событии
                            result.append({
                                'url': event_url,
                                'summary': summary,
                                'start': occ_time,
                                'is_recurring': True,
                                'parent_start': dtstart,
                                'rrule': str(vevent.rrule.value) if hasattr(vevent, 'rrule') else None
                            })
                    else:
                        # Обычное событие
                        if from_date <= dtstart <= to_date:
                            result.append({
                                'url': event_url,
                                'summary': summary,
                                'start': dtstart,
                                'is_recurring': False
                            })
                except Exception as e:
                    logger.error(f"parse event error: {e}")
                    continue
            
            # Сортируем по времени
            result.sort(key=lambda x: x['start'])
            
            # Дедупликация на случай дублей
            unique_result = []
            seen_keys = set()
            for ev in result:
                key = f"{ev['url']}_{ev['start'].strftime('%Y%m%d%H%M')}"
                if key not in seen_keys:
                    seen_keys.add(key)
                    unique_result.append(ev)
            
            return unique_result
        except Exception as e:
            logger.error(f"get_events error: {e}")
            return []

def get_caldav_available():
    return bool(YANDEX_EMAIL and YANDEX_APP_PASSWORD)

async def check_caldav_connection():
    if not get_caldav_available():
        return False, "CalDAV не настроен"
    api = CalDAVCalendarAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
    cal = api.get_calendar()
    if cal:
        return True, "Подключено"
    return False, "Ошибка"

async def update_calendar_cache():
    """Обновляет кэш календаря на 60 дней вперед"""
    global calendar_events_cache, last_sync_time
    now = get_current_time()
    api = CalDAVCalendarAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
    
    # Получаем события на 60 дней вперед
    end_date = now + timedelta(days=60)
    
    events = await api.get_events(now, end_date)
    
    calendar_events_cache['all'] = events
    last_sync_time = now
    logger.info(f"Обновлён кэш календаря: {len(events)} событий")
    return events

async def get_pending_notifications() -> List[Dict]:
    """Получает просроченные уведомления (время которых уже прошло)"""
    now = get_current_time()
    await update_calendar_cache()
    events = calendar_events_cache.get('all', [])
    
    pending = []
    for ev in events:
        # Событие просрочено, если его время начала <= текущего времени
        if ev['start'] <= now:
            # Генерируем короткий ID для callback
            unique_key = f"{ev['url']}_{ev['start'].strftime('%Y%m%d%H%M')}"
            short_id = hashlib.md5(unique_key.encode()).hexdigest()[:12]
            
            pending.append({
                'url': ev['url'],
                'short_id': short_id,
                'text': ev['summary'],
                'time': ev['start'],
                'is_recurring': ev['is_recurring']
            })
    
    # Сортируем по времени
    pending.sort(key=lambda x: x['time'])
    logger.info(f"Найдено просроченных событий: {len(pending)}")
    return pending

async def get_upcoming_events() -> List[Tuple[datetime, Dict]]:
    """Получает предстоящие события для отображения (на сегодня и завтра)"""
    now = get_current_time()
    await update_calendar_cache()
    events = calendar_events_cache.get('all', [])
    
    today = now.date()
    tomorrow = today + timedelta(days=1)
    day_after_tomorrow = tomorrow + timedelta(days=1)
    
    future = []
    for ev in events:
        dt = ev['start']
        event_date = dt.date()
        
        # Показываем события на сегодня и завтра
        if event_date == today or event_date == tomorrow:
            future.append((dt, ev))
    
    future.sort(key=lambda x: x[0])
    return future

async def get_formatted_calendar_events():
    """Форматирует события календаря для отображения (только сегодня и завтра)"""
    future = await get_upcoming_events()
    
    if not future:
        return "📅 **Нет событий на сегодня и завтра**\n\nВсе заметки из календаря будут отображаться здесь."
    
    now = get_current_time()
    today = now.date()
    tomorrow = today + timedelta(days=1)
    
    text = "📅 **СОБЫТИЯ НА СЕГОДНЯ И ЗАВТРА**\n\n"
    
    # Группируем события по датам
    today_events = []
    tomorrow_events = []
    
    for dt, ev in future:
        if dt.date() == today:
            today_events.append((dt, ev))
        elif dt.date() == tomorrow:
            tomorrow_events.append((dt, ev))
    
    # Отображаем события на сегодня
    if today_events:
        text += f"🔴 **СЕГОДНЯ**\n"
        for dt, ev in today_events:
            time_str = dt.strftime('%H:%M')
            recurring_mark = " 🔁" if ev['is_recurring'] else ""
            text += f"   • {time_str} — **{ev['summary']}**{recurring_mark}\n"
        text += "\n"
    
    # Отображаем события на завтра
    if tomorrow_events:
        text += f"🟠 **ЗАВТРА**\n"
        for dt, ev in tomorrow_events:
            time_str = dt.strftime('%H:%M')
            recurring_mark = " 🔁" if ev['is_recurring'] else ""
            text += f"   • {time_str} — **{ev['summary']}**{recurring_mark}\n"
        text += "\n"
    
    # Добавляем информацию о других событиях (если есть)
    all_events = calendar_events_cache.get('all', [])
    future_events = [ev for ev in all_events if ev['start'].date() > tomorrow and ev['start'] >= now]
    if future_events:
        text += f"📌 **А также есть события на другие дни**\n"
        text += f"   • Всего событий в календаре: {len(all_events)}\n"
        text += f"   • Из них предстоит: {len([e for e in all_events if e['start'] >= now])}\n"
        text += f"\n💡 *Нажмите кнопку \"Все события\" для полного списка*"
    
    if last_sync_time:
        text += f"\n\n🔄 *Последняя синхронизация:* {last_sync_time.strftime('%d.%m.%Y %H:%M:%S')}"
    return text

async def show_calendar_events(chat_id, persistent=False):
    text = await get_formatted_calendar_events()
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🔄 Обновить", callback_data="refresh_calendar"),
        InlineKeyboardButton("📥 Синхр.", callback_data="sync_calendar"),
        InlineKeyboardButton("📋 Все события", callback_data="all_events")
    )
    if persistent:
        await send_persistent_message(chat_id, text, reply_markup=kb)
    else:
        await send_with_auto_delete(chat_id, text, reply_markup=kb, delay=3600)

async def show_all_events(chat_id, persistent=False):
    """Показывает все события календаря (без ограничения по датам)"""
    await update_calendar_cache()
    events = calendar_events_cache.get('all', [])
    now = get_current_time()
    
    if not events:
        text = "📅 **В календаре нет событий**"
    else:
        # Группируем по датам
        events_by_date = {}
        for ev in events:
            date_key = ev['start'].date()
            if date_key not in events_by_date:
                events_by_date[date_key] = []
            events_by_date[date_key].append(ev)
        
        text = "📅 **ВСЕ СОБЫТИЯ КАЛЕНДАРЯ**\n\n"
        today = now.date()
        
        # Показываем только будущие события (включая сегодня)
        for date_key in sorted(events_by_date.keys()):
            if date_key < today:
                continue  # Пропускаем прошедшие даты
            
            if date_key == today:
                prefix = "🔴 "
            elif date_key == today + timedelta(days=1):
                prefix = "🟠 "
            else:
                prefix = "📌 "
            
            weekday = ["пн","вт","ср","чт","пт","сб","вс"][date_key.weekday()]
            text += f"{prefix}**{date_key.day:02d}.{date_key.month:02d}.{date_key.year}** ({weekday})\n"
            
            for ev in events_by_date[date_key]:
                time_str = ev['start'].strftime('%H:%M')
                recurring_mark = " 🔁" if ev['is_recurring'] else ""
                text += f"   • {time_str} — **{ev['summary']}**{recurring_mark}\n"
            text += "\n"
        
        if len(text) > 4000:
            text = text[:3500] + "\n\n... и ещё события"
    
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("◀️ Назад к календарю", callback_data="back_to_calendar"))
    
    if persistent:
        await send_persistent_message(chat_id, text, reply_markup=kb)
    else:
        await send_with_auto_delete(chat_id, text, reply_markup=kb, delay=3600)

class NotificationStates(StatesGroup):
    waiting_for_text = State()
    waiting_for_specific_date = State()

def init_config():
    if not os.path.exists(CONFIG_FILE):
        default_config = {
            'notifications_enabled': True,
            'timezone': 'Europe/Moscow'
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

async def show_pending_actions(chat_id, short_id, text, event_time, is_recurring=False):
    """Отправляет уведомление о просроченном событии с кнопками действий"""
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Выполнено", callback_data=f"done_{short_id}"),
        InlineKeyboardButton("📅 Отложить", callback_data=f"snooze_{short_id}"),
        InlineKeyboardButton("❌ Отложить на час", callback_data=f"hour_{short_id}")
    )
    recurring_text = " 🔁" if is_recurring else ""
    
    # Определяем, насколько просрочено событие
    now = get_current_time()
    minutes_late = int((now - event_time).total_seconds() / 60)
    late_text = f"\n⚠️ Просрочено на {minutes_late} мин." if minutes_late > 0 else ""
    
    await bot.send_message(
        chat_id,
        f"🔔 **ПРОСРОЧЕННОЕ НАПОМИНАНИЕ!**{recurring_text}\n\n"
        f"📝 {text}\n"
        f"⏰ {event_time.strftime('%d.%m.%Y %H:%M')}{late_text}\n\n"
        f"❗️ Время истекло! Выберите действие:",
        reply_markup=kb,
        parse_mode='Markdown'
    )

async def check_pending():
    """Проверяет просроченные события и отправляет уведомления каждый час"""
    global notifications_enabled, last_notification_time
    
    while True:
        if notifications_enabled:
            pending = await get_pending_notifications()
            now = get_current_time()
            
            for p in pending:
                # Создаем уникальный ключ для события
                event_key = f"{p['short_id']}_{p['time'].strftime('%Y%m%d%H%M')}"
                
                # Проверяем, когда последний раз отправляли уведомление
                last_time = last_notification_time.get(event_key)
                
                # Отправляем уведомление, если:
                # 1. Уведомление еще не отправляли, или
                # 2. Прошло больше часа с последнего уведомления
                if last_time is None or (now - last_time) >= timedelta(hours=1):
                    await show_pending_actions(ADMIN_ID, p['short_id'], p['text'], p['time'], p['is_recurring'])
                    last_notification_time[event_key] = now
                    logger.info(f"Отправлено уведомление для события: {p['text']} в {p['time']}")
                    
                    # Удаляем старые записи (старше 24 часов)
                    to_remove = []
                    for key, value in last_notification_time.items():
                        if (now - value) > timedelta(hours=24):
                            to_remove.append(key)
                    for key in to_remove:
                        del last_notification_time[key]
        
        await asyncio.sleep(60)  # Проверяем каждую минуту

async def snooze_event(short_id, hours=1):
    """Откладывает событие на указанное количество часов"""
    try:
        pending = await get_pending_notifications()
        event_url = None
        event_time = None
        for p in pending:
            if p['short_id'] == short_id:
                event_url = p['url']
                event_time = p['time']
                break
        
        if not event_url:
            logger.error(f"Событие не найдено для short_id: {short_id}")
            return False
        
        api = CalDAVCalendarAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
        cal = api.get_calendar()
        if not cal:
            return False
        
        new_start = event_time + timedelta(hours=hours)
        new_end = new_start + timedelta(hours=1)
        
        tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
        if new_start.tzinfo is None:
            new_start = tz.localize(new_start)
        if new_end.tzinfo is None:
            new_end = tz.localize(new_end)
        
        # Удаляем старое событие
        for event in cal.events():
            if str(event.url) == event_url:
                old_vevent = event.vobject_instance.vevent
                summary = str(old_vevent.summary.value) if hasattr(old_vevent, 'summary') else 'Без названия'
                is_recurring = hasattr(old_vevent, 'rrule') and old_vevent.rrule.value is not None
                
                # Удаляем старое событие
                event.delete()
                
                if is_recurring:
                    # Для повторяющихся событий создаем новое с EXDATE для пропущенного
                    tzid = config.get('timezone', 'Europe/Moscow')
                    uid = f"{uuid4()}@myuved.bot"
                    
                    # Получаем RRULE из старого события
                    rrule_str = str(old_vevent.rrule.value) if hasattr(old_vevent, 'rrule') else None
                    
                    ical = f"""BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:{uid}
DTSTAMP:{datetime.now().strftime('%Y%m%dT%H%M%SZ')}
DTSTART;TZID={tzid}:{new_start.strftime('%Y%m%dT%H%M%S')}
DTEND;TZID={tzid}:{new_end.strftime('%Y%m%dT%H%M%S')}
SUMMARY:{summary[:255]}
"""
                    if rrule_str:
                        ical += f"RRULE:{rrule_str}\n"
                    
                    # Добавляем EXDATE для пропущенного вхождения
                    exdate_str = event_time.strftime('%Y%m%d')
                    ical += f"EXDATE:{exdate_str}\n"
                    
                    ical += """END:VEVENT
END:VCALENDAR"""
                    
                    cal.save_event(ical)
                else:
                    # Для обычных событий просто создаем новое
                    tzid = config.get('timezone', 'Europe/Moscow')
                    uid = f"{uuid4()}@myuved.bot"
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
                    cal.save_event(ical)
                
                logger.info(f"Событие отложено на {hours} часов: {summary}")
                return True
        
        return False
    except Exception as e:
        logger.error(f"snooze_event error: {e}")
        return False

async def mark_done(short_id, is_recurring=False, event_time=None):
    """Отмечает событие как выполненное"""
    try:
        pending = await get_pending_notifications()
        event_url = None
        for p in pending:
            if p['short_id'] == short_id:
                event_url = p['url']
                is_recurring = p['is_recurring']
                event_time = p['time']
                break
        
        if not event_url:
            logger.error(f"Событие не найдено для short_id: {short_id}")
            return False
        
        api = CalDAVCalendarAPI(YANDEX_EMAIL, YANDEX_APP_PASSWORD)
        
        if is_recurring and event_time:
            # Для повторяющихся событий добавляем исключение (только для текущего вхождения)
            logger.info(f"Добавление исключения для повторяющегося события: {event_url}")
            return await api.add_exception_to_recurring(event_url, event_time)
        else:
            # Для обычных событий удаляем полностью
            logger.info(f"Удаление обычного события: {event_url}")
            return await api.delete_event(event_url)
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
    """Показывает список всех просроченных событий"""
    pending = await get_pending_notifications()
    
    if not pending:
        msg = "✅ **Нет просроченных уведомлений!**\n\nВсе напоминания выполнены или ещё не наступили."
        await send_persistent_message(chat_id, msg)
        return
    
    await send_persistent_message(chat_id, f"⚠️ **ПРОСРОЧЕННЫЕ УВЕДОМЛЕНИЯ** ({len(pending)} шт.)\n\n")
    
    for idx, p in enumerate(pending, 1):
        recurring_mark = " 🔁" if p['is_recurring'] else ""
        
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("✅ Выполнено", callback_data=f"done_{p['short_id']}"),
            InlineKeyboardButton("📅 Отложить", callback_data=f"snooze_{p['short_id']}"),
            InlineKeyboardButton("❌ Отложить на час", callback_data=f"hour_{p['short_id']}")
        )
        
        text = f"⚠️ **{idx}. {p['text']}**{recurring_mark}\n⏰ {p['time'].strftime('%d.%m.%Y %H:%M')}\n\nВыберите действие:"
        
        await send_persistent_message(chat_id, text, reply_markup=kb)

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

📅 **В календаре показываются события на сегодня и завтра**
   (кнопка "Все события" покажет полный список)"""
    
    await send_persistent_message(msg.chat.id, welcome)
    await send_persistent_message(msg.chat.id, "👋 **Выберите действие:**", reply_markup=get_main_keyboard())
    
    await update_calendar_cache()
    await show_calendar_events(msg.chat.id, persistent=True)
    await show_pending_list(msg.chat.id, persistent=True)

@dp.message_handler(commands=['menu'])
async def show_menu(msg):
    await delete_user_message(msg)
    await send_persistent_message(msg.chat.id, "👋 **Главное меню:**", reply_markup=get_main_keyboard())
    await update_calendar_cache()
    await show_calendar_events(msg.chat.id, persistent=True)
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
    ev_id = await api.create_event(data['text'], dt)
    
    if ev_id:
        await send_with_auto_delete(msg.chat.id, f"✅ **Уведомление создано!**\n📝 {data['text']}\n⏰ {dt.strftime('%d.%m.%Y %H:%M')}", delay=3600)
        await update_calendar_cache()
        await show_calendar_events(msg.chat.id, persistent=True)
        await show_pending_list(msg.chat.id, persistent=True)
    else:
        await send_with_auto_delete(msg.chat.id, "❌ **Ошибка при создании уведомления!**", delay=3600)
    
    await state.finish()

# ---------- ОБРАБОТЧИКИ ДЛЯ ПРОСРОЧЕННЫХ ----------
@dp.callback_query_handler(lambda c: c.data and c.data.startswith('done_'), state='*')
async def handle_done(cb):
    short_id = cb.data.replace('done_', '')
    logger.info(f"Обработка done для short_id: {short_id}")
    
    success = await mark_done(short_id)
    
    if success:
        try:
            await cb.message.delete()
        except:
            pass
        await cb.answer("✅ Событие отмечено как выполненное и удалено из календаря!")
        await update_calendar_cache()
        await show_calendar_events(cb.from_user.id, persistent=True)
        await show_pending_list(cb.from_user.id, persistent=True)
    else:
        await cb.answer("❌ Ошибка при удалении события! Попробуйте позже.", show_alert=True)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('snooze_') and not c.data.startswith(('snooze_1h_', 'snooze_3h_', 'snooze_1d_', 'snooze_7d_')), state='*')
async def handle_snooze(cb):
    short_id = cb.data.replace('snooze_', '')
    
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("1 час", callback_data=f"snooze_1h_{short_id}"),
        InlineKeyboardButton("3 часа", callback_data=f"snooze_3h_{short_id}"),
        InlineKeyboardButton("1 день", callback_data=f"snooze_1d_{short_id}"),
        InlineKeyboardButton("7 дней", callback_data=f"snooze_7d_{short_id}"),
        InlineKeyboardButton("◀️ Назад", callback_data="back_to_pending")
    )
    await cb.message.edit_text("⏰ **На сколько отложить?**", reply_markup=kb)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('snooze_1h_'), state='*')
async def snooze_1h(cb):
    short_id = cb.data.replace('snooze_1h_', '')
    await process_snooze(cb, short_id, 1)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('snooze_3h_'), state='*')
async def snooze_3h(cb):
    short_id = cb.data.replace('snooze_3h_', '')
    await process_snooze(cb, short_id, 3)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('snooze_1d_'), state='*')
async def snooze_1d(cb):
    short_id = cb.data.replace('snooze_1d_', '')
    await process_snooze(cb, short_id, 24)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('snooze_7d_'), state='*')
async def snooze_7d(cb):
    short_id = cb.data.replace('snooze_7d_', '')
    await process_snooze(cb, short_id, 168)

@dp.callback_query_handler(lambda c: c.data == "back_to_pending", state='*')
async def back_to_pending(cb):
    await show_pending_list(cb.from_user.id, persistent=True)
    await cb.answer()

async def process_snooze(cb, short_id, hours):
    success = await snooze_event(short_id, hours)
    if success:
        try:
            await cb.message.delete()
        except:
            pass
        await cb.answer(f"✅ Событие отложено на {hours} час(ов)!")
        await update_calendar_cache()
        await show_calendar_events(cb.from_user.id, persistent=True)
        await show_pending_list(cb.from_user.id, persistent=True)
    else:
        await cb.answer("❌ Ошибка при откладывании события!", show_alert=True)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith('hour_'), state='*')
async def handle_hour(cb):
    short_id = cb.data.replace('hour_', '')
    await process_snooze(cb, short_id, 1)

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
    await show_calendar_events(msg.chat.id, persistent=True)

@dp.callback_query_handler(lambda c: c.data == "refresh_calendar", state='*')
async def refresh_calendar(cb):
    await update_calendar_cache()
    await show_calendar_events(cb.from_user.id, persistent=True)
    await cb.answer("✅ Календарь обновлён")

@dp.callback_query_handler(lambda c: c.data == "sync_calendar", state='*')
async def sync_calendar(cb):
    await cb.message.edit_text("🔄 **Синхронизация с календарём...**")
    await update_calendar_cache()
    await show_calendar_events(cb.from_user.id, persistent=True)
    await cb.answer("✅ Синхронизация завершена")

@dp.callback_query_handler(lambda c: c.data == "all_events", state='*')
async def handle_all_events(cb):
    await show_all_events(cb.from_user.id, persistent=True)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data == "back_to_calendar", state='*')
async def back_to_calendar(cb):
    await show_calendar_events(cb.from_user.id, persistent=True)
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
        await update_calendar_cache()
        await show_calendar_events(cb.from_user.id, persistent=True)
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
    """Автоматически обновляет кэш календаря каждые 15 минут"""
    while True:
        try:
            await update_calendar_cache()
            logger.info("Автообновление календаря выполнено")
        except Exception as e:
            logger.error(f"auto_update error: {e}")
        await asyncio.sleep(900)  # 15 минут

async def on_startup(dp):
    init_config()
    load_config()
    
    # Удаляем старый файл если есть
    old_data_file = 'notifications.json'
    if os.path.exists(old_data_file):
        os.remove(old_data_file)
        logger.info("Удалён старый файл notifications.json")
    
    calendar_events_cache.clear()
    
    logger.info(f"\n{'='*50}\n🤖 БОТ v{BOT_VERSION} ЗАПУЩЕН\n{'='*50}")
    if get_caldav_available():
        ok, msg = await check_caldav_connection()
        logger.info(f"CalDAV: {'✅' if ok else '❌'} {msg}")
    logger.info(f"Часовой пояс: {config.get('timezone', 'Europe/Moscow')}")
    logger.info(f"Текущее время: {get_current_time().strftime('%d.%m.%Y %H:%M:%S')}")
    logger.info(f"Логирование настроено: файл {LOG_FILE}, макс. размер {LOG_MAX_BYTES} байт")
    logger.info(f"{'='*50}\n")
    
    # Запускаем фоновые задачи
    asyncio.create_task(check_pending())
    asyncio.create_task(auto_update_cache())
    logger.info("✅ Бот готов")

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)