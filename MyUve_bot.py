import asyncio
import json
import os
import logging
import logging.handlers
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pytz
import hashlib
from uuid import uuid4
import aiohttp
from urllib.parse import urlencode

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
from dotenv import load_dotenv, set_key

load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN')
ADMIN_ID = int(os.getenv('ADMIN_ID')) if os.getenv('ADMIN_ID') else None
YANDEX_CLIENT_ID = os.getenv('YANDEX_CLIENT_ID')
YANDEX_CLIENT_SECRET = os.getenv('YANDEX_CLIENT_SECRET')
YANDEX_API_TOKEN = os.getenv('YANDEX_API_TOKEN')

BOT_VERSION = "5.9"
BOT_VERSION_DATE = "22.04.2026"

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
dp.middleware.setup(LoggingMiddleware())

CONFIG_FILE = 'config.json'
ENV_FILE = '.env'

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

YANDEX_API_BASE = "https://api.calendar.yandex.net/v1"
YANDEX_OAUTH_AUTH_URL = "https://oauth.yandex.ru/authorize"
YANDEX_OAUTH_TOKEN_URL = "https://oauth.yandex.ru/token"

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

class YandexCalendarAPI:
    def __init__(self, token: str):
        self.token = token
        self.calendar_id = "primary"
    
    def _get_headers(self):
        return {
            "Authorization": f"OAuth {self.token}",
            "Content-Type": "application/json"
        }
    
    async def create_event(self, summary: str, start_time: datetime) -> Optional[str]:
        """Создает новое событие в календаре"""
        try:
            tz = config.get('timezone', 'Europe/Moscow')
            end_time = start_time + timedelta(hours=1)
            
            event_data = {
                "summary": summary[:255],
                "start": {
                    "dateTime": start_time.isoformat(),
                    "timeZone": tz
                },
                "end": {
                    "dateTime": end_time.isoformat(),
                    "timeZone": tz
                }
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{YANDEX_API_BASE}/calendars/{self.calendar_id}/events",
                    headers=self._get_headers(),
                    json=event_data
                ) as resp:
                    if resp.status == 201:
                        data = await resp.json()
                        logger.info(f"Создано событие: {data.get('id')}")
                        return data.get('id')
                    else:
                        text = await resp.text()
                        logger.error(f"Create event error: {resp.status} - {text}")
                        return None
        except Exception as e:
            logger.error(f"create_event error: {e}")
            return None
    
    async def delete_event_instance(self, event_id: str, event_time: datetime) -> bool:
        """
        Удаляет конкретное вхождение повторяющегося события.
        Для обычных событий удаляет всё событие.
        """
        try:
            async with aiohttp.ClientSession() as session:
                # Сначала получаем информацию о событии
                async with session.get(
                    f"{YANDEX_API_BASE}/calendars/{self.calendar_id}/events/{event_id}",
                    headers=self._get_headers()
                ) as resp:
                    if resp.status != 200:
                        logger.error(f"Get event info error: {resp.status}")
                        return False
                    event_data = await resp.json()
                
                # Проверяем, является ли событие повторяющимся
                is_recurring = 'recurrence' in event_data and event_data.get('recurrence')
                
                if not is_recurring:
                    # Обычное событие - удаляем полностью
                    async with session.delete(
                        f"{YANDEX_API_BASE}/calendars/{self.calendar_id}/events/{event_id}",
                        headers=self._get_headers()
                    ) as resp:
                        if resp.status == 204:
                            logger.info(f"Обычное событие удалено: {event_id}")
                            return True
                        else:
                            logger.error(f"Delete event error: {resp.status}")
                            return False
                else:
                    # Повторяющееся событие - нужно удалить только конкретное вхождение
                    event_date = event_time.strftime('%Y-%m-%d')
                    
                    async with session.get(
                        f"{YANDEX_API_BASE}/calendars/{self.calendar_id}/events/{event_id}/instances",
                        headers=self._get_headers()
                    ) as resp:
                        if resp.status != 200:
                            logger.error(f"Get instances error: {resp.status}")
                            return False
                        instances_data = await resp.json()
                    
                    # Ищем экземпляр на нужную дату
                    instance_id = None
                    for instance in instances_data.get('items', []):
                        start = instance.get('start', {})
                        instance_date = start.get('date') or start.get('dateTime', '')[:10]
                        if instance_date == event_date:
                            instance_id = instance.get('id')
                            break
                    
                    if not instance_id:
                        logger.error(f"Instance not found for date {event_date}")
                        return False
                    
                    # Удаляем экземпляр
                    async with session.delete(
                        f"{YANDEX_API_BASE}/calendars/{self.calendar_id}/events/{instance_id}",
                        headers=self._get_headers()
                    ) as resp:
                        if resp.status == 204:
                            logger.info(f"Вхождение повторяющегося события удалено: {instance_id}")
                            return True
                        else:
                            logger.error(f"Delete instance error: {resp.status}")
                            return False
        except Exception as e:
            logger.error(f"delete_event_instance error: {e}")
            return False
    
    async def get_events(self, from_date: datetime, to_date: datetime) -> List[Dict]:
        """Получает события из календаря за указанный период"""
        try:
            from_utc = from_date.astimezone(pytz.UTC)
            to_utc = to_date.astimezone(pytz.UTC)
            
            params = {
                "timeMin": from_utc.isoformat().replace('+00:00', 'Z'),
                "timeMax": to_utc.isoformat().replace('+00:00', 'Z'),
                "singleEvents": "true",
                "orderBy": "startTime"
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{YANDEX_API_BASE}/calendars/{self.calendar_id}/events",
                    headers=self._get_headers(),
                    params=params
                ) as resp:
                    if resp.status != 200:
                        logger.error(f"Get events error: {resp.status}")
                        return []
                    
                    data = await resp.json()
                    events = []
                    tz = pytz.timezone(config.get('timezone', 'Europe/Moscow'))
                    
                    for item in data.get('items', []):
                        try:
                            start_data = item.get('start', {})
                            start_str = start_data.get('dateTime') or start_data.get('date')
                            
                            if not start_str:
                                continue
                            
                            if 'T' in start_str:
                                dt = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
                            else:
                                dt = datetime.fromisoformat(start_str)
                                dt = tz.localize(dt)
                            
                            if dt.tzinfo is None:
                                dt = pytz.UTC.localize(dt)
                            dt = dt.astimezone(tz)
                            
                            is_recurring = 'recurrence' in item and item.get('recurrence')
                            
                            events.append({
                                'id': item.get('id'),
                                'summary': item.get('summary', 'Без названия'),
                                'start': dt,
                                'is_recurring': bool(is_recurring),
                                'recurring_event_id': item.get('recurringEventId')
                            })
                        except Exception as e:
                            logger.error(f"Parse event error: {e}")
                            continue
                    
                    return events
        except Exception as e:
            logger.error(f"get_events error: {e}")
            return []

def get_yandex_api_available():
    return bool(YANDEX_API_TOKEN)

async def check_yandex_connection():
    if not get_yandex_api_available():
        return False, "API не настроен (нужен OAuth токен)"
    api = YandexCalendarAPI(YANDEX_API_TOKEN)
    try:
        events = await api.get_events(get_current_time(), get_current_time() + timedelta(days=1))
        return True, "Подключено"
    except Exception as e:
        return False, f"Ошибка: {e}"

async def update_calendar_cache():
    """Обновляет кэш календаря на 60 дней вперед"""
    global calendar_events_cache, last_sync_time
    now = get_current_time()
    
    if not get_yandex_api_available():
        logger.warning("Yandex API не настроен")
        calendar_events_cache['all'] = []
        last_sync_time = now
        return []
    
    api = YandexCalendarAPI(YANDEX_API_TOKEN)
    end_date = now + timedelta(days=60)
    events = await api.get_events(now, end_date)
    
    calendar_events_cache['all'] = events
    last_sync_time = now
    logger.info(f"Обновлён кэш календаря: {len(events)} событий")
    return events

async def get_pending_notifications() -> List[Dict]:
    """Получает просроченные уведомления"""
    now = get_current_time()
    await update_calendar_cache()
    events = calendar_events_cache.get('all', [])
    
    pending = []
    for ev in events:
        if ev['start'] <= now:
            unique_key = f"{ev['id']}_{ev['start'].strftime('%Y%m%d%H%M')}"
            short_id = hashlib.md5(unique_key.encode()).hexdigest()[:12]
            
            pending.append({
                'id': ev['id'],
                'short_id': short_id,
                'text': ev['summary'],
                'time': ev['start'],
                'is_recurring': ev.get('is_recurring', False)
            })
    
    pending.sort(key=lambda x: x['time'])
    logger.info(f"Найдено просроченных событий: {len(pending)}")
    return pending

async def get_today_tomorrow_events() -> List[Tuple[datetime, Dict]]:
    """Получает события на сегодня и завтра"""
    now = get_current_time()
    await update_calendar_cache()
    events = calendar_events_cache.get('all', [])
    
    today = now.date()
    tomorrow = today + timedelta(days=1)
    
    result = []
    for ev in events:
        dt = ev['start']
        event_date = dt.date()
        
        if event_date == today or event_date == tomorrow:
            result.append((dt, ev))
    
    result.sort(key=lambda x: x[0])
    return result

async def get_formatted_calendar_events():
    """Форматирует события календаря для отображения"""
    events = await get_today_tomorrow_events()
    
    if not events:
        if not get_yandex_api_available():
            return "📅 **Требуется настройка API**\n\nНажмите кнопку \"Настройки\" и получите OAuth токен."
        return "📅 **Нет событий на сегодня и завтра**"
    
    now = get_current_time()
    today = now.date()
    tomorrow = today + timedelta(days=1)
    
    text = "📅 **СОБЫТИЯ НА СЕГОДНЯ И ЗАВТРА**\n\n"
    
    today_events = []
    tomorrow_events = []
    
    for dt, ev in events:
        if dt.date() == today:
            today_events.append((dt, ev))
        elif dt.date() == tomorrow:
            tomorrow_events.append((dt, ev))
    
    if today_events:
        text += f"🔴 **СЕГОДНЯ**\n"
        for dt, ev in today_events:
            time_str = dt.strftime('%H:%M')
            recurring_mark = " 🔁" if ev.get('is_recurring', False) else ""
            text += f"   • {time_str} — **{ev['summary']}**{recurring_mark}\n"
        text += "\n"
    
    if tomorrow_events:
        text += f"🟠 **ЗАВТРА**\n"
        for dt, ev in tomorrow_events:
            time_str = dt.strftime('%H:%M')
            recurring_mark = " 🔁" if ev.get('is_recurring', False) else ""
            text += f"   • {time_str} — **{ev['summary']}**{recurring_mark}\n"
        text += "\n"
    
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
    """Показывает все события календаря"""
    await update_calendar_cache()
    events = calendar_events_cache.get('all', [])
    now = get_current_time()
    
    if not events:
        text = "📅 **В календаре нет событий**"
    else:
        events_by_date = {}
        for ev in events:
            if ev['start'] < now:
                continue
            date_key = ev['start'].date()
            if date_key not in events_by_date:
                events_by_date[date_key] = []
            events_by_date[date_key].append(ev)
        
        text = "📅 **ВСЕ СОБЫТИЯ КАЛЕНДАРЯ**\n\n"
        today = now.date()
        
        for date_key in sorted(events_by_date.keys()):
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
                recurring_mark = " 🔁" if ev.get('is_recurring', False) else ""
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

class TokenSetupStates(StatesGroup):
    waiting_for_confirmation = State()

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
    """Отправляет уведомление о просроченном событии"""
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Выполнено", callback_data=f"done_{short_id}"),
        InlineKeyboardButton("📅 Отложить", callback_data=f"snooze_{short_id}"),
        InlineKeyboardButton("❌ Отложить на час", callback_data=f"hour_{short_id}")
    )
    recurring_text = " 🔁" if is_recurring else ""
    
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
        if notifications_enabled and get_yandex_api_available():
            pending = await get_pending_notifications()
            now = get_current_time()
            
            for p in pending:
                event_key = f"{p['short_id']}_{p['time'].strftime('%Y%m%d%H%M')}"
                last_time = last_notification_time.get(event_key)
                
                if last_time is None or (now - last_time) >= timedelta(hours=1):
                    await show_pending_actions(ADMIN_ID, p['short_id'], p['text'], p['time'], p['is_recurring'])
                    last_notification_time[event_key] = now
                    logger.info(f"Отправлено уведомление для события: {p['text']}")
                    
                    to_remove = []
                    for key, value in last_notification_time.items():
                        if (now - value) > timedelta(hours=24):
                            to_remove.append(key)
                    for key in to_remove:
                        del last_notification_time[key]
        
        await asyncio.sleep(60)

async def snooze_event(short_id, hours=1):
    """Откладывает событие на указанное количество часов"""
    try:
        pending = await get_pending_notifications()
        event_id = None
        event_time = None
        event_summary = None
        for p in pending:
            if p['short_id'] == short_id:
                event_id = p['id']
                event_time = p['time']
                event_summary = p['text']
                break
        
        if not event_id:
            logger.error(f"Событие не найдено для short_id: {short_id}")
            return False
        
        api = YandexCalendarAPI(YANDEX_API_TOKEN)
        new_start = event_time + timedelta(hours=hours)
        new_id = await api.create_event(event_summary, new_start)
        
        if new_id:
            await api.delete_event_instance(event_id, event_time)
            logger.info(f"Событие отложено на {hours} часов: {event_summary}")
            return True
        
        return False
    except Exception as e:
        logger.error(f"snooze_event error: {e}")
        return False

async def mark_done(short_id):
    """Отмечает событие как выполненное"""
    try:
        pending = await get_pending_notifications()
        event_id = None
        event_time = None
        
        for p in pending:
            if p['short_id'] == short_id:
                event_id = p['id']
                event_time = p['time']
                break
        
        if not event_id:
            logger.error(f"Событие не найдено для short_id: {short_id}")
            return False
        
        api = YandexCalendarAPI(YANDEX_API_TOKEN)
        return await api.delete_event_instance(event_id, event_time)
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
        msg = "✅ **Нет просроченных уведомлений!**"
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

async def setup_yandex_token(message: types.Message):
    """Настройка OAuth токена для Яндекс.Календаря"""
    if not YANDEX_CLIENT_ID:
        await send_persistent_message(
            message.chat.id,
            "❌ **Ошибка настройки!**\n\n"
            "Не указан YANDEX_CLIENT_ID в файле .env\n\n"
            "Для получения Client ID:\n"
            "1. Перейдите на https://oauth.yandex.ru/\n"
            "2. Создайте новое приложение\n"
            "3. В разделе 'Доступ к API' укажите 'Яндекс.Календарь'\n"
            "4. В поле 'Callback URL' укажите: https://oauth.yandex.ru/verification_code\n"
            "5. Скопируйте Client ID в файл .env"
        )
        return
    
    # Создаем URL для авторизации
    auth_params = {
        'response_type': 'code',
        'client_id': YANDEX_CLIENT_ID,
        'redirect_uri': 'https://oauth.yandex.ru/verification_code'
    }
    auth_url = f"{YANDEX_OAUTH_AUTH_URL}?{urlencode(auth_params)}"
    
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("🔑 Перейти к авторизации", url=auth_url))
    kb.add(InlineKeyboardButton("✅ Я получил код", callback_data="token_received"))
    kb.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_token_setup"))
    
    await send_persistent_message(
        message.chat.id,
        f"🔐 **Настройка доступа к Яндекс.Календарю**\n\n"
        f"Для работы бота необходимо получить OAuth токен.\n\n"
        f"**Инструкция:**\n"
        f"1. Нажмите кнопку 'Перейти к авторизации'\n"
        f"2. Войдите в свой Яндекс аккаунт\n"
        f"3. Разрешите доступ к календарю\n"
        f"4. После авторизации вы увидите страницу с кодом\n"
        f"5. Скопируйте полученный код\n"
        f"6. Нажмите '✅ Я получил код' и вставьте код\n\n"
        f"🔗 **Ссылка для авторизации:**\n{auth_url}",
        reply_markup=kb
    )

async def process_token_code(message: types.Message, state: FSMContext):
    """Обрабатывает полученный код авторизации"""
    code = message.text.strip()
    
    if not code:
        await send_persistent_message(message.chat.id, "❌ Введите корректный код авторизации")
        return
    
    # Обмениваем код на токен
    data = {
        'grant_type': 'authorization_code',
        'code': code,
        'client_id': YANDEX_CLIENT_ID,
        'client_secret': YANDEX_CLIENT_SECRET
    }
    
    await send_persistent_message(message.chat.id, "🔄 Обмен кода на токен...")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(YANDEX_OAUTH_TOKEN_URL, data=data) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    token = result.get('access_token')
                    
                    if token:
                        # Сохраняем токен в .env
                        env_path = os.path.join(os.path.dirname(__file__), ENV_FILE)
                        set_key(env_path, 'YANDEX_API_TOKEN', token)
                        global YANDEX_API_TOKEN
                        YANDEX_API_TOKEN = token
                        
                        await send_persistent_message(
                            message.chat.id,
                            "✅ **Токен успешно получен и сохранен!**\n\n"
                            "Бот готов к работе с Яндекс.Календарем."
                        )
                        
                        # Обновляем кэш
                        await update_calendar_cache()
                        await show_calendar_events(message.chat.id, persistent=True)
                    else:
                        await send_persistent_message(message.chat.id, "❌ Не удалось получить токен")
                else:
                    text = await resp.text()
                    await send_persistent_message(
                        message.chat.id,
                        f"❌ **Ошибка получения токена**\n\nКод ошибки: {resp.status}\n\n"
                        f"Проверьте правильность кода и попробуйте снова."
                    )
                    logger.error(f"Token exchange error: {resp.status} - {text}")
    except Exception as e:
        logger.error(f"process_token_code error: {e}")
        await send_persistent_message(message.chat.id, f"❌ Ошибка: {str(e)}")
    
    await state.finish()

# ---------- ОСНОВНЫЕ ОБРАБОТЧИКИ ----------
@dp.message_handler(commands=['start'])
async def cmd_start(msg, state):
    await delete_user_message(msg)
    await state.finish()
    if ADMIN_ID and msg.from_user.id != ADMIN_ID:
        return await msg.reply("❌ Нет доступа")
    
    ok, _ = await check_yandex_connection()
    
    if not ok and not YANDEX_API_TOKEN:
        welcome = f"""👋 **Добро пожаловать!**
🤖 Версия v{BOT_VERSION}

🔐 **Для работы бота необходимо настроить доступ к Яндекс.Календарю**

Нажмите кнопку "Настройки" и следуйте инструкции для получения OAuth токена."""
        
        await send_persistent_message(msg.chat.id, welcome)
        await send_persistent_message(msg.chat.id, "👋 **Выберите действие:**", reply_markup=get_main_keyboard())
    else:
        welcome = f"""👋 **Добро пожаловать!**
🤖 Версия v{BOT_VERSION}
📧 Яндекс API: {'✅ Доступен' if ok else '❌ Ошибка'}
🌍 Часовой пояс: {config.get('timezone', 'Europe/Moscow')}

📌 **Как это работает:**
• Все уведомления берутся ТОЛЬКО из Яндекс.Календаря
• При отметке "Выполнено" для обычного события - оно удаляется
• Для повторяющегося события - удаляется ТОЛЬКО сегодняшнее вхождение

📅 **В календаре показываются события на сегодня и завтра**"""
        
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
    if not get_yandex_api_available():
        await send_persistent_message(
            msg.chat.id,
            "❌ **Сначала настройте доступ к Яндекс.Календарю!**\n\nНажмите кнопку 'Настройки'"
        )
        return
    
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
    api = YandexCalendarAPI(YANDEX_API_TOKEN)
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
        await cb.answer("✅ Событие отмечено как выполненное!")
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
    
    if get_yandex_api_available():
        ok, _ = await check_yandex_connection()
        api_status = "✅ Доступен" if ok else "❌ Ошибка"
    else:
        api_status = "❌ Не настроен"
    
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton(f"Уведомления: {status}", callback_data="toggle_notify"),
        InlineKeyboardButton("🌍 Часовой пояс", callback_data="set_timezone"),
        InlineKeyboardButton("🔐 Настроить Яндекс API", callback_data="setup_yandex_token"),
        InlineKeyboardButton("🔍 Проверить календарь", callback_data="check_cal"),
        InlineKeyboardButton("ℹ️ Информация", callback_data="info")
    )
    await send_with_auto_delete(
        msg.chat.id,
        f"⚙️ **НАСТРОЙКИ**\n\n"
        f"📧 Яндекс API: {api_status}\n"
        f"🌍 Часовой пояс: {config.get('timezone', 'Europe/Moscow')}\n\n"
        f"{'🔐 Токен не настроен! Нажмите кнопку ниже для настройки.' if not get_yandex_api_available() else ''}",
        reply_markup=kb,
        delay=3600
    )

@dp.callback_query_handler(lambda c: c.data == "setup_yandex_token", state='*')
async def setup_token_callback(cb):
    await setup_yandex_token(cb.message)
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data == "token_received", state='*')
async def token_received(cb, state):
    await cb.message.edit_text(
        "📝 **Введите код авторизации**\n\n"
        "Скопируйте полученный код и отправьте его сюда.\n\n"
        "Для отмены отправьте /cancel"
    )
    await TokenSetupStates.waiting_for_confirmation.set()
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data == "cancel_token_setup", state='*')
async def cancel_token_setup(cb, state):
    await state.finish()
    await settings_menu_handler(cb.message)
    await cb.answer()

@dp.message_handler(state=TokenSetupStates.waiting_for_confirmation)
async def handle_token_code(msg, state):
    await process_token_code(msg, state)

@dp.callback_query_handler(lambda c: c.data == "check_cal", state='*')
async def check_cal(cb):
    if not get_yandex_api_available():
        return await cb.message.edit_text("❌ **Яндекс API не настроен!**\n\nНажмите кнопку 'Настроить Яндекс API'")
    await cb.message.edit_text("🔍 **Проверка подключения...**")
    ok, msg = await check_yandex_connection()
    if ok:
        await cb.message.edit_text(f"✅ **{msg}**")
        await update_calendar_cache()
        await show_calendar_events(cb.from_user.id, persistent=True)
    else:
        await cb.message.edit_text(f"❌ **{msg}**\n\nВозможно, токен устарел. Получите новый через настройки.")
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
    ok, _ = await check_yandex_connection() if get_yandex_api_available() else (False, "")
    api_status = "✅ Доступен" if ok else "❌ Ошибка" if get_yandex_api_available() else "❌ Не настроен"
    info_text = f"""📊 **СТАТИСТИКА**

🤖 **Версия:** v{BOT_VERSION} ({BOT_VERSION_DATE})

🌍 **Часовой пояс:** `{config.get('timezone', 'Europe/Moscow')}`
🕐 **Текущее время:** `{get_current_time().strftime('%d.%m.%Y %H:%M:%S')}`
🔔 **Уведомления:** `{'Вкл' if notifications_enabled else 'Выкл'}`
📧 **Яндекс API:** `{api_status}`

📌 Бот работает с Яндекс.Календарем через REST API"""
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
            if get_yandex_api_available():
                await update_calendar_cache()
                logger.info("Автообновление календаря выполнено")
        except Exception as e:
            logger.error(f"auto_update error: {e}")
        await asyncio.sleep(900)

async def on_startup(dp):
    init_config()
    load_config()
    
    old_data_file = 'notifications.json'
    if os.path.exists(old_data_file):
        os.remove(old_data_file)
        logger.info("Удалён старый файл notifications.json")
    
    calendar_events_cache.clear()
    
    logger.info(f"\n{'='*50}\n🤖 БОТ v{BOT_VERSION} ЗАПУЩЕН\n{'='*50}")
    
    if get_yandex_api_available():
        ok, msg = await check_yandex_connection()
        logger.info(f"Яндекс API: {'✅' if ok else '❌'} {msg}")
    else:
        logger.warning("Яндекс API не настроен! Используйте настройки бота для получения токена.")
        logger.info("Для настройки: отправьте боту команду /start и нажмите 'Настройки'")
    
    logger.info(f"Часовой пояс: {config.get('timezone', 'Europe/Moscow')}")
    logger.info(f"Текущее время: {get_current_time().strftime('%d.%m.%Y %H:%M:%S')}")
    logger.info(f"Логирование настроено: файл {LOG_FILE}, макс. размер {LOG_MAX_BYTES} байт")
    logger.info(f"{'='*50}\n")
    
    asyncio.create_task(check_pending())
    asyncio.create_task(auto_update_cache())
    logger.info("✅ Бот готов")

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)