import logging
import logging.handlers
import json
import re
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pytz
import hashlib
from urllib.parse import urlencode

# Настройка логирования
LOG_FILE = 'bot.log'
LOG_MAX_BYTES = 200 * 1024
LOG_BACKUP_COUNT = 1

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

logger = logging.getLogger(__name__)

# Добавляем файловый обработчик
file_handler = logging.handlers.RotatingFileHandler(
    LOG_FILE, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT, encoding='utf-8'
)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, 
    MessageHandler, filters, ConversationHandler, ContextTypes
)
from dotenv import load_dotenv
import requests

load_dotenv()

# Настройки
TELEGRAM_BOT_TOKEN = os.getenv('BOT_TOKEN')
ADMIN_ID = int(os.getenv('ADMIN_ID')) if os.getenv('ADMIN_ID') else None
YANDEX_OAUTH_TOKEN = os.getenv('YANDEX_API_TOKEN')
YANDEX_CLIENT_ID = os.getenv('YANDEX_CLIENT_ID')
YANDEX_CLIENT_SECRET = os.getenv('YANDEX_CLIENT_SECRET')

YANDEX_CALENDAR_API_URL = "https://api.calendar.yandex.net/v1"
YANDEX_OAUTH_AUTH_URL = "https://oauth.yandex.ru/authorize"
YANDEX_OAUTH_TOKEN_URL = "https://oauth.yandex.ru/token"

BOT_VERSION = "7.2"
BOT_VERSION_DATE = "22.04.2026"

# Состояния для ConversationHandler
WAITING_FOR_EVENT_TEXT = 1
WAITING_FOR_EVENT_DATE = 2
WAITING_FOR_TOKEN_CODE = 3

# Хранилище для кэша календаря
calendar_events_cache: List[Dict] = []
last_sync_time: Optional[datetime] = None
notifications_enabled = True
selected_calendar_id = "primary"

# Настройки часовых поясов
TIMEZONES = {
    'Москва (UTC+3)': 'Europe/Moscow',
    'Калининград (UTC+2)': 'Europe/Kaliningrad',
    'Екатеринбург (UTC+5)': 'Asia/Yekaterinburg',
    'Новосибирск (UTC+7)': 'Asia/Novosibirsk',
    'Владивосток (UTC+10)': 'Asia/Vladivostok',
    'Камчатка (UTC+12)': 'Asia/Kamchatka'
}

def load_config():
    """Загружает конфигурацию"""
    global notifications_enabled, selected_calendar_id
    config_file = 'config.json'
    if os.path.exists(config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
                notifications_enabled = config.get('notifications_enabled', True)
                selected_calendar_id = config.get('selected_calendar', 'primary')
        except:
            pass

def save_config():
    """Сохраняет конфигурацию"""
    config_file = 'config.json'
    config = {
        'notifications_enabled': notifications_enabled,
        'selected_calendar': selected_calendar_id,
        'timezone': 'Europe/Moscow'
    }
    with open(config_file, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

def get_current_time():
    """Возвращает текущее время в московском часовом поясе"""
    tz = pytz.timezone('Europe/Moscow')
    return datetime.now(tz)

def parse_datetime(date_str: str):
    """Парсит дату из строки"""
    now = get_current_time()
    tz = pytz.timezone('Europe/Moscow')
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

def get_main_keyboard():
    """Возвращает главную клавиатуру"""
    kb = ReplyKeyboardMarkup(
        [
            ["➕ Добавить", "📅 Календарь"],
            ["⚠️ Просроченные", "⚙️ Настройки"]
        ],
        resize_keyboard=True
    )
    return kb

class YandexCalendarAPI:
    """Класс для работы с Яндекс Календарем"""
    
    def __init__(self, token: str):
        self.token = token
        self.calendar_id = "primary"
    
    def _get_headers(self):
        return {
            "Authorization": f"OAuth {self.token}",
            "Content-Type": "application/json"
        }
    
    def test_connection(self) -> bool:
        """Тестирует подключение к API"""
        try:
            response = requests.get(
                f"{YANDEX_CALENDAR_API_URL}/calendars",
                headers=self._get_headers(),
                timeout=30
            )
            logger.info(f"Test connection status: {response.status_code}")
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Test connection error: {e}")
            return False
    
    def get_calendars(self) -> List[Dict]:
        """Получает список календарей"""
        try:
            response = requests.get(
                f"{YANDEX_CALENDAR_API_URL}/calendars",
                headers=self._get_headers(),
                timeout=30
            )
            if response.status_code == 200:
                data = response.json()
                return data.get('items', [])
            return []
        except Exception as e:
            logger.error(f"get_calendars error: {e}")
            return []
    
    def create_event(self, summary: str, start_time: datetime, end_time: datetime = None) -> Optional[str]:
        """Создает новое событие в календаре"""
        try:
            if end_time is None:
                end_time = start_time + timedelta(hours=1)
            
            event_data = {
                "summary": summary[:255],
                "start": {
                    "dateTime": start_time.isoformat(),
                    "timeZone": "Europe/Moscow"
                },
                "end": {
                    "dateTime": end_time.isoformat(),
                    "timeZone": "Europe/Moscow"
                }
            }
            
            logger.info(f"Creating event: {event_data}")
            
            response = requests.post(
                f"{YANDEX_CALENDAR_API_URL}/calendars/{self.calendar_id}/events",
                headers=self._get_headers(),
                json=event_data,
                timeout=30
            )
            
            logger.info(f"Create event response: {response.status_code}")
            
            if response.status_code == 201:
                data = response.json()
                logger.info(f"Создано событие: {data.get('id')}")
                return data.get('id')
            else:
                logger.error(f"Create event error: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"create_event error: {e}")
            return None
    
    def delete_event(self, event_id: str) -> bool:
        """Удаляет событие"""
        try:
            response = requests.delete(
                f"{YANDEX_CALENDAR_API_URL}/calendars/{self.calendar_id}/events/{event_id}",
                headers=self._get_headers(),
                timeout=30
            )
            if response.status_code == 204:
                logger.info(f"Событие удалено: {event_id}")
                return True
            logger.error(f"Delete event failed: {response.status_code}")
            return False
        except Exception as e:
            logger.error(f"delete_event error: {e}")
            return False
    
    def get_events(self, from_date: datetime, to_date: datetime) -> List[Dict]:
        """Получает события из календаря за указанный период"""
        try:
            # Форматируем даты для API
            from_str = from_date.strftime('%Y-%m-%dT00:00:00+03:00')
            to_str = to_date.strftime('%Y-%m-%dT23:59:59+03:00')
            
            params = {
                "timeMin": from_str,
                "timeMax": to_str,
                "singleEvents": "true",
                "orderBy": "startTime"
            }
            
            logger.info(f"Getting events from {from_str} to {to_str}")
            
            response = requests.get(
                f"{YANDEX_CALENDAR_API_URL}/calendars/{self.calendar_id}/events",
                headers=self._get_headers(),
                params=params,
                timeout=30
            )
            
            logger.info(f"Get events response: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Raw API response: {json.dumps(data, ensure_ascii=False)[:500]}")
                
                events = []
                tz = pytz.timezone('Europe/Moscow')
                
                for item in data.get('items', []):
                    try:
                        start_data = item.get('start', {})
                        start_str = start_data.get('dateTime') or start_data.get('date')
                        
                        if not start_str:
                            continue
                        
                        # Парсим время начала
                        if 'T' in start_str:
                            # Событие с временем
                            dt = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
                        else:
                            # Целодневное событие
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
                        })
                    except Exception as e:
                        logger.error(f"Parse event error: {e}")
                        continue
                
                logger.info(f"Parsed {len(events)} events")
                return events
            else:
                logger.error(f"Get events error: {response.status_code} - {response.text}")
                return []
        except Exception as e:
            logger.error(f"get_events error: {e}")
            return []

def get_yandex_api_available():
    """Проверяет доступность API"""
    return bool(YANDEX_OAUTH_TOKEN)

def update_calendar_cache():
    """Обновляет кэш календаря"""
    global calendar_events_cache, last_sync_time
    now = get_current_time()
    
    if not get_yandex_api_available():
        calendar_events_cache = []
        last_sync_time = now
        return []
    
    api = YandexCalendarAPI(YANDEX_OAUTH_TOKEN)
    
    # Проверяем подключение
    if not api.test_connection():
        logger.error("API connection failed")
        calendar_events_cache = []
        last_sync_time = now
        return []
    
    # Получаем события на 60 дней вперед
    end_date = now + timedelta(days=60)
    events = api.get_events(now, end_date)
    
    calendar_events_cache = events
    last_sync_time = now
    logger.info(f"Обновлён кэш календаря: {len(events)} событий")
    
    # Логируем первые 5 событий для отладки
    for i, ev in enumerate(events[:5]):
        logger.info(f"Event {i+1}: {ev['summary']} at {ev['start']}")
    
    return events

def get_pending_notifications() -> List[Dict]:
    """Получает просроченные уведомления"""
    now = get_current_time()
    update_calendar_cache()
    
    pending = []
    for ev in calendar_events_cache:
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
    logger.info(f"Found {len(pending)} pending notifications")
    return pending

def get_today_tomorrow_events() -> List[Tuple[datetime, Dict]]:
    """Получает события на сегодня и завтра"""
    now = get_current_time()
    update_calendar_cache()
    
    today = now.date()
    tomorrow = today + timedelta(days=1)
    
    result = []
    for ev in calendar_events_cache:
        dt = ev['start']
        event_date = dt.date()
        
        if event_date == today or event_date == tomorrow:
            result.append((dt, ev))
    
    result.sort(key=lambda x: x[0])
    logger.info(f"Found {len(result)} events for today/tomorrow")
    return result

def get_formatted_calendar_events() -> str:
    """Форматирует события календаря для отображения"""
    events = get_today_tomorrow_events()
    
    if not events:
        if not get_yandex_api_available():
            return "📅 **Требуется настройка API**\n\nНажмите кнопку \"Настройки\" и получите OAuth токен."
        
        if not calendar_events_cache:
            # Проверяем, есть ли ошибка подключения
            api = YandexCalendarAPI(YANDEX_OAUTH_TOKEN)
            if not api.test_connection():
                return "📅 **⚠️ Ошибка подключения к API**\n\nПроверьте токен и интернет-соединение.\n\nНажмите 'Настройки' → 'Проверить календарь'"
            return "📅 **В календаре нет событий**\n\nДобавьте события через Яндекс.Календарь или кнопку ➕ Добавить"
        else:
            return "📅 **Нет событий на сегодня и завтра**\n\nЕсть события на другие дни. Нажмите 'Все события' для просмотра."
    
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
    
    future_events = [ev for ev in calendar_events_cache if ev['start'].date() > tomorrow and ev['start'] >= now]
    if future_events:
        text += f"📌 **А также есть события на другие дни**\n"
        text += f"   • Всего событий в календаре: {len(calendar_events_cache)}\n"
        text += f"   • Из них предстоит: {len([e for e in calendar_events_cache if e['start'] >= now])}\n"
    
    if last_sync_time:
        text += f"\n🔄 *Последняя синхронизация:* {last_sync_time.strftime('%d.%m.%Y %H:%M:%S')}"
    return text

def get_pending_list_formatted() -> str:
    """Форматирует список просроченных событий"""
    pending = get_pending_notifications()
    
    if not pending:
        return "✅ **Нет просроченных уведомлений!**"
    
    text = f"⚠️ **ПРОСРОЧЕННЫЕ УВЕДОМЛЕНИЯ** ({len(pending)} шт.)\n\n"
    for idx, p in enumerate(pending, 1):
        recurring_mark = " 🔁" if p['is_recurring'] else ""
        text += f"{idx}. **{p['text']}**{recurring_mark}\n"
        text += f"   ⏰ {p['time'].strftime('%d.%m.%Y %H:%M')}\n\n"
    
    return text

# ---------- ОБРАБОТЧИКИ КОМАНД ----------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    if ADMIN_ID and update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Нет доступа")
        return
    
    load_config()
    
    if not get_yandex_api_available():
        welcome = f"""👋 **Добро пожаловать!**
🤖 Версия v{BOT_VERSION}

🔐 **Для работы бота необходимо настроить доступ к Яндекс.Календарю**

Нажмите кнопку "Настройки" и следуйте инструкции для получения OAuth токена."""
        
        await update.message.reply_text(welcome, parse_mode='Markdown')
        await update.message.reply_text("👋 **Выберите действие:**", reply_markup=get_main_keyboard(), parse_mode='Markdown')
    else:
        # Проверяем подключение
        api = YandexCalendarAPI(YANDEX_OAUTH_TOKEN)
        connection_ok = api.test_connection()
        
        if connection_ok:
            update_calendar_cache()
            connection_status = "✅ Подключено"
        else:
            connection_status = "❌ Ошибка подключения"
        
        welcome = f"""👋 **Добро пожаловать!**
🤖 Версия v{BOT_VERSION}
📧 Яндекс API: {connection_status}
🌍 Часовой пояс: Europe/Moscow

📌 **Как это работает:**
• Все уведомления берутся ТОЛЬКО из Яндекс.Календаря
• При отметке "Выполнено" событие удаляется из календаря

📅 **В календаре показываются события на сегодня и завтра**"""
        
        await update.message.reply_text(welcome, parse_mode='Markdown')
        await update.message.reply_text("👋 **Выберите действие:**", reply_markup=get_main_keyboard(), parse_mode='Markdown')
        
        events_text = get_formatted_calendar_events()
        await update.message.reply_text(events_text, parse_mode='Markdown')
        
        pending_text = get_pending_list_formatted()
        await update.message.reply_text(pending_text, parse_mode='Markdown')

async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать главное меню"""
    await update.message.reply_text("👋 **Главное меню:**", reply_markup=get_main_keyboard(), parse_mode='Markdown')
    update_calendar_cache()
    events_text = get_formatted_calendar_events()
    await update.message.reply_text(events_text, parse_mode='Markdown')
    pending_text = get_pending_list_formatted()
    await update.message.reply_text(pending_text, parse_mode='Markdown')

async def button_calendar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать календарь"""
    events_text = get_formatted_calendar_events()
    
    keyboard = [
        [InlineKeyboardButton("🔄 Обновить", callback_data="refresh")],
        [InlineKeyboardButton("📋 Все события", callback_data="all_events")],
        [InlineKeyboardButton("🔍 Проверить API", callback_data="check_api")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(events_text, parse_mode='Markdown', reply_markup=reply_markup)

async def button_pending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать просроченные"""
    pending_text = get_pending_list_formatted()
    
    if "Нет просроченных" in pending_text:
        await update.message.reply_text(pending_text, parse_mode='Markdown')
    else:
        keyboard = [[InlineKeyboardButton("✅ Выполнить все", callback_data="complete_all")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(pending_text, parse_mode='Markdown', reply_markup=reply_markup)

async def button_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать настройки"""
    status = "🔔 Вкл" if notifications_enabled else "🔕 Выкл"
    
    # Проверяем подключение
    api = YandexCalendarAPI(YANDEX_OAUTH_TOKEN) if YANDEX_OAUTH_TOKEN else None
    if api:
        connection_ok = api.test_connection()
        api_status = "✅ Подключено" if connection_ok else "❌ Ошибка подключения"
    else:
        api_status = "❌ Не настроен"
    
    keyboard = [
        [InlineKeyboardButton(f"Уведомления: {status}", callback_data="toggle_notify")],
        [InlineKeyboardButton("🌍 Часовой пояс", callback_data="set_timezone")],
        [InlineKeyboardButton("🔐 Настроить Яндекс API", callback_data="setup_token")],
        [InlineKeyboardButton("🔍 Проверить API", callback_data="check_api")],
        [InlineKeyboardButton("ℹ️ Информация", callback_data="info")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = f"⚙️ **НАСТРОЙКИ**\n\n📧 Яндекс API: {api_status}\n🌍 Часовой пояс: Europe/Moscow"
    if not get_yandex_api_available():
        text += "\n\n🔐 Токен не настроен! Нажмите кнопку ниже для настройки."
    
    await update.message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup)

async def add_event_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начать добавление события"""
    if not get_yandex_api_available():
        await update.message.reply_text("❌ **Сначала настройте доступ к Яндекс.Календарю!**\n\nНажмите кнопку 'Настройки'", parse_mode='Markdown')
        return ConversationHandler.END
    
    await update.message.reply_text("✏️ **Введите текст уведомления:**\n\n💡 Для отмены /cancel", parse_mode='Markdown')
    return WAITING_FOR_EVENT_TEXT

async def add_event_get_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получить текст события"""
    if not update.message.text:
        await update.message.reply_text("❌ Введите текст.", parse_mode='Markdown')
        return WAITING_FOR_EVENT_TEXT
    
    context.user_data['event_text'] = update.message.text
    await update.message.reply_text(
        "🗓️ **Введите дату и время**\n📝 Форматы:\n• `22.04 14:00`\n• `22.04.2026 14:00`\n• `22.04.2026` (весь день)",
        parse_mode='Markdown'
    )
    return WAITING_FOR_EVENT_DATE

async def add_event_get_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Получить дату и создать событие"""
    dt = parse_datetime(update.message.text)
    if dt is None or dt <= get_current_time():
        await update.message.reply_text("❌ **Неверный формат или дата в прошлом!**", parse_mode='Markdown')
        return WAITING_FOR_EVENT_DATE
    
    text = context.user_data.get('event_text')
    api = YandexCalendarAPI(YANDEX_OAUTH_TOKEN)
    event_id = api.create_event(text, dt)
    
    if event_id:
        await update.message.reply_text(
            f"✅ **Уведомление создано!**\n📝 {text}\n⏰ {dt.strftime('%d.%m.%Y %H:%M')}",
            parse_mode='Markdown'
        )
        update_calendar_cache()
        
        events_text = get_formatted_calendar_events()
        await update.message.reply_text(events_text, parse_mode='Markdown')
    else:
        await update.message.reply_text("❌ **Ошибка при создании уведомления!**\n\nПроверьте логи для деталей.", parse_mode='Markdown')
    
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена операции"""
    await update.message.reply_text("✅ **Операция отменена!**", parse_mode='Markdown')
    return ConversationHandler.END

# ---------- CALLBACK ОБРАБОТЧИКИ ----------
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик inline кнопок"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "refresh":
        update_calendar_cache()
        events_text = get_formatted_calendar_events()
        await query.edit_message_text(events_text, parse_mode='Markdown')
    
    elif query.data == "check_api":
        if not get_yandex_api_available():
            await query.edit_message_text("❌ **API не настроен!**\n\nНажмите 'Настройки' → 'Настроить Яндекс API'", parse_mode='Markdown')
            return
        
        await query.edit_message_text("🔍 **Проверка подключения к API...**", parse_mode='Markdown')
        
        api = YandexCalendarAPI(YANDEX_OAUTH_TOKEN)
        connection_ok = api.test_connection()
        
        if connection_ok:
            # Пробуем получить список календарей
            calendars = api.get_calendars()
            calendars_text = f"\n📁 Найдено календарей: {len(calendars)}"
            if calendars:
                calendars_text += "\n📋 Список календарей:\n"
                for cal in calendars[:5]:
                    calendars_text += f"   • {cal.get('summary', 'Без названия')} (ID: {cal.get('id', '?')})\n"
            
            # Пробуем получить события
            now = get_current_time()
            end_date = now + timedelta(days=7)
            events = api.get_events(now, end_date)
            
            await query.edit_message_text(
                f"✅ **API работает корректно!**{calendars_text}\n\n📊 Найдено событий на неделю: {len(events)}",
                parse_mode='Markdown'
            )
            
            # Обновляем кэш
            update_calendar_cache()
        else:
            await query.edit_message_text(
                "❌ **Ошибка подключения к API!**\n\n"
                "Проверьте:\n"
                "1. Корректность OAuth токена\n"
                "2. Интернет-соединение\n"
                "3. Права доступа приложения",
                parse_mode='Markdown'
            )
    
    elif query.data == "all_events":
        update_calendar_cache()
        if not calendar_events_cache:
            text = "📅 **В календаре нет событий**"
        else:
            text = "📅 **ВСЕ СОБЫТИЯ КАЛЕНДАРЯ**\n\n"
            now = get_current_time()
            events_by_date = {}
            for ev in calendar_events_cache:
                if ev['start'] < now:
                    continue
                date_key = ev['start'].date()
                if date_key not in events_by_date:
                    events_by_date[date_key] = []
                events_by_date[date_key].append(ev)
            
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
        
        keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="back")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text, parse_mode='Markdown', reply_markup=reply_markup)
    
    elif query.data == "back":
        events_text = get_formatted_calendar_events()
        keyboard = [
            [InlineKeyboardButton("🔄 Обновить", callback_data="refresh")],
            [InlineKeyboardButton("📋 Все события", callback_data="all_events")],
            [InlineKeyboardButton("🔍 Проверить API", callback_data="check_api")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(events_text, parse_mode='Markdown', reply_markup=reply_markup)
    
    elif query.data == "complete_all":
        pending = get_pending_notifications()
        api = YandexCalendarAPI(YANDEX_OAUTH_TOKEN)
        for p in pending:
            api.delete_event(p['id'])
        update_calendar_cache()
        await query.edit_message_text("✅ **Все просроченные события удалены!**", parse_mode='Markdown')
    
    elif query.data == "toggle_notify":
        global notifications_enabled
        notifications_enabled = not notifications_enabled
        save_config()
        status = "🔔 Вкл" if notifications_enabled else "🔕 Выкл"
        await query.edit_message_text(f"✅ **Уведомления {'включены' if notifications_enabled else 'выключены'}!**", parse_mode='Markdown')
    
    elif query.data == "setup_token":
        await setup_token(update, context)
    
    elif query.data == "set_timezone":
        keyboard = []
        for name in TIMEZONES:
            keyboard.append([InlineKeyboardButton(name, callback_data=f"tz_{name}")])
        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="cancel_tz")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("🌍 **Выберите часовой пояс**\n\nТекущий: Europe/Moscow", parse_mode='Markdown', reply_markup=reply_markup)
    
    elif query.data.startswith("tz_"):
        name = query.data.replace("tz_", "")
        tz = TIMEZONES.get(name, 'Europe/Moscow')
        config_data = {'timezone': tz}
        with open('config.json', 'w', encoding='utf-8') as f:
            json.dump(config_data, f)
        await query.edit_message_text(f"✅ **Часовой пояс установлен:** {name}", parse_mode='Markdown')
    
    elif query.data == "cancel_tz":
        await query.edit_message_text("❌ **Операция отменена**", parse_mode='Markdown')
    
    elif query.data == "info":
        api_status = "✅ Настроен" if get_yandex_api_available() else "❌ Не настроен"
        info_text = f"""📊 **СТАТИСТИКА**

🤖 **Версия:** v{BOT_VERSION} ({BOT_VERSION_DATE})

🌍 **Часовой пояс:** Europe/Moscow
🕐 **Текущее время:** `{get_current_time().strftime('%d.%m.%Y %H:%M:%S')}`
🔔 **Уведомления:** `{'Вкл' if notifications_enabled else 'Выкл'}`
📧 **Яндекс API:** `{api_status}`
📁 **Календарь:** primary
📊 **Событий в кэше:** {len(calendar_events_cache)}

📌 Бот работает с Яндекс.Календарем через REST API

💡 **Совет:** Если не видите события, нажмите "Проверить API" для диагностики"""
        await query.edit_message_text(info_text, parse_mode='Markdown')

async def setup_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Настройка OAuth токена"""
    if not YANDEX_CLIENT_ID:
        text = """❌ **Ошибка настройки!**

Не указан YANDEX_CLIENT_ID в файле .env

Для получения Client ID:
1. Перейдите на https://oauth.yandex.ru/
2. Создайте новое приложение
3. В разделе 'Доступ к API' укажите 'Яндекс.Календарь'
4. В поле 'Callback URL' укажите: https://oauth.yandex.ru/verification_code
5. Скопируйте Client ID в файл .env"""
        
        if update.callback_query:
            await update.callback_query.edit_message_text(text, parse_mode='Markdown')
        else:
            await update.message.reply_text(text, parse_mode='Markdown')
        return
    
    auth_params = {
        'response_type': 'code',
        'client_id': YANDEX_CLIENT_ID,
        'redirect_uri': 'https://oauth.yandex.ru/verification_code'
    }
    auth_url = f"{YANDEX_OAUTH_AUTH_URL}?{urlencode(auth_params)}"
    
    keyboard = [
        [InlineKeyboardButton("🔑 Перейти к авторизации", url=auth_url)],
        [InlineKeyboardButton("✅ Я получил код", callback_data="enter_code")],
        [InlineKeyboardButton("❌ Отмена", callback_data="cancel_setup")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = f"""🔐 **Настройка доступа к Яндекс.Календарю**

**Инструкция:**
1. Нажмите кнопку 'Перейти к авторизации'
2. Войдите в свой Яндекс аккаунт
3. Разрешите доступ к календарю
4. Скопируйте полученный код
5. Нажмите '✅ Я получил код' и вставьте код

🔗 **Ссылка для авторизации:**
{auth_url}"""
    
    if update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode='Markdown', reply_markup=reply_markup)
    else:
        await update.message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup)

async def enter_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запросить код авторизации"""
    await update.callback_query.edit_message_text(
        "📝 **Введите код авторизации**\n\nСкопируйте полученный код и отправьте его сюда.\n\nДля отмены отправьте /cancel",
        parse_mode='Markdown'
    )
    return WAITING_FOR_TOKEN_CODE

async def process_token_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработать полученный код"""
    code = update.message.text.strip()
    
    if not code:
        await update.message.reply_text("❌ Введите корректный код авторизации")
        return WAITING_FOR_TOKEN_CODE
    
    data = {
        'grant_type': 'authorization_code',
        'code': code,
        'client_id': YANDEX_CLIENT_ID,
        'client_secret': YANDEX_CLIENT_SECRET
    }
    
    await update.message.reply_text("🔄 Обмен кода на токен...")
    
    try:
        response = requests.post(YANDEX_OAUTH_TOKEN_URL, data=data, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            token = result.get('access_token')
            
            if token:
                # Сохраняем токен в .env
                env_path = os.path.join(os.path.dirname(__file__), '.env')
                with open(env_path, 'a') as f:
                    f.write(f'\nYANDEX_API_TOKEN={token}')
                
                global YANDEX_OAUTH_TOKEN
                YANDEX_OAUTH_TOKEN = token
                
                await update.message.reply_text(
                    "✅ **Токен успешно получен и сохранен!**\n\n"
                    "Бот готов к работе с Яндекс.Календарем.\n\n"
                    "Нажмите /start для перезапуска",
                    parse_mode='Markdown'
                )
                return ConversationHandler.END
            else:
                await update.message.reply_text("❌ Не удалось получить токен")
        else:
            await update.message.reply_text(
                f"❌ **Ошибка получения токена**\n\nКод ошибки: {response.status_code}\n\n"
                f"Проверьте правильность кода и попробуйте снова."
            )
    except Exception as e:
        logger.error(f"process_token_code error: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")
    
    return WAITING_FOR_TOKEN_CODE

async def cancel_setup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена настройки токена"""
    await update.callback_query.edit_message_text("❌ **Настройка отменена**", parse_mode='Markdown')
    return ConversationHandler.END

# ---------- ОСНОВНАЯ ФУНКЦИЯ ----------
def main():
    """Главная функция запуска бота"""
    load_config()
    
    # Создаем приложение
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    # ConversationHandler для добавления события
    add_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex('^➕ Добавить$'), add_event_start)],
        states={
            WAITING_FOR_EVENT_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_event_get_text)],
            WAITING_FOR_EVENT_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_event_get_date)],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
    )
    
    # ConversationHandler для получения токена
    token_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(enter_code, pattern='^enter_code$')],
        states={
            WAITING_FOR_TOKEN_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_token_code)],
        },
        fallbacks=[CommandHandler('cancel', cancel), CallbackQueryHandler(cancel_setup, pattern='^cancel_setup$')],
    )
    
    # Регистрируем обработчики
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("menu", cmd_menu))
    application.add_handler(CommandHandler("cancel", cancel))
    application.add_handler(add_conv)
    application.add_handler(token_conv)
    
    # Обработчики кнопок меню
    application.add_handler(MessageHandler(filters.Regex('^📅 Календарь$'), button_calendar))
    application.add_handler(MessageHandler(filters.Regex('^⚠️ Просроченные$'), button_pending))
    application.add_handler(MessageHandler(filters.Regex('^⚙️ Настройки$'), button_settings))
    
    # Callback обработчики
    application.add_handler(CallbackQueryHandler(handle_callback, pattern='^(refresh|all_events|back|complete_all|toggle_notify|setup_token|set_timezone|cancel_tz|info|check_api)$'))
    application.add_handler(CallbackQueryHandler(setup_token, pattern='^setup_token$'))
    application.add_handler(CallbackQueryHandler(enter_code, pattern='^enter_code$'))
    application.add_handler(CallbackQueryHandler(cancel_setup, pattern='^cancel_setup$'))
    
    # Запускаем бота
    logger.info(f"Бот v{BOT_VERSION} запущен")
    application.run_polling()

if __name__ == '__main__':
    main()