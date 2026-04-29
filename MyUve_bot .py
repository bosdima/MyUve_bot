import os
import logging
import asyncio
import datetime
from typing import List, Optional
from zoneinfo import ZoneInfo

import caldav
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, CallbackContext
from pydantic import BaseModel, Field, ValidationError
from tzlocal import get_localzone_name

# --- Конфигурация и Логирование ---
load_dotenv()

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Часовой пояс (из логов видно +03:00, скорее всего Москва)
try:
    LOCAL_TZ = ZoneInfo(get_localzone_name())
except Exception:
    LOCAL_TZ = ZoneInfo("Europe/Moscow")

# --- Модели данных (Pydantic) ---
class BotSettings(BaseModel):
    bot_token: str = Field(..., alias="BOT_TOKEN")
    admin_id: int = Field(..., alias="ADMIN_ID")
    yandex_password: str = Field(..., alias="YANDEX_APP_PASSWORD")
    caldav_url: str = Field(default="https://caldav.calendar.yandex.ru/principals/users/", alias="CALDAV_URL")
    yandex_login: Optional[str] = Field(default=None, alias="YANDEX_LOGIN")

    class Config:
        populate_by_name = True

# --- Глобальные переменные ---
settings = None
caldav_client = None
calendar_obj = None
last_sync_time = None

# --- Инициализация CalDAV ---
def init_caldav():
    global caldav_client, calendar_obj, settings
    
    try:
        settings = BotSettings.model_validate(os.environ)
        logger.info("Настройки успешно загружены.")
        
        # Формирование полного URL для пользователя
        principal_url = settings.caldav_url
        if settings.yandex_login:
            # Если логин указан, добавляем его к пути (формат может варьироваться в зависимости от провайдера)
            # Для Яндекса часто достаточно базового URL с авторизацией, но иногда требуется путь /principals/users/{login}/
            if not principal_url.endswith('/'):
                principal_url += '/'
            # Проверка, не содержит ли URL уже логин
            if settings.yandex_login not in principal_url:
                principal_url += f"{settings.yandex_login}/"
        
        logger.info(f"Подключение к CalDAV: {principal_url}")
        
        caldav_client = caldav.DAVClient(
            url=principal_url,
            username=settings.yandex_login or "user", # Яндекс иногда игнорирует юзернейм в URL, если он в пути
            password=settings.yandex_password
        )
        
        my_principal = caldav_client.principal()
        calendars = my_principal.calendars()
        
        if not calendars:
            logger.error("Календари не найдены!")
            return False
            
        # Берем первый найденный календарь (или можно искать по имени)
        calendar_obj = calendars[0]
        logger.info(f"Подключено к календарю: {calendar_obj.name}")
        return True
        
    except ValidationError as e:
        logger.error(f"Ошибка валидации настроек: {e}")
        return False
    except Exception as e:
        logger.error(f"Ошибка подключения к CalDAV: {e}", exc_info=True)
        return False

# --- Логика синхронизации ---
async def sync_calendar_events(context: ContextTypes.DEFAULT_TYPE):
    """Периодическая задача синхронизации событий"""
    global last_sync_time
    
    if not calendar_obj:
        logger.warning("CalDAV клиент не инициализирован. Пропуск синхронизации.")
        return

    now = datetime.datetime.now(LOCAL_TZ)
    
    # Определяем диапазон загрузки
    # Если это первый запуск, грузим события на ближайшие 7 дней от текущего момента
    # Иначе грузим изменения с момента последней проверки (упрощенно: грузим окно вокруг текущего времени)
    if last_sync_time is None:
        start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + datetime.timedelta(days=7)
        logger.info(f"Первая загрузка событий с {start_date} по {end_date}")
    else:
        # Сдвигаем окно вперед или обновляем текущее
        start_date = last_sync_time
        end_date = now + datetime.timedelta(days=1)
        logger.info(f"Загрузка событий с {start_date} по {end_date}")

    try:
        events = calendar_obj.date_search(
            start=start_date,
            end=end_date,
            expand=True
        )
        
        logger.info(f"Найдено событий: {len(events)}")
        
        # Здесь логика сравнения с предыдущим состоянием и отправки уведомлений
        # Для примера просто отправим список новых событий админу
        # В реальном боте нужно хранить ID обработанных событий в БД или памяти
        
        message_text = f" Обновление календаря ({now.strftime('%d.%m %H:%M')})\n\n"
        count = 0
        
        for event in events:
            # Парсинг события (упрощенно)
            # event.instance.vevent.dtstart и т.д. зависят от библиотеки caldav
            try:
                summary = event.vobject_instance.vevent.summary.value if hasattr(event, 'vobject_instance') and event.vobject_instance.vevent.summary else "Без названия"
                dtstart = event.vobject_instance.vevent.dtstart.value if hasattr(event, 'vobject_instance') and event.vobject_instance.vevent.dtstart else now
                
                if isinstance(dtstart, datetime.datetime):
                    time_str = dtstart.astimezone(LOCAL_TZ).strftime("%d.%m %H:%M")
                else:
                    time_str = str(dtstart)
                
                message_text += f"• {time_str} — {summary}\n"
                count += 1
            except Exception as parse_err:
                logger.warning(f"Ошибка парсинга события: {parse_err}")
                continue

        if count > 0:
            await context.bot.send_message(
                chat_id=settings.admin_id,
                text=message_text
            )
            logger.info(f"Отправлено уведомление с {count} событиями.")
        else:
            logger.info("Новых событий для уведомления нет.")

        last_sync_time = now

    except Exception as e:
        logger.error(f"Ошибка при поиске событий: {e}", exc_info=True)

# --- Обработчики команд Telegram ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != settings.admin_id:
        await update.message.reply_text("Доступ запрещен.")
        return
    
    status = "✅ Подключено" if calendar_obj else "❌ Ошибка подключения"
    await update.message.reply_text(
        f"Бот календаря запущен (v1.3.5)\nСтатус CalDAV: {status}\nПоследняя синхронизация: {last_sync_time}"
    )

async def force_sync(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != settings.admin_id:
        return
    await update.message.reply_text("Запускаю принудительную синхронизацию...")
    await sync_calendar_events(context)
    await update.message.reply_text("Готово.")

# --- Основной запуск ---
def main():
    global last_sync_time
    
    # Инициализация CalDAV
    if not init_caldav():
        logger.error("Не удалось инициализировать бот из-за ошибки CalDAV.")
        return

    # Создание приложения
    application = ApplicationBuilder().token(settings.bot_token).build()

    # Добавление обработчиков
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("sync", force_sync))

    # Планировщик задач (JobQueue)
    job_queue = application.job_queue
    
    # Запуск первой синхронизации сразу после старта
    job_queue.run_once(sync_calendar_events, 1)
    
    # Периодическая синхронизация каждые 60 секунд (для тестов, в продакшене можно увеличить до 300-600 сек)
    # Судя по логам, интервал около 1 минуты
    job_queue.run_repeating(sync_calendar_events, interval=60, first=10)

    logger.info("Бот запущен и ожидает команды...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()