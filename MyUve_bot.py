#!/usr/bin/env python3
"""
CalDAV Telegram Bot
Версия 3.6 - Исправлено удаление повторяющихся событий на сегодня
"""

import os
import sys
import asyncio
import logging
import json
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo  # Python 3.9+

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils import executor
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage

from caldav import DAVClient
from icalendar import Calendar, Event
import pytz
from dotenv import load_dotenv

load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация
TELEGRAM_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
CALDAV_URL = os.getenv('CALDAV_URL')
CALDAV_USERNAME = os.getenv('CALDAV_USERNAME')
CALDAV_PASSWORD = os.getenv('CALDAV_PASSWORD')
TIMEZONE = os.getenv('TIMEZONE', 'Europe/Moscow')
ADMIN_ID = int(os.getenv('ADMIN_ID', '0')) if os.getenv('ADMIN_ID') else None

# Часовой пояс
LOCAL_TZ = ZoneInfo(TIMEZONE)

class EventStates(StatesGroup):
    waiting_for_title = State()
    waiting_for_date = State()
    waiting_for_time = State()
    waiting_for_end_time = State()
    waiting_for_repeat = State()
    waiting_for_repeat_until = State()
    waiting_for_delete_selection = State()
    waiting_for_edit_selection = State()
    waiting_for_edit_title = State()
    waiting_for_edit_date = State()
    waiting_for_edit_time = State()
    waiting_for_edit_end_time = State()

class CalDAVClient:
    def __init__(self):
        self.client = None
        self.principal = None
        self.calendars = []
        self._connect()
    
    def _connect(self):
        """Подключение к CalDAV серверу"""
        try:
            self.client = DAVClient(
                url=CALDAV_URL,
                username=CALDAV_USERNAME,
                password=CALDAV_PASSWORD
            )
            self.principal = self.client.principal()
            self.calendars = self.principal.calendars()
            logger.info(f"CalDAV: ✅ Подключено, найдено календарей: {len(self.calendars)}")
            return True
        except Exception as e:
            logger.error(f"CalDAV: ❌ Ошибка подключения: {e}")
            return False
    
    def get_default_calendar(self):
        """Получение основного календаря"""
        if self.calendars:
            return self.calendars[0]
        return None
    
    def get_events(self, start_date: date, end_date: date = None, include_recurring: bool = True) -> List[Dict]:
        """Получение событий за период"""
        if not self.client:
            if not self._connect():
                return []
        
        if end_date is None:
            end_date = start_date + timedelta(days=1)
        
        # Используем datetime с правильным часовым поясом для CalDAV
        start_dt = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=LOCAL_TZ)
        end_dt = datetime.combine(end_date, datetime.min.time()).replace(tzinfo=LOCAL_TZ)
        
        try:
            calendar = self.get_default_calendar()
            if not calendar:
                logger.error("Календарь не найден")
                return []
            
            # Получаем события с учетом повторений
            events_raw = calendar.date_search(
                start=start_dt,
                end=end_dt,
                expand=include_recurring  # expand=True - разворачивает повторяющиеся события
            )
            
            events = []
            for event_data in events_raw:
                try:
                    cal = Calendar.from_ical(event_data.data)
                    for component in cal.walk():
                        if component.name == "VEVENT":
                            event = self._parse_event(component, event_data.url)
                            if event:
                                events.append(event)
                except Exception as e:
                    logger.error(f"Ошибка парсинга события: {e}")
                    continue
            
            logger.info(f"Найдено событий: {len(events)}")
            return events
            
        except Exception as e:
            logger.error(f"Ошибка получения событий: {e}")
            return []
    
    def _parse_event(self, component, url: str = None) -> Optional[Dict]:
        """Парсинг события из компонента iCalendar"""
        try:
            uid = str(component.get('UID', ''))
            summary = str(component.get('SUMMARY', 'Без названия'))
            
            # Парсинг даты и времени
            dtstart = component.get('DTSTART')
            dtend = component.get('DTEND')
            recurrence_id = component.get('RECURRENCE-ID')  # Важно для повторяющихся событий!
            
            if not dtstart:
                return None
            
            # Определяем время начала
            if hasattr(dtstart, 'dt'):
                start_time = dtstart.dt
            else:
                start_time = dtstart
            
            # Определяем время окончания
            if dtend:
                if hasattr(dtend, 'dt'):
                    end_time = dtend.dt
                else:
                    end_time = dtend
            else:
                # Если нет DTEND, ставим +1 час
                if isinstance(start_time, datetime):
                    end_time = start_time + timedelta(hours=1)
                else:
                    end_time = start_time + timedelta(days=1)
            
            # Приводим к datetime если нужно
            if isinstance(start_time, date) and not isinstance(start_time, datetime):
                start_time = datetime.combine(start_time, datetime.min.time())
            if isinstance(end_time, date) and not isinstance(end_time, datetime):
                end_time = datetime.combine(end_time, datetime.max.time())
            
            # Проверка на целый день
            is_all_day = not isinstance(component.get('DTSTART').dt, datetime)
            
            # Парсинг повторения
            rrule = component.get('RRULE')
            is_recurring = rrule is not None
            
            recurrence_info = None
            if is_recurring:
                recurrence_info = str(rrule.to_ical(), 'utf-8') if rrule else None
            
            return {
                'uid': uid,
                'url': url,
                'summary': summary,
                'start_time': start_time,
                'end_time': end_time,
                'is_all_day': is_all_day,
                'is_recurring': is_recurring,
                'recurrence_id': recurrence_id,  # Важно для удаления конкретного вхождения
                'recurrence_info': recurrence_info,
                'raw_component': component
            }
        except Exception as e:
            logger.error(f"Ошибка парсинга события: {e}")
            return None
    
    def add_event(self, summary: str, start_time: datetime, end_time: datetime = None, 
                  is_all_day: bool = False, recurrence: str = None, recurrence_until: date = None) -> bool:
        """Добавление события в календарь"""
        if not self.client:
            if not self._connect():
                return False
        
        try:
            calendar = self.get_default_calendar()
            if not calendar:
                return False
            
            # Создаем событие
            cal = Calendar()
            cal.add('prodid', '-//MyUve Bot//CalDAV//RU')
            cal.add('version', '2.0')
            
            event = Event()
            event.add('uid', self._generate_uid())
            event.add('summary', summary)
            
            # Настройка времени
            if is_all_day:
                event.add('dtstart', start_time.date())
                if end_time:
                    event.add('dtend', end_time.date())
                else:
                    event.add('dtend', start_time.date() + timedelta(days=1))
            else:
                # Приводим к UTC для CalDAV
                start_utc = start_time.astimezone(pytz.UTC)
                event.add('dtstart', start_utc)
                
                if end_time:
                    end_utc = end_time.astimezone(pytz.UTC)
                    event.add('dtend', end_utc)
                else:
                    event.add('dtend', start_utc + timedelta(hours=1))
            
            # Добавляем повторение
            if recurrence:
                if recurrence == 'daily':
                    rrule_str = 'FREQ=DAILY'
                elif recurrence == 'weekly':
                    rrule_str = 'FREQ=WEEKLY'
                elif recurrence == 'monthly':
                    rrule_str = 'FREQ=MONTHLY'
                elif recurrence == 'yearly':
                    rrule_str = 'FREQ=YEARLY'
                else:
                    rrule_str = recurrence
                
                if recurrence_until and recurrence != 'daily':
                    until_str = recurrence_until.strftime('%Y%m%d')
                    rrule_str += f';UNTIL={until_str}T235959Z'
                elif recurrence == 'daily' and recurrence_until:
                    until_str = recurrence_until.strftime('%Y%m%d')
                    rrule_str += f';UNTIL={until_str}T235959Z'
                
                event.add('rrule', rrule_str)
            
            cal.add_component(event)
            
            # Сохраняем
            calendar.save_event(cal.to_ical())
            logger.info(f"Событие добавлено: {summary}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка добавления события: {e}")
            return False
    
    def delete_event(self, event_url: str, recurrence_id: str = None) -> bool:
        """
        Удаление события.
        Если recurrence_id указан - удаляем только конкретное вхождение повторяющегося события.
        """
        if not self.client:
            if not self._connect():
                return False
        
        try:
            calendar = self.get_default_calendar()
            if not calendar:
                return False
            
            # Получаем событие по URL
            event = calendar.event_by_url(event_url)
            if not event:
                logger.error(f"Событие не найдено: {event_url}")
                return False
            
            if recurrence_id:
                # Удаляем конкретное вхождение повторяющегося события
                # Добавляем EXDATE для этого вхождения
                event_data = event.data
                cal = Calendar.from_ical(event_data)
                
                for component in cal.walk():
                    if component.name == "VEVENT":
                        # Получаем существующий EXDATE или создаем новый
                        exdate = component.get('EXDATE', [])
                        if not isinstance(exdate, list):
                            exdate = [exdate]
                        
                        # Добавляем дату для исключения
                        from icalendar import vDDDTypes
                        recurrence_date = recurrence_id
                        if isinstance(recurrence_id, str):
                            # Парсим дату из recurrence_id
                            try:
                                if 'T' in recurrence_id:
                                    dt = datetime.fromisoformat(recurrence_id.replace('Z', '+00:00'))
                                else:
                                    dt = datetime.strptime(recurrence_id, '%Y%m%d')
                                exdate.append(vDDDTypes(dt))
                            except:
                                # Если не удалось распарсить, пробуем другие форматы
                                exdate.append(vDDDTypes(recurrence_id))
                        
                        component['EXDATE'] = exdate
                        break
                
                # Сохраняем измененное событие
                calendar.save_event(cal.to_ical())
                logger.info(f"Добавлено исключение для повторяющегося события: {event_url}")
                return True
            else:
                # Удаляем все событие (мастер-событие)
                event.delete()
                logger.info(f"Событие удалено: {event_url}")
                return True
                
        except Exception as e:
            logger.error(f"Ошибка удаления события: {e}")
            return False
    
    def delete_event_instance(self, event_url: str, instance_date: date) -> bool:
        """
        Удаление конкретного вхождения повторяющегося события по дате.
        Более удобный метод для удаления события на сегодня.
        """
        if not self.client:
            if not self._connect():
                return False
        
        try:
            calendar = self.get_default_calendar()
            if not calendar:
                return False
            
            # Получаем мастер-событие
            master_event = calendar.event_by_url(event_url)
            if not master_event:
                logger.error(f"Мастер-событие не найдено: {event_url}")
                return False
            
            # Парсим существующие данные
            event_data = master_event.data
            cal = Calendar.from_ical(event_data)
            
            # Находим VEVENT компонент
            vevent = None
            for component in cal.walk():
                if component.name == "VEVENT":
                    vevent = component
                    break
            
            if not vevent:
                return False
            
            # Создаем или обновляем EXDATE
            exdate = vevent.get('EXDATE', [])
            if not isinstance(exdate, list):
                exdate = [exdate]
            
            # Добавляем дату для исключения
            from icalendar import vDDDTypes
            dt_instance = datetime.combine(instance_date, datetime.min.time())
            # Приводим к UTC для CalDAV
            dt_instance_utc = dt_instance.replace(tzinfo=LOCAL_TZ).astimezone(pytz.UTC)
            exdate.append(vDDDTypes(dt_instance_utc))
            
            vevent['EXDATE'] = exdate
            
            # Сохраняем обновленное событие
            calendar.save_event(cal.to_ical())
            logger.info(f"Добавлено исключение для {instance_date} в событии {event_url}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка добавления исключения: {e}")
            return False
    
    def _generate_uid(self) -> str:
        """Генерация уникального ID для события"""
        import uuid
        return f"{uuid.uuid4()}@myuved.bot"
    
    def update_event(self, event_url: str, summary: str = None, 
                     start_time: datetime = None, end_time: datetime = None,
                     is_all_day: bool = None) -> bool:
        """Обновление события"""
        if not self.client:
            if not self._connect():
                return False
        
        try:
            calendar = self.get_default_calendar()
            if not calendar:
                return False
            
            event = calendar.event_by_url(event_url)
            if not event:
                return False
            
            event_data = event.data
            cal = Calendar.from_ical(event_data)
            
            for component in cal.walk():
                if component.name == "VEVENT":
                    if summary:
                        component['SUMMARY'] = summary
                    if start_time:
                        if is_all_day:
                            component['DTSTART'] = start_time.date()
                        else:
                            start_utc = start_time.astimezone(pytz.UTC)
                            component['DTSTART'] = start_utc
                    if end_time:
                        if is_all_day:
                            component['DTEND'] = end_time.date()
                        else:
                            end_utc = end_time.astimezone(pytz.UTC)
                            component['DTEND'] = end_utc
                    break
            
            calendar.save_event(cal.to_ical())
            logger.info(f"Событие обновлено: {event_url}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка обновления события: {e}")
            return False


class BotHandlers:
    def __init__(self):
        self.bot = Bot(token=TELEGRAM_TOKEN)
        self.storage = MemoryStorage()
        self.dp = Dispatcher(self.bot, storage=self.storage)
        self.dp.middleware.setup(LoggingMiddleware())
        self.caldav = CalDAVClient()
        self.exceptions_file = 'exceptions.json'
        self._load_exceptions()
        
        self._setup_handlers()
    
    def _load_exceptions(self):
        """Загрузка исключений для повторяющихся событий"""
        if os.path.exists(self.exceptions_file):
            try:
                with open(self.exceptions_file, 'r', encoding='utf-8') as f:
                    self.exceptions = json.load(f)
            except:
                self.exceptions = {}
        else:
            self.exceptions = {}
    
    def _save_exceptions(self):
        """Сохранение исключений для повторяющихся событий"""
        with open(self.exceptions_file, 'w', encoding='utf-8') as f:
            json.dump(self.exceptions, f, ensure_ascii=False, indent=2)
    
    def add_exception(self, event_uid: str, date_str: str):
        """Добавление исключения для повторяющегося события"""
        if event_uid not in self.exceptions:
            self.exceptions[event_uid] = []
        if date_str not in self.exceptions[event_uid]:
            self.exceptions[event_uid].append(date_str)
            self._save_exceptions()
            logger.info(f"Добавлено исключение для {event_uid} на {date_str}")
    
    def is_exception(self, event_uid: str, date_str: str) -> bool:
        """Проверка, является ли дата исключением для события"""
        return event_uid in self.exceptions and date_str in self.exceptions[event_uid]
    
    def _setup_handlers(self):
        """Настройка обработчиков команд"""
        
        # Команда /start
        @self.dp.message_handler(commands=['start'])
        async def cmd_start(message: types.Message):
            keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
            keyboard.add(KeyboardButton("📅 Сегодня"), KeyboardButton("📆 Завтра"))
            keyboard.add(KeyboardButton("➕ Добавить событие"), KeyboardButton("✏️ Редактировать"))
            keyboard.add(KeyboardButton("🇷🇺 Московское время"), KeyboardButton("ℹ️ Помощь"))
            
            await message.answer(
                "🌟 *Мой Уведомлятор Бот* 🌟\n\n"
                "Я помогу вам не забыть о важных делах!\n\n"
                "📌 *Команды:*\n"
                "• 📅 *Сегодня* - события на сегодня\n"
                "• 📆 *Завтра* - события на завтра\n"
                "• ➕ *Добавить событие* - создать новое\n"
                "• ✏️ *Редактировать* - изменить/удалить событие\n"
                "• 🇷🇺 *Московское время* - текущее время\n"
                "• ℹ️ *Помощь* - описание возможностей\n\n"
                "⏰ Бот учитывает *повторяющиеся события* и уведомляет вас вовремя!",
                reply_markup=keyboard,
                parse_mode='Markdown'
            )
        
        # Обработка кнопок основного меню
        @self.dp.message_handler(lambda message: message.text in ["📅 Сегодня", "📆 Завтра", "🇷🇺 Московское время", "ℹ️ Помощь"])
        async def handle_menu_buttons(message: types.Message):
            if message.text == "📅 Сегодня":
                await self.show_today_events(message)
            elif message.text == "📆 Завтра":
                await self.show_tomorrow_events(message)
            elif message.text == "🇷🇺 Московское время":
                now = datetime.now(LOCAL_TZ)
                await message.answer(f"🕐 *Московское время:* {now.strftime('%d.%m.%Y %H:%M:%S')}", parse_mode='Markdown')
            elif message.text == "ℹ️ Помощь":
                await self.show_help(message)
        
        @self.dp.message_handler(lambda message: message.text == "➕ Добавить событие")
        async def add_event_start(message: types.Message, state: FSMContext):
            await message.answer("✏️ *Введите название события:*", parse_mode='Markdown')
            await EventStates.waiting_for_title.set()
        
        @self.dp.message_handler(state=EventStates.waiting_for_title)
        async def add_event_title(message: types.Message, state: FSMContext):
            async with state.proxy() as data:
                data['title'] = message.text
            await message.answer("📅 *Введите дату (ДД.ММ.ГГГГ):*\nНапример: 25.12.2024", parse_mode='Markdown')
            await EventStates.waiting_for_date.set()
        
        @self.dp.message_handler(state=EventStates.waiting_for_date)
        async def add_event_date(message: types.Message, state: FSMContext):
            try:
                date_str = message.text.strip()
                event_date = datetime.strptime(date_str, "%d.%m.%Y").date()
                async with state.proxy() as data:
                    data['date'] = event_date
                
                keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
                keyboard.add(KeyboardButton("Целый день"), KeyboardButton("Указать время"))
                keyboard.add(KeyboardButton("❌ Отмена"))
                
                await message.answer("⏰ *Весь день события или указать время?*", reply_markup=keyboard, parse_mode='Markdown')
                await EventStates.waiting_for_time.set()
            except ValueError:
                await message.answer("❌ *Неверный формат!* Используйте ДД.ММ.ГГГГ", parse_mode='Markdown')
        
        @self.dp.message_handler(state=EventStates.waiting_for_time)
        async def add_event_time_type(message: types.Message, state: FSMContext):
            if message.text == "Целый день":
                async with state.proxy() as data:
                    data['is_all_day'] = True
                    data['start_time'] = datetime.combine(data['date'], datetime.min.time())
                    data['end_time'] = datetime.combine(data['date'] + timedelta(days=1), datetime.min.time())
                await self.ask_recurrence(message, state)
            elif message.text == "Указать время":
                async with state.proxy() as data:
                    data['is_all_day'] = False
                await message.answer("⏰ *Введите время начала (ЧЧ:ММ):*\nНапример: 14:30", parse_mode='Markdown')
                await EventStates.waiting_for_end_time.set()
            elif message.text == "❌ Отмена":
                await state.finish()
                await message.answer("❌ Создание события отменено", reply_markup=self.get_main_keyboard())
            else:
                await message.answer("❌ Пожалуйста, выберите один из вариантов")
        
        @self.dp.message_handler(state=EventStates.waiting_for_end_time)
        async def add_event_start_time(message: types.Message, state: FSMContext):
            try:
                time_str = message.text.strip()
                start_time = datetime.strptime(time_str, "%H:%M").time()
                async with state.proxy() as data:
                    data['start_time'] = datetime.combine(data['date'], start_time)
                await message.answer("⏰ *Введите время окончания (ЧЧ:ММ):*\nНапример: 15:30", parse_mode='Markdown')
                await EventStates.waiting_for_edit_title.set()
            except ValueError:
                await message.answer("❌ *Неверный формат!* Используйте ЧЧ:ММ", parse_mode='Markdown')
        
        @self.dp.message_handler(state=EventStates.waiting_for_edit_title)
        async def add_event_end_time(message: types.Message, state: FSMContext):
            try:
                time_str = message.text.strip()
                end_time = datetime.strptime(time_str, "%H:%M").time()
                async with state.proxy() as data:
                    data['end_time'] = datetime.combine(data['date'], end_time)
                await self.ask_recurrence(message, state)
            except ValueError:
                await message.answer("❌ *Неверный формат!* Используйте ЧЧ:ММ", parse_mode='Markdown')
        
        # Обработка повторений
        @self.dp.message_handler(state=EventStates.waiting_for_repeat)
        async def add_event_repeat(message: types.Message, state: FSMContext):
            repeat = message.text
            if repeat == "❌ Отмена":
                await state.finish()
                await message.answer("❌ Создание события отменено", reply_markup=self.get_main_keyboard())
                return
            
            async with state.proxy() as data:
                if repeat == "🔁 Без повторения":
                    data['recurrence'] = None
                    data['recurrence_until'] = None
                    await self.save_and_confirm(message, state)
                elif repeat in ["Ежедневно", "Еженедельно", "Ежемесячно", "Ежегодно"]:
                    repeat_map = {
                        "Ежедневно": "daily",
                        "Еженедельно": "weekly",
                        "Ежемесячно": "monthly",
                        "Ежегодно": "yearly"
                    }
                    data['recurrence'] = repeat_map[repeat]
                    
                    if repeat != "Ежедневно":
                        await message.answer("📅 *Введите последнюю дату повторения (ДД.ММ.ГГГГ):*\n*Необязательно* - нажмите 'Пропустить'", parse_mode='Markdown', reply_markup=self.get_skip_keyboard())
                        await EventStates.waiting_for_repeat_until.set()
                    else:
                        await message.answer("📅 *Введите последнюю дату повторения (ДД.ММ.ГГГГ):*\n*Необязательно* - нажмите 'Пропустить'", parse_mode='Markdown', reply_markup=self.get_skip_keyboard())
                        await EventStates.waiting_for_repeat_until.set()
                else:
                    await message.answer("❌ Пожалуйста, выберите вариант из меню")
        
        @self.dp.message_handler(state=EventStates.waiting_for_repeat_until)
        async def add_event_repeat_until(message: types.Message, state: FSMContext):
            if message.text == "⏭ Пропустить":
                async with state.proxy() as data:
                    data['recurrence_until'] = None
                await self.save_and_confirm(message, state)
                return
            
            try:
                date_str = message.text.strip()
                until_date = datetime.strptime(date_str, "%d.%m.%Y").date()
                async with state.proxy() as data:
                    data['recurrence_until'] = until_date
                await self.save_and_confirm(message, state)
            except ValueError:
                await message.answer("❌ *Неверный формат!* Используйте ДД.ММ.ГГГГ или нажмите 'Пропустить'", parse_mode='Markdown')
        
        @self.dp.message_handler(lambda message: message.text == "✏️ Редактировать")
        async def edit_events_list(message: types.Message, state: FSMContext):
            await self.show_editable_events(message, state)
        
        @self.dp.callback_query_handler(lambda c: c.data.startswith('edit_'))
        async def select_event_to_edit(callback_query: types.CallbackQuery, state: FSMContext):
            event_url = callback_query.data.replace('edit_', '')
            await callback_query.message.delete()
            
            # Ищем событие по URL
            events = self.caldav.get_events(datetime.now(LOCAL_TZ).date() - timedelta(days=30), 
                                           datetime.now(LOCAL_TZ).date() + timedelta(days=365))
            
            event = None
            for e in events:
                if e.get('url') == event_url:
                    event = e
                    break
            
            if not event:
                await callback_query.message.answer("❌ Событие не найдено")
                return
            
            async with state.proxy() as data:
                data['edit_event_url'] = event_url
                data['edit_event_data'] = event
            
            keyboard = InlineKeyboardMarkup(row_width=2)
            keyboard.add(
                InlineKeyboardButton("📝 Изменить название", callback_data="edit_title"),
                InlineKeyboardButton("📅 Изменить дату", callback_data="edit_date"),
                InlineKeyboardButton("⏰ Изменить время", callback_data="edit_time"),
                InlineKeyboardButton("🗑 Удалить событие", callback_data="delete_event")
            )
            
            start_str = event['start_time'].strftime('%d.%m.%Y %H:%M') if not event['is_all_day'] else event['start_time'].strftime('%d.%m.%Y')
            await callback_query.message.answer(
                f"✏️ *Редактирование:* {event['summary']}\n\n"
                f"📅 {start_str}\n"
                f"🔄 {'Повторяющееся' if event['is_recurring'] else 'Обычное'}",
                reply_markup=keyboard,
                parse_mode='Markdown'
            )
            await EventStates.waiting_for_edit_selection.set()
        
        @self.dp.callback_query_handler(lambda c: c.data in ['edit_title', 'edit_date', 'edit_time', 'delete_event'], state=EventStates.waiting_for_edit_selection)
        async def handle_edit_action(callback_query: types.CallbackQuery, state: FSMContext):
            action = callback_query.data
            async with state.proxy() as data:
                event_url = data.get('edit_event_url')
                event_data = data.get('edit_event_data')
            
            if action == 'edit_title':
                await callback_query.message.answer("✏️ *Введите новое название:*", parse_mode='Markdown')
                await EventStates.waiting_for_edit_title.set()
            
            elif action == 'edit_date':
                await callback_query.message.answer("📅 *Введите новую дату (ДД.ММ.ГГГГ):*", parse_mode='Markdown')
                await EventStates.waiting_for_edit_date.set()
            
            elif action == 'edit_time':
                if event_data['is_all_day']:
                    await callback_query.message.answer("⏰ *Это событие целый день. Хотите изменить на конкретное время?*\nВведите время начала (ЧЧ:ММ) или 'целый день'", parse_mode='Markdown')
                else:
                    await callback_query.message.answer("⏰ *Введите новое время начала (ЧЧ:ММ):*", parse_mode='Markdown')
                await EventStates.waiting_for_edit_time.set()
            
            elif action == 'delete_event':
                await callback_query.message.answer(
                    "⚠️ *Удалить это событие?*\n\n"
                    "📍 *Важно:* Для повторяющихся событий - удалится ТОЛЬКО это вхождение (на эту дату).\n"
                    "Для удаления всех повторений - используйте CalDAV клиент.",
                    reply_markup=self.get_confirm_delete_keyboard(),
                    parse_mode='Markdown'
                )
                await EventStates.waiting_for_delete_selection.set()
            
            await callback_query.answer()
        
        @self.dp.message_handler(state=EventStates.waiting_for_edit_title)
        async def save_edit_title(message: types.Message, state: FSMContext):
            new_title = message.text
            async with state.proxy() as data:
                event_url = data.get('edit_event_url')
            
            if self.caldav.update_event(event_url, summary=new_title):
                await message.answer(f"✅ *Название изменено на:* {new_title}", parse_mode='Markdown')
            else:
                await message.answer("❌ *Ошибка при изменении названия*", parse_mode='Markdown')
            
            await state.finish()
            await self.show_editable_events(message, state)
        
        @self.dp.message_handler(state=EventStates.waiting_for_edit_date)
        async def save_edit_date(message: types.Message, state: FSMContext):
            try:
                new_date = datetime.strptime(message.text.strip(), "%d.%m.%Y")
                async with state.proxy() as data:
                    event_url = data.get('edit_event_url')
                    event_data = data.get('edit_event_data')
                
                start_time = datetime.combine(new_date.date(), event_data['start_time'].time())
                end_time = datetime.combine(new_date.date(), event_data['end_time'].time())
                
                if self.caldav.update_event(event_url, start_time=start_time, end_time=end_time):
                    await message.answer(f"✅ *Дата изменена на:* {message.text}", parse_mode='Markdown')
                else:
                    await message.answer("❌ *Ошибка при изменении даты*", parse_mode='Markdown')
            except ValueError:
                await message.answer("❌ *Неверный формат!* Используйте ДД.ММ.ГГГГ", parse_mode='Markdown')
                return
            
            await state.finish()
            await self.show_editable_events(message, state)
        
        @self.dp.message_handler(state=EventStates.waiting_for_edit_time)
        async def save_edit_time(message: types.Message, state: FSMContext):
            async with state.proxy() as data:
                event_url = data.get('edit_event_url')
                event_data = data.get('edit_event_data')
            
            if message.text.lower() == "целый день":
                # Меняем на целый день
                new_start = datetime.combine(event_data['start_time'].date(), datetime.min.time())
                new_end = datetime.combine(event_data['start_time'].date() + timedelta(days=1), datetime.min.time())
                if self.caldav.update_event(event_url, start_time=new_start, end_time=new_end, is_all_day=True):
                    await message.answer("✅ *Событие изменено на 'целый день'*", parse_mode='Markdown')
                else:
                    await message.answer("❌ *Ошибка при изменении*", parse_mode='Markdown')
            else:
                try:
                    time_str = message.text.strip()
                    new_time = datetime.strptime(time_str, "%H:%M").time()
                    async with state.proxy() as data:
                        event_data = data.get('edit_event_data')
                    
                    # Рассчитываем новое время окончания (сохраняем длительность)
                    duration = event_data['end_time'] - event_data['start_time']
                    new_start = datetime.combine(event_data['start_time'].date(), new_time)
                    new_end = new_start + duration
                    
                    if self.caldav.update_event(event_url, start_time=new_start, end_time=new_end, is_all_day=False):
                        await message.answer(f"✅ *Время изменено на:* {new_start.strftime('%H:%M')} - {new_end.strftime('%H:%M')}", parse_mode='Markdown')
                    else:
                        await message.answer("❌ *Ошибка при изменении времени*", parse_mode='Markdown')
                except ValueError:
                    await message.answer("❌ *Неверный формат!* Используйте ЧЧ:ММ", parse_mode='Markdown')
                    return
            
            await state.finish()
            await self.show_editable_events(message, state)
        
        @self.dp.message_handler(state=EventStates.waiting_for_delete_selection)
        async def confirm_delete(message: types.Message, state: FSMContext):
            if message.text == "✅ Да, удалить":
                async with state.proxy() as data:
                    event_url = data.get('edit_event_url')
                    event_data = data.get('edit_event_data')
                
                # Получаем сегодняшнюю дату
                today = datetime.now(LOCAL_TZ).date()
                
                # Определяем, нужно ли удалять только конкретное вхождение
                if event_data['is_recurring']:
                    # Для повторяющихся событий - удаляем только экземпляр на сегодня
                    # Используем метод delete_event_instance
                    if self.caldav.delete_event_instance(event_url, today):
                        # Сохраняем в локальный файл исключений для быстрой проверки
                        self.add_exception(event_data['uid'], today.strftime('%Y%m%d'))
                        await message.answer(
                            f"✅ *Успешно!*\n\n"
                            f"Событие *«{event_data['summary']}»* на {today.strftime('%d.%m.%Y')} удалено.\n\n"
                            f"📌 *Повторяющееся событие продолжит действовать в другие дни.*",
                            parse_mode='Markdown'
                        )
                    else:
                        await message.answer("❌ *Ошибка при удалении события*", parse_mode='Markdown')
                else:
                    # Для обычных событий - удаляем полностью
                    if self.caldav.delete_event(event_url):
                        await message.answer(f"✅ *Событие «{event_data['summary']}» успешно удалено!*", parse_mode='Markdown')
                    else:
                        await message.answer("❌ *Ошибка при удалении события*", parse_mode='Markdown')
            else:
                await message.answer("❌ Удаление отменено", reply_markup=self.get_main_keyboard())
            
            await state.finish()
        
        @self.dp.callback_query_handler(lambda c: c.data.startswith('done_'))
        async def mark_event_done(callback_query: types.CallbackQuery):
            """Отметка события как выполненного - удаление конкретного вхождения"""
            short_id = callback_query.data.replace('done_', '')
            await callback_query.message.delete()
            
            # Ищем событие в сообщении
            for event in self.get_today_events_raw():
                if self._get_short_uid(event['uid']) == short_id:
                    today = datetime.now(LOCAL_TZ).date()
                    
                    if event['is_recurring']:
                        # Для повторяющихся - удаляем только сегодняшнее вхождение
                        if self.caldav.delete_event_instance(event['url'], today):
                            self.add_exception(event['uid'], today.strftime('%Y%m%d'))
                            await callback_query.message.answer(
                                f"✅ *Отлично!*\n\n"
                                f"Событие *«{event['summary']}»* на {today.strftime('%d.%m.%Y')} отмечено как выполненное и удалено.\n\n"
                                f"📌 Повторение сохранено для будущих дней.",
                                parse_mode='Markdown'
                            )
                        else:
                            await callback_query.message.answer(
                                f"❌ *Ошибка при удалении события*\n\n"
                                f"Не удалось удалить событие «{event['summary']}»",
                                parse_mode='Markdown'
                            )
                    else:
                        # Для обычных - удаляем полностью
                        if self.caldav.delete_event(event['url']):
                            await callback_query.message.answer(
                                f"✅ *Отлично!*\n\n"
                                f"Событие *«{event['summary']}»* отмечено как выполненное и удалено.",
                                parse_mode='Markdown'
                            )
                        else:
                            await callback_query.message.answer(
                                f"❌ *Ошибка при удалении события*\n\n"
                                f"Не удалось удалить событие «{event['summary']}»",
                                parse_mode='Markdown'
                            )
                    break
            
            await callback_query.answer()
    
    def get_main_keyboard(self):
        keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
        keyboard.add(KeyboardButton("📅 Сегодня"), KeyboardButton("📆 Завтра"))
        keyboard.add(KeyboardButton("➕ Добавить событие"), KeyboardButton("✏️ Редактировать"))
        keyboard.add(KeyboardButton("🇷🇺 Московское время"), KeyboardButton("ℹ️ Помощь"))
        return keyboard
    
    def get_skip_keyboard(self):
        keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
        keyboard.add(KeyboardButton("⏭ Пропустить"))
        return keyboard
    
    def get_confirm_delete_keyboard(self):
        keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
        keyboard.add(KeyboardButton("✅ Да, удалить"), KeyboardButton("❌ Нет, отмена"))
        return keyboard
    
    async def ask_recurrence(self, message: types.Message, state: FSMContext):
        keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
        keyboard.add(
            KeyboardButton("🔁 Без повторения"),
            KeyboardButton("Ежедневно"),
            KeyboardButton("Еженедельно")
        )
        keyboard.add(
            KeyboardButton("Ежемесячно"),
            KeyboardButton("Ежегодно")
        )
        keyboard.add(KeyboardButton("❌ Отмена"))
        
        await message.answer(
            "🔄 *Как часто повторять событие?*\n\n"
            "• 🔁 Без повторения - однократное\n"
            "• Ежедневно - каждый день\n"
            "• Еженедельно - каждую неделю\n"
            "• Ежемесячно - каждый месяц\n"
            "• Ежегодно - каждый год",
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
        await EventStates.waiting_for_repeat.set()
    
    async def save_and_confirm(self, message: types.Message, state: FSMContext):
        async with state.proxy() as data:
            title = data['title']
            start_time = data['start_time']
            end_time = data['end_time']
            is_all_day = data['is_all_day']
            recurrence = data.get('recurrence')
            recurrence_until = data.get('recurrence_until')
        
        # Сохраняем в CalDAV
        success = self.caldav.add_event(
            summary=title,
            start_time=start_time,
            end_time=end_time,
            is_all_day=is_all_day,
            recurrence=recurrence,
            recurrence_until=recurrence_until
        )
        
        if success:
            await message.answer(
                f"✅ *Событие успешно добавлено!*\n\n"
                f"📌 *Название:* {title}\n"
                f"📅 *Дата/время:* {start_time.strftime('%d.%m.%Y %H:%M') if not is_all_day else start_time.strftime('%d.%m.%Y')}\n"
                f"🔄 *Повторение:* {recurrence if recurrence else 'Нет'}",
                parse_mode='Markdown',
                reply_markup=self.get_main_keyboard()
            )
        else:
            await message.answer(
                "❌ *Ошибка при добавлении события!*\nПроверьте подключение к CalDAV.",
                parse_mode='Markdown',
                reply_markup=self.get_main_keyboard()
            )
        
        await state.finish()
    
    def get_today_events_raw(self) -> List[Dict]:
        """Получение событий на сегодня (сырые данные)"""
        today = datetime.now(LOCAL_TZ).date()
        tomorrow = today + timedelta(days=1)
        
        events = self.caldav.get_events(today, tomorrow, include_recurring=True)
        
        # Фильтруем исключения
        filtered_events = []
        today_str = today.strftime('%Y%m%d')
        
        for event in events:
            # Проверяем, не отмечено ли это вхождение как выполненное
            if event['is_recurring'] and self.is_exception(event['uid'], today_str):
                continue
            filtered_events.append(event)
        
        return filtered_events
    
    async def show_today_events(self, message: types.Message):
        """Показать события на сегодня"""
        today = datetime.now(LOCAL_TZ).date()
        events = self.get_today_events_raw()
        
        if not events:
            await message.answer(
                f"📅 *События на {today.strftime('%d.%m.%Y')}*\n\n"
                "✨ Нет запланированных событий на сегодня!",
                parse_mode='Markdown',
                reply_markup=self.get_main_keyboard()
            )
            return
        
        # Сортируем по времени начала
        events.sort(key=lambda x: x['start_time'])
        
        # Сначала показываем просроченные
        now = datetime.now(LOCAL_TZ)
        overdue = []
        upcoming = []
        
        for event in events:
            event_time = event['start_time']
            if event_time < now and not event['is_all_day']:
                overdue.append(event)
            else:
                upcoming.append(event)
        
        message_text = f"📅 *События на {today.strftime('%d.%m.%Y')}*\n\n"
        
        if overdue:
            message_text += "⚠️ *ПРОСРОЧЕННЫЕ:*\n"
            for event in overdue:
                time_str = event['start_time'].strftime('%H:%M')
                message_text += f"• `{time_str}` {event['summary']}\n"
            message_text += "\n"
        
        if upcoming:
            message_text += "🕐 *ПРЕДСТОЯЩИЕ:*\n"
            for event in upcoming:
                if event['is_all_day']:
                    time_str = "Целый день"
                else:
                    time_str = event['start_time'].strftime('%H:%M')
                recurring_mark = " 🔁" if event['is_recurring'] else ""
                message_text += f"• `{time_str}` {event['summary']}{recurring_mark}\n"
        
        message_text += "\n✅ *Нажмите кнопку ниже, чтобы отметить событие как выполненное*"
        
        # Создаем inline кнопки для каждого события
        keyboard = InlineKeyboardMarkup(row_width=1)
        for event in events:
            short_uid = self._get_short_uid(event['uid'])
            keyboard.add(InlineKeyboardButton(
                f"✅ {event['summary'][:40]}",
                callback_data=f"done_{short_uid}"
            ))
        
        await message.answer(message_text, parse_mode='Markdown', reply_markup=keyboard)
    
    async def show_tomorrow_events(self, message: types.Message):
        """Показать события на завтра"""
        tomorrow = datetime.now(LOCAL_TZ).date() + timedelta(days=1)
        day_after = tomorrow + timedelta(days=1)
        
        events = self.caldav.get_events(tomorrow, day_after, include_recurring=True)
        
        # Фильтруем исключения
        tomorrow_str = tomorrow.strftime('%Y%m%d')
        filtered_events = []
        for event in events:
            if event['is_recurring'] and self.is_exception(event['uid'], tomorrow_str):
                continue
            filtered_events.append(event)
        
        if not filtered_events:
            await message.answer(
                f"📆 *События на {tomorrow.strftime('%d.%m.%Y')}*\n\n"
                "✨ Нет запланированных событий на завтра!",
                parse_mode='Markdown',
                reply_markup=self.get_main_keyboard()
            )
            return
        
        filtered_events.sort(key=lambda x: x['start_time'])
        
        message_text = f"📆 *События на {tomorrow.strftime('%d.%m.%Y')}*\n\n"
        
        for event in filtered_events:
            if event['is_all_day']:
                time_str = "📅 Целый день"
            else:
                time_str = f"🕐 {event['start_time'].strftime('%H:%M')}"
            recurring_mark = " 🔁" if event['is_recurring'] else ""
            message_text += f"{time_str} • *{event['summary']}*{recurring_mark}\n"
        
        await message.answer(message_text, parse_mode='Markdown', reply_markup=self.get_main_keyboard())
    
    async def show_editable_events(self, message: types.Message, state: FSMContext):
        """Показать список событий для редактирования"""
        today = datetime.now(LOCAL_TZ).date()
        next_month = today + timedelta(days=30)
        
        events = self.caldav.get_events(today, next_month, include_recurring=True)
        
        if not events:
            await message.answer(
                "✏️ *Нет событий для редактирования*",
                parse_mode='Markdown',
                reply_markup=self.get_main_keyboard()
            )
            return
        
        # Группируем повторяющиеся события и показываем только мастер
        unique_events = {}
        for event in events:
            uid = event['uid']
            if uid not in unique_events:
                unique_events[uid] = event
        
        message_text = "✏️ *Выберите событие для редактирования:*\n\n"
        keyboard = InlineKeyboardMarkup(row_width=1)
        
        for uid, event in unique_events.items():
            start_str = event['start_time'].strftime('%d.%m') if not event['is_all_day'] else event['start_time'].strftime('%d.%m')
            recurring_mark = " 🔁" if event['is_recurring'] else ""
            keyboard.add(InlineKeyboardButton(
                f"📝 {start_str} {event['summary'][:35]}{recurring_mark}",
                callback_data=f"edit_{event['url']}"
            ))
        
        await message.answer(message_text, parse_mode='Markdown', reply_markup=keyboard)
    
    def _get_short_uid(self, uid: str) -> str:
        """Получение короткого ID для UID"""
        import hashlib
        return hashlib.md5(uid.encode()).hexdigest()[:12]
    
    async def show_help(self, message: types.Message):
        """Показать помощь"""
        help_text = """
ℹ️ *Помощь по боту*

📌 *Основные команды:*
• 📅 *Сегодня* - список событий на сегодня
• 📆 *Завтра* - список событий на завтра
• ➕ *Добавить событие* - создать новое событие
• ✏️ *Редактировать* - изменить или удалить событие
• 🇷🇺 *Московское время* - текущее время

✨ *Возможности:*
• *Повторяющиеся события* - ежедневно, еженедельно, ежемесячно, ежегодно
• *Целый день* - события без привязки ко времени
• *Удаление конкретного дня* - для повторяющихся событий можно удалить только одно вхождение
• *Inline кнопки* - удобная отметка выполненных дел

⏰ *Уведомления:*
Бот автоматически присылает уведомления за 5 минут до события
и повторяет через 5 минут, если событие не отмечено выполненным.

📱 *Разработано для управления задачами через CalDAV*
        """
        await message.answer(help_text, parse_mode='Markdown', reply_markup=self.get_main_keyboard())
    
    def run(self):
        """Запуск бота"""
        logger.info("=" * 50)
        logger.info(f"🤖 БОТ v3.6 ЗАПУЩЕН")
        logger.info("=" * 50)
        logger.info(f"CalDAV: ✅ Подключено")
        logger.info(f"Часовой пояс: {TIMEZONE}")
        logger.info(f"Текущее время: {datetime.now(LOCAL_TZ).strftime('%d.%m.%Y %H:%M:%S')}")
        logger.info("=" * 50)
        logger.info("✅ Бот готов")
        
        executor.start_polling(self.dp, skip_updates=True)


if __name__ == "__main__":
    bot = BotHandlers()
    bot.run()