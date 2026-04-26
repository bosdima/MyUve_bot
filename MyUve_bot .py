import telebot
from telebot import types
import sqlite3
import os
from datetime import datetime, timedelta
import pytz

# --- КОНФИГУРАЦИЯ ---
BOT_TOKEN = 'ВАШ_ТОКЕН_ЗДЕСЬ'  # Вставьте токен от @BotFather
DB_NAME = 'tasks.db'
TIMEZONE_MOSCOW = pytz.timezone('Europe/Moscow')

# Инициализация бота
bot = telebot.TeleBot(BOT_TOKEN)

# --- РАБОТА С БАЗОЙ ДАННЫХ ---
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            due_date TEXT NOT NULL,
            due_time TEXT NOT NULL,
            status TEXT DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def add_task(title, due_date, due_time):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO tasks (title, due_date, due_time)
        VALUES (?, ?, ?)
    ''', (title, due_date, due_time))
    conn.commit()
    task_id = cursor.lastrowid
    conn.close()
    return task_id

def get_all_tasks():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('SELECT id, title, due_date, due_time FROM tasks WHERE status="active" ORDER BY due_date, due_time')
    tasks = cursor.fetchall()
    conn.close()
    return tasks

def delete_task(task_id):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('UPDATE tasks SET status="deleted" WHERE id=?', (task_id,))
    conn.commit()
    conn.close()

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def get_moscow_time_str():
    """Возвращает текущее время в Москве в формате ДД.ММ.ГГГГ ЧЧ:ММ:СС"""
    now_msk = datetime.now(TIMEZONE_MOSCOW)
    return now_msk.strftime('%d.%m.%Y %H:%M:%S')

def get_week_range():
    """Возвращает строку с диапазоном текущей недели (Пн - Вс)"""
    now = datetime.now(TIMEZONE_MOSCOW)
    start_of_week = now - timedelta(days=now.weekday()) # Понедельник
    end_of_week = start_of_week + timedelta(days=6)     # Воскресенье
    
    start_str = start_of_week.strftime('%d.%m')
    end_str = end_of_week.strftime('%d.%m')
    
    # Определяем названия дней недели
    days_ru = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']
    day_name_start = days_ru[start_of_week.weekday()]
    day_name_end = days_ru[end_of_week.weekday()]
    
    return f"{day_name_start}, {start_str} — {day_name_end}, {end_str}"

def format_task_for_display(task):
    """Форматирует задачу для красивого вывода"""
    task_id, title, due_date, due_time = task
    
    # Парсим дату для определения дня недели
    try:
        date_obj = datetime.strptime(due_date, '%Y-%m-%d')
        days_ru = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']
        day_name = days_ru[date_obj.weekday()]
        date_display = date_obj.strftime('%d.%m')
    except ValueError:
        day_name = ""
        date_display = due_date

    # Определение цвета/статуса (упрощенно)
    # Если задача на сегодня или просрочена - красный, иначе зеленый/желтый
    today = datetime.now(TIMEZONE_MOSCOW).date()
    if date_obj.date() < today:
        icon = "🔴" # Просрочено
    elif date_obj.date() == today:
        icon = "🟡" # Сегодня
    else:
        icon = "🟢" # Будущее

    return f"{icon} {due_time} — {title} ({day_name}, {date_display})"

# --- КЛАВИАТУРЫ ---
def create_main_keyboard():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    # Убрали дублирующие кнопки "+ Добавить" и "Настройки", оставили только уникальные действия
    btn_add = types.KeyboardButton('+ Добавить заметку')
    btn_settings = types.KeyboardButton('⚙️ Настройки')
    btn_delete_mode = types.KeyboardButton('🗑 Управление (Удалить)')
    btn_refresh = types.KeyboardButton('🔄 Обновить')
    
    markup.add(btn_add, btn_settings)
    markup.add(btn_delete_mode, btn_refresh)
    return markup

def create_inline_task_list(tasks):
    """Создает инлайн-клавиатуру со списком задач для удаления"""
    markup = types.InlineKeyboardMarkup(row_width=1)
    
    if not tasks:
        return None

    for task in tasks:
        task_id, title, due_date, due_time = task
        # Отображаем короткую версию задачи в кнопке
        button_text = f"❌ {title} ({due_time})"
        callback_data = f"delete_{task_id}"
        markup.add(types.InlineKeyboardButton(text=button_text, callback_data=callback_data))
        
    # Кнопка отмены
    markup.add(types.InlineKeyboardButton(text="↩️ Отмена", callback_data="cancel_delete"))
    
    return markup

# --- ОБРАБОТЧИКИ КОМАНД И СООБЩЕНИЙ ---

@bot.message_handler(commands=['start'])
def send_welcome(message):
    init_db()
    show_main_menu(message.chat.id)

@bot.message_handler(func=lambda message: message.text == '+ Добавить заметку')
def start_add_task(message):
    msg = bot.send_message(message.chat.id, "✍️ Введите название новой задачи:")
    bot.register_next_step_handler(msg, process_add_task_title)

def process_add_task_title(message):
    title = message.text
    if not title:
        bot.send_message(message.chat.id, "❌ Название не может быть пустым.")
        return
        
    msg = bot.send_message(message.chat.id, "📅 Введите дату выполнения (в формате ДД.ММ.ГГГГ или просто 'завтра', 'послезавтра'):")
    bot.register_next_step_handler(msg, process_add_task_date, title)

def process_add_task_date(message, title):
    date_input = message.text.lower().strip()
    today = datetime.now(TIMEZONE_MOSCOW).date()
    
    try:
        if date_input == 'завтра':
            due_date = (today + timedelta(days=1)).strftime('%Y-%m-%d')
        elif date_input == 'послезавтра':
            due_date = (today + timedelta(days=2)).strftime('%Y-%m-%d')
        else:
            # Пробуем парсить формат ДД.ММ.ГГГГ
            due_date_obj = datetime.strptime(date_input, '%d.%m.%Y')
            due_date = due_date_obj.strftime('%Y-%m-%d')
    except ValueError:
        bot.send_message(message.chat.id, "❌ Неверный формат даты. Используйте ДД.ММ.ГГГГ или слова 'завтра'/'послезавтра'.")
        return

    msg = bot.send_message(message.chat.id, "⏰ Введите время выполнения (в формате ЧЧ:ММ):")
    bot.register_next_step_handler(msg, process_add_task_time, title, due_date)

def process_add_task_time(message, title, due_date):
    time_input = message.text.strip()
    try:
        # Проверка формата времени
        datetime.strptime(time_input, '%H:%M')
    except ValueError:
        bot.send_message(message.chat.id, "❌ Неверный формат времени. Используйте ЧЧ:ММ (например, 18:30).")
        return

    add_task(title, due_date, time_input)
    bot.send_message(message.chat.id, "✅ Задача успешно добавлена!")
    show_main_menu(message.chat.id)

@bot.message_handler(func=lambda message: message.text == '🗑 Управление (Удалить)')
def start_delete_process(message):
    tasks = get_all_tasks()
    
    if not tasks:
        bot.send_message(message.chat.id, "📭 Список задач пуст. Нечего удалять.")
        return

    bot.send_message(message.chat.id, "Выберите задачу для удаления:", reply_markup=create_inline_task_list(tasks))

@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call):
    if call.data.startswith('delete_'):
        task_id = int(call.data.split('_')[1])
        delete_task(task_id)
        bot.answer_callback_query(call.id, "Задача удалена")
        bot.edit_message_text(
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            text="✅ Задача удалена.",
            reply_markup=None
        )
        # Показываем обновленное главное меню
        show_main_menu(call.message.chat.id)
        
    elif call.data == 'cancel_delete':
        bot.edit_message_text(
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            text="❌ Удаление отменено.",
            reply_markup=None
        )
        show_main_menu(call.message.chat.id)

@bot.message_handler(func=lambda message: message.text == '🔄 Обновить')
def refresh_view(message):
    show_main_menu(message.chat.id)

@bot.message_handler(func=lambda message: message.text == '⚙️ Настройки')
def open_settings(message):
    bot.send_message(message.chat.id, "⚙️ Раздел настроек в разработке.\nЗдесь можно будет настроить уведомления и часовой пояс.")

# --- ГЛАВНОЕ МЕНЮ ---
def show_main_menu(chat_id):
    tasks = get_all_tasks()
    week_range = get_week_range()
    current_time_msk = get_moscow_time_str()
    
    # Формируем текст списка задач
    if tasks:
        tasks_list = "\n".join([format_task_for_display(task) for task in tasks])
    else:
        tasks_list = "📭 Нет активных задач."

    full_text = (
        f"🤖 *Бот запущен! Версия: 1.3.1*\n"
        f"📅 Период: {week_range}\n\n"
        f"{tasks_list}\n\n"
        f"🕒 Последняя синхронизация: {current_time_msk}"
    )
    
    bot.send_message(chat_id, full_text, parse_mode='Markdown', reply_markup=create_main_keyboard())

# --- ЗАПУСК ---
if __name__ == '__main__':
    init_db()
    print("Бот запущен...")
    bot.polling(none_stop=True)