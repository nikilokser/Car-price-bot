import telebot
import atexit
import difflib
import pandas as pd
import time
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from neo4j import GraphDatabase
from llama_analyzer import get_liquidity_analysis
from catboost_model import predict_car_price
import os

# Проверяем, используется ли Docker (dotenv только для локальной разработки)
if os.getenv('DOCKER_ENV') != 'true':
    from dotenv import load_dotenv
    load_dotenv()

# configuration load
TOKEN = os.getenv("BOT_TOKEN")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j-db:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")

print(f"🔌 Подключение к Neo4j: {NEO4J_URI}")
print(f"👤 Пользователь Neo4j: {NEO4J_USER}")

bot = telebot.TeleBot(TOKEN)

def wait_for_neo4j(max_attempts=60, delay=5):
    """Ожидание готовности Neo4j с повторными попытками подключения"""
    for attempt in range(max_attempts):
        try:
            print(f"🔄 Попытка подключения к Neo4j #{attempt + 1}/{max_attempts}")
            test_driver = GraphDatabase.driver(
                NEO4J_URI, 
                auth=(NEO4J_USER, NEO4J_PASSWORD),
                connection_timeout=10,
                max_connection_lifetime=30
            )
            with test_driver.session() as session:
                result = session.run("RETURN 'Connection successful' AS status")
                record = result.single()
                if record and record["status"] == "Connection successful":
                    print("✅ Neo4j готов к работе!")
                    test_driver.close()
                    return True
            test_driver.close()
        except Exception as e:
            print(f"⏳ Neo4j не готов (попытка {attempt + 1}): {e}")
            if attempt < max_attempts - 1:
                time.sleep(delay)
    
    print("❌ Не удалось дождаться готовности Neo4j")
    return False

# Ожидаем готовности Neo4j
print("🔌 Ожидание готовности Neo4j...")
if wait_for_neo4j():
    try:
        driver = GraphDatabase.driver(
            NEO4J_URI, 
            auth=(NEO4J_USER, NEO4J_PASSWORD),
            connection_timeout=15,
            max_connection_lifetime=300
        )
        # Проверяем подключение
        with driver.session() as session:
            result = session.run("RETURN 'Connection successful' AS status")
            print("✅ Подключение к Neo4j успешно!")
    except Exception as e:
        print(f"❌ Ошибка подключения к Neo4j: {e}")
        driver = None
else:
    print("❌ Запуск без подключения к Neo4j")
    driver = None

@atexit.register
def cleanup():
    if driver:
        driver.close()

# temp storage
user_states = {}

# inline buttons
markup = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
markup.add(KeyboardButton("/start"), KeyboardButton("/help"))

def normalize_transmission(value):
    replacements = {"А": "A", "а": "a", "Т": "T", "т": "t", "М": "M", "м": "m"}
    return ''.join(replacements.get(ch, ch) for ch in value).upper().strip()

def get_all_titles():
    if not driver:
        return []
    try:
        with driver.session() as session:
            result = session.run("MATCH (c:Car) RETURN DISTINCT toLower(c.title) AS title")
            return [record["title"] for record in result]
    except Exception as e:
        print(f"❌ Ошибка получения списка автомобилей: {e}")
        return []

def get_years_by_model(title):
    if not driver:
        return []
    try:
        with driver.session() as session:
            result = session.run("""
                MATCH (c:Car)-[:HAS_YEAR]->(y:Year)
                WHERE c.title = $title
                RETURN DISTINCT y.value AS year
                ORDER BY y.value DESC
            """, title=title)
            return [record["year"] for record in result]
    except Exception as e:
        print(f"❌ Ошибка получения годов: {e}")
        return []

def get_transmissions(title, year):
    if not driver:
        return []
    try:
        with driver.session() as session:
            result = session.run("""
                MATCH (c:Car)-[:HAS_YEAR]->(:Year {value: $year}),
                      (c)-[:HAS_TRANSMISSION]->(t:Transmission)
                WHERE c.title = $title
                RETURN DISTINCT t.type AS transmission
            """, title=title, year=year)
            return [record["transmission"] for record in result]
    except Exception as e:
        print(f"❌ Ошибка получения трансмиссий: {e}")
        return []

def get_drives(title, year, transmission):
    if not driver:
        return []
    try:
        with driver.session() as session:
            result = session.run("""
                MATCH (c:Car)-[:HAS_YEAR]->(:Year {value: $year}),
                      (c)-[:HAS_TRANSMISSION]->(:Transmission {type: $transmission}),
                      (c)-[:HAS_DRIVE]->(d:Drive)
                WHERE c.title = $title
                RETURN DISTINCT d.type AS drive
            """, title=title, year=year, transmission=transmission)
            return [record["drive"] for record in result]
    except Exception as e:
        print(f"❌ Ошибка получения типов привода: {e}")
        return []

def get_colors(title, year, transmission, drive):
    if not driver:
        return []
    try:
        with driver.session() as session:
            result = session.run("""
                MATCH (c:Car)-[:HAS_YEAR]->(:Year {value: $year}),
                      (c)-[:HAS_TRANSMISSION]->(:Transmission {type: $transmission}),
                      (c)-[:HAS_DRIVE]->(:Drive {type: $drive}),
                      (c)-[:HAS_COLOR]->(clr:Color)
                WHERE c.title = $title
                RETURN DISTINCT clr.name AS color
            """, title=title, year=year, transmission=transmission, drive=drive)
            return [record["color"] for record in result]
    except Exception as e:
        print(f"❌ Ошибка получения цветов: {e}")
        return []

def get_car_features(state):
    if not driver:
        return pd.DataFrame()
    try:
        with driver.session() as session:
            result = session.run("""
                MATCH (c:Car)-[:HAS_YEAR]->(y:Year {value: $year}),
                      (c)-[:HAS_TRANSMISSION]->(t:Transmission {type: $transmission}),
                      (c)-[:HAS_DRIVE]->(d:Drive {type: $drive}),
                      (c)-[:HAS_COLOR]->(clr:Color),
                      (c)-[:HAS_BODY]->(b:BodyType),
                      (c)-[:HAS_ENGINE]->(e:Engine),
                      (c)-[:HAS_ENV_STANDARD]->(env:EnvStandard),
                      (c)-[:HAS_FUEL_TYPE]->(f:Fuel),
                      (c)-[:FROM_AUCTION]->(a:Auction),
                      (c)-[:HAS_MILEAGE]->(m:Mileage),
                      (c)-[:HAS_POWER]->(p:Power),
                      (c)-[:HAS_CHINA_PRICE]->(cp:ChinaPrice)
                WHERE c.title = $title AND clr.name = $color
                RETURN a.location AS auction, b.type AS body_type, clr.name AS color, 
                       d.type AS drive_type, e.code AS engine, e.volume AS engine_volume,
                       env.standard AS environmental_standards, f.type AS fuel_type,
                       m.value AS mileage, p.value AS power, c.title AS title,
                       t.type AS transmission, y.value AS year, cp.value AS china_price,
                       c.url AS url
            """, state)
            return pd.DataFrame([record.data() for record in result])
    except Exception as e:
        print(f"❌ Ошибка получения характеристик автомобиля: {e}")
        return pd.DataFrame()

def predict_price(state):
    df = get_car_features(state)
    if df.empty:
        return None, []

    predicted_price = predict_car_price(df)

    if not driver:
        return predicted_price, []

    try:
        with driver.session() as session:
            result = session.run("""
                MATCH (c:Car)-[:HAS_YEAR]->(:Year {value: $year}),
                      (c)-[:HAS_TRANSMISSION]->(:Transmission {type: $transmission}),
                      (c)-[:HAS_DRIVE]->(:Drive {type: $drive}),
                      (c)-[:HAS_COLOR]->(:Color {name: $color}),
                      (c)-[:HAS_CHINA_PRICE]->(cp:ChinaPrice)
                WHERE c.title = $title AND cp.value < $predicted_price
                RETURN c.url AS url
            """, {
                "title": state["title"],
                "year": state["year"],
                "transmission": state["transmission"],
                "drive": state["drive"],
                "color": state["color"],
                "predicted_price": predicted_price
            })

            cheaper_urls = [record["url"] for record in result if record["url"]]

        return predicted_price, cheaper_urls
    except Exception as e:
        print(f"❌ Ошибка прогнозирования цены: {e}")
        return predicted_price, []

@bot.message_handler(commands=['start'])
def send_welcome(message):
    welcome_text = (
        "🚗✨ *Добро пожаловать в умного помощника по китайским аукционам автомобилей!* ✨🚗\n\n"
        "Я здесь, чтобы помочь вам сделать лучший выбор и получить максимум информации:\n\n"
        "🔹 *Прогноз цены* — я проанализирую данные и подскажу примерную стоимость выбранной модели.\n"
        "🔹 *Диапазон цен* — покажу, в каких пределах обычно варьируются цены для разных комплектаций.\n"
        "🔹 *Ликвидность и популярность* — расскажу, насколько быстро и легко продаётся эта модель на рынке.\n\n"
        "Чтобы начать, просто введите марку и модель автомобиля, например:\n"
        "`Toyota Camry`\n\n"
        "🚀 Введите название авто, и мы приступим к поиску и анализу!"
    )
    bot.reply_to(message, welcome_text, parse_mode="Markdown", reply_markup=markup)

@bot.message_handler(commands=['help'])
def send_help(message):
    welcome_text = (
        "В случае возникновения проблем, обращайтесь:\n"
        "@crdlts\n"
        "@nikilokser"
    )
    bot.reply_to(message, welcome_text, parse_mode="Markdown", reply_markup=markup)

@bot.message_handler(func=lambda msg: True)
def handle_model_input(message):
    user_input = message.text.strip().lower()
    titles = get_all_titles()
    cid = message.chat.id

    if user_input in titles:
        user_states[cid] = {"title": user_input}
        years = get_years_by_model(user_input)
        markup = InlineKeyboardMarkup()
        for y in years:
            markup.add(InlineKeyboardButton(str(y), callback_data=f"year|{y}"))
        bot.send_message(cid, "📅 Выберите год выпуска:", reply_markup=markup)
        return

    matches = difflib.get_close_matches(user_input, titles, n=1, cutoff=0.6)
    if matches:
        suggested = matches[0]
        markup = InlineKeyboardMarkup()
        markup.row(
            InlineKeyboardButton("✅ Да", callback_data=f"yes|{suggested}"),
            InlineKeyboardButton("❌ Нет", callback_data="no|")
        )
        bot.send_message(cid, f"🤔 Возможно, вы имели в виду: *{suggested.title()}*", parse_mode="Markdown", reply_markup=markup)
    else:
        bot.send_message(cid, "🚫 Модель не найдена.")

@bot.callback_query_handler(func=lambda call: True)
def handle_selection(call):
    cid = call.message.chat.id
    
    try:
        # Проверка на существование call.data
        if not call.data:
            bot.send_message(cid, "❌ Ошибка данных. Попробуйте еще раз.")
            return
        
        # Проверка на корректный формат данных для команд с разделителем
        if call.data.startswith(("yes|", "year|", "transmission|", "drive|", "color|")) and "|" not in call.data:
            bot.send_message(cid, "❌ Некорректный формат данных.")
            return

        if call.data.startswith("yes|"):
            parts = call.data.split("|", 1)  # Разделяем только на 2 части
            if len(parts) < 2:
                bot.send_message(cid, "❌ Некорректные данные.")
                return
            title = parts[1]
            user_states[cid] = {"title": title}
            years = get_years_by_model(title)
            markup = InlineKeyboardMarkup()
            for y in years:
                markup.add(InlineKeyboardButton(str(y), callback_data=f"year|{y}"))
            bot.send_message(cid, "📅 Выберите год выпуска:", reply_markup=markup)

        elif call.data.startswith("year|"):
            parts = call.data.split("|", 1)
            if len(parts) < 2:
                bot.send_message(cid, "❌ Некорректные данные.")
                return
            try:
                year = int(parts[1])
            except ValueError:
                bot.send_message(cid, "❌ Некорректный год.")
                return
            user_states[cid]["year"] = year
            trans = get_transmissions(user_states[cid]["title"], year)
            markup = InlineKeyboardMarkup()
            for t in trans:
                markup.add(InlineKeyboardButton(t, callback_data=f"transmission|{t}"))
            bot.send_message(cid, "⚙️ Выберите коробку передач:", reply_markup=markup)

        elif call.data.startswith("transmission|"):
            parts = call.data.split("|", 1)
            if len(parts) < 2:
                bot.send_message(cid, "❌ Некорректные данные.")
                return
            t = parts[1]
            user_states[cid]["transmission"] = normalize_transmission(t)
            drives = get_drives(user_states[cid]["title"], user_states[cid]["year"], t)
            markup = InlineKeyboardMarkup()
            for d in drives:
                markup.add(InlineKeyboardButton(d, callback_data=f"drive|{d}"))
            bot.send_message(cid, "🛞 Выберите тип привода:", reply_markup=markup)

        elif call.data.startswith("drive|"):
            parts = call.data.split("|", 1)
            if len(parts) < 2:
                bot.send_message(cid, "❌ Некорректные данные.")
                return
            d = parts[1]
            user_states[cid]["drive"] = d
            colors = get_colors(user_states[cid]["title"], user_states[cid]["year"], user_states[cid]["transmission"], d)
            markup = InlineKeyboardMarkup()
            for c in colors:
                markup.add(InlineKeyboardButton(c, callback_data=f"color|{c}"))
            bot.send_message(cid, "🎨 Выберите цвет:", reply_markup=markup)

        elif call.data.startswith("color|"):
            parts = call.data.split("|", 1)
            if len(parts) < 2:
                bot.send_message(cid, "❌ Некорректные данные.")
                return
            user_states[cid]["color"] = parts[1]

            state = user_states[cid]
            predicted, links = predict_price(state)

            if predicted is None:
                bot.send_message(cid, "🚫 Не удалось найти подходящие автомобили.")
            else:
                summary = (
                    f"📦 *Модель с заданными харктеристиками:*\n\n"
                    f"🚗 Модель: *{state['title'].title()}*\n"
                    f"📅 Год: *{state['year']}*\n"
                    f"⚙️ КПП: *{state['transmission']}*\n"
                    f"🛞 Привод: *{state['drive']}*\n"
                    f"🎨 Цвет: *{state['color']}*\n\n"
                )

                analysis = get_liquidity_analysis(state, predicted) # llama analysis

                msg = summary
                msg += f"📈 *Прогнозируемая цена:* `{predicted} ¥`\n\n"
                msg += f"📊 *Анализ ликвидности:*\n{analysis}\n\n"

                if links:
                    msg += "🔗 *Объявления дешевле прогноза:*\n" + "\n".join(f"{i+1}. {link}" for i, link in enumerate(links))
                else:
                    msg += "🚘 Объявлений дешевле прогнозируемой цены не найдено."

                bot.send_message(cid, msg, parse_mode="Markdown")

            user_states.pop(cid, None)

        elif call.data.startswith("no|"):
            bot.send_message(cid, "Введите корректное название автомобиля.")
            
    except Exception as e:
        print(f"❌ Ошибка в handle_selection: {e}")
        bot.send_message(cid, "❌ Произошла ошибка. Попробуйте еще раз.")
        # Очищаем состояние пользователя при ошибке
        user_states.pop(cid, None)

bot.polling(timeout=60, long_polling_timeout=10)
