import threading
import subprocess
import time
import os

FLAG_FILE = "import_flag.txt"

def run_bot():
    print("🤖 Запуск Telegram-бота...")
    try:
        subprocess.run(["python3", "telegram_bot.py"])
    except Exception as e:
        print(f"❌ Ошибка при запуске бота: {e}")

def run_parser():
    print("🔄 Запуск парсера в фоне...")
    try:
        subprocess.run(["python3", "parser.py"])
    except Exception as e:
        print(f"❌ Ошибка при запуске парсера: {e}")

def run_eda(input_file):
    print(f"🧼 Запуск eda.py для {input_file}...")
    subprocess.run(["python3", "eda.py", input_file])

def run_import(input_file):
    print(f"📦 Импорт в Neo4j из {input_file}...")
    subprocess.run(["python3", "import_neo4j.py", input_file])

def remove_temp_files():
    for file in ["new_cars.csv", "clean_new_cars.csv"]:
        if os.path.exists(file):
            os.remove(file)
            print(f"🗑️ Удалён файл: {file}")

def read_flag():
    if os.path.exists(FLAG_FILE):
        with open(FLAG_FILE, "r") as f:
            val = f.read().strip()
            return val.lower() == "true"
    return False

def write_flag(value: bool):
    with open(FLAG_FILE, "w") as f:
        f.write("True" if value else "False")

# === Инициализация флага ===
if not os.path.exists(FLAG_FILE):
    write_flag(False)

# === Запуск фоновых потоков ===
bot_thread = threading.Thread(target=run_bot, daemon=True)
parser_thread = threading.Thread(target=run_parser, daemon=True)

bot_thread.start()
parser_thread.start()

print("📡 Наблюдение за new_cars.csv запущено...")

was_processed = False
first_full_import_done = read_flag()

while True:
    if os.path.exists("new_cars.csv") and not was_processed:
        print("📥 Обнаружен файл new_cars.csv. Обработка...")

        # === Одноразовый импорт всех машин из cars.csv ===
        if not first_full_import_done and os.path.exists("cars.csv"):
            print("🚀 Первый импорт из cars.csv...")
            run_eda("cars.csv")
            if os.path.exists("clean_cars.csv"):
                run_import("clean_cars.csv")
                write_flag(True)
                first_full_import_done = True
            else:
                print("⚠️ Файл clean_cars.csv не найден после eda. Пропускаем импорт.")

        # === Импорт новых машин ===
        run_eda("new_cars.csv")

        if os.path.exists("clean_new_cars.csv"):
            run_import("clean_new_cars.csv")
            remove_temp_files()
            was_processed = True
            print("✅ Импорт завершён и временные файлы удалены.")
        else:
            remove_temp_files()
            print("⚠️ Файл clean_new_cars.csv не найден после eda. Пропускаем импорт.")

    # Если new_cars.csv недавно обновился → сбрасываем флаг
    if os.path.exists("new_cars.csv"):
        file_time = os.path.getmtime("new_cars.csv")
        current_time = time.time()
        if current_time - file_time < 60:
            was_processed = False
    
    time.sleep(30)
