import csv
import os
import re
import concurrent.futures
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime

CSV_FILE = "cars.csv"
NEW_CARS_FILE = "new_cars.csv"

def save_row(fieldnames, row, first_write=False):
    mode = "w" if first_write else "a"
    with open(CSV_FILE, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if first_write:
            writer.writeheader()
        writer.writerow(row)

def save_new_cars_to_file(fieldnames, new_cars):
    """Сохраняет новые машины в отдельный файл, перезаписывая его"""
    if not new_cars:
        # Если новых машин нет, создаем пустой файл с заголовками
        with open(NEW_CARS_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
        return
    
    with open(NEW_CARS_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in new_cars:
            writer.writerow(row)

def prepend_new_cars_to_main_csv(fieldnames, new_cars):
    """Добавляет новые машины в начало основного CSV файла"""
    if not new_cars:
        return
    
    # Читаем существующие данные
    existing_data = []
    if os.path.exists(CSV_FILE):
        try:
            with open(CSV_FILE, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                existing_data = list(reader)
        except Exception as e:
            print(f"Ошибка при чтении существующего файла: {e}")
    
    # Записываем новые данные в начало файла
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        # Сначала записываем новые машины
        for row in new_cars:
            writer.writerow(row)
        
        # Затем записываем существующие данные
        for row in existing_data:
            writer.writerow(row)

def get_existing_car_urls():
    """Получает URL первых 100 машин из CSV для проверки новых объявлений"""
    if not os.path.exists(CSV_FILE):
        return set()
    
    urls = set()
    try:
        with open(CSV_FILE, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i >= 100:  # Проверяем первые 100 записей
                    break
                if 'url' in row:
                    urls.add(row['url'])
    except Exception as e:
        print(f"Ошибка при чтении существующих URL: {e}")
    
    return urls

def check_for_new_cars():
    """Проверяет первые 3 страницы на наличие новых объявлений"""
    existing_urls = get_existing_car_urls()
    new_cars = []
    
    print(f"Проверяем новые объявления среди первых {len(existing_urls)} существующих...")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
    }
    
    for page_num in range(1, 4):  # Проверяем первые 3 страницы
        print(f"Проверяем страницу {page_num}...")
        url = f"https://mado.group/statistic-china/?PAGE={page_num}"
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            print(f"Ошибка загрузки страницы {page_num}: {e}")
            continue

        # Ищем карточки товаров
        cards = soup.select("div.statistic_items_list")
        if not cards:
            print(f"Страница {page_num}: карточки не найдены")
            continue

        car_links = []
        for card in cards:
            links = card.select("a.name")
            for link_element in links:
                href = link_element.get("href")
                title = link_element.get_text(strip=True)
                if href:
                    full_url = f"https://mado.group{href}"
                    if full_url not in existing_urls:
                        car_links.append({
                            "title": title,
                            "url": full_url,
                            "relative_url": href
                        })

        print(f"Страница {page_num}: найдено {len(car_links)} новых объявлений")

        if car_links:
            detailed_cars = get_cars_details_parallel_requests(car_links)
            print(f"Страница {page_num}: обработано {len(detailed_cars)} объявлений")
            
            for car in detailed_cars:
                parsed_info = parse_car_info(car["brief_info"], car["detailed_specs"])
                
                row = {
                    "title": clean_car_title(car["title"]),
                    "url": car["url"],
                    "price_rub": car["price_rub"],
                    "year": parsed_info["year"],
                    "mileage": parsed_info["mileage"],
                    "transmission": parsed_info["transmission"],
                    "color": parsed_info["color"],
                    "drive_type": parsed_info["drive_type"],
                    "fuel_type": parsed_info["fuel_type"],
                    "power": parsed_info["power"],
                    "auction": parsed_info["auction"],
                    "china_price": parsed_info["china_price"],
                    "engine_volume": parsed_info["engine_volume"],
                    "body_type": parsed_info["body_type"],
                    "environmental_standards": parsed_info["environmental_standards"],
                    "engine": parsed_info["engine"],
                    "gear_count": parsed_info["gear_count"]
                }
                new_cars.append(row)
    
    return new_cars

def parse_all_pages(max_pages=None, new_cars_buffer=None):
    first_write = not os.path.exists(CSV_FILE)
    seen_fields = set()
    page_num = 1
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
    }
    
    # Список для накопления новых машин перед записью
    if new_cars_buffer is None:
        new_cars_buffer = []

    while True:
        if max_pages and page_num > max_pages:
            break
        url = f"https://mado.group/statistic-china/?PAGE={page_num}"
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            print(f"Ошибка загрузки страницы {page_num}: {e}")
            break

        cards = soup.select("div.statistic_items_list")
        if not cards:
            break

        car_links = []
        for card in cards:
            links = card.select("a.name")
            for link_element in links:
                href = link_element.get("href")
                title = link_element.get_text(strip=True)
                if href:
                    full_url = f"https://mado.group{href}"
                    car_links.append({
                        "title": title,
                        "url": full_url,
                        "relative_url": href
                    })

        if not car_links:
            break

        if car_links:
            detailed_cars = get_cars_details_parallel_requests(car_links)
            
            print(f"Страница {page_num}: {len(car_links)} найдено, {len(detailed_cars)} обработано")
            
            page_cars = []
            for car in detailed_cars:
                parsed_info = parse_car_info(car["brief_info"], car["detailed_specs"])
                
                row = {
                    "title": clean_car_title(car["title"]),
                    "url": car["url"],
                    "price_rub": car["price_rub"],
                    "year": parsed_info["year"],
                    "mileage": parsed_info["mileage"],
                    "transmission": parsed_info["transmission"],
                    "color": parsed_info["color"],
                    "drive_type": parsed_info["drive_type"],
                    "fuel_type": parsed_info["fuel_type"],
                    "power": parsed_info["power"],
                    "auction": parsed_info["auction"],
                    "china_price": parsed_info["china_price"],
                    "engine_volume": parsed_info["engine_volume"],
                    "body_type": parsed_info["body_type"],
                    "environmental_standards": parsed_info["environmental_standards"],
                    "engine": parsed_info["engine"],
                    "gear_count": parsed_info["gear_count"]
                }
                page_cars.append(row)

            # Добавляем машины в начало буфера (чтобы сохранить порядок как на сайте)
            new_cars_buffer = page_cars + new_cars_buffer
            
            # Определяем все поля
            for car in page_cars:
                seen_fields.update(car.keys())

            # Записываем данные в основной файл при первом запуске
            if first_write:
                all_keys = sorted(seen_fields)
                for car in page_cars:
                    save_row(all_keys, car, first_write=first_write)
                    first_write = False
            else:
                # Для последующих страниц записываем без заголовка
                all_keys = sorted(seen_fields)
                for car in page_cars:
                    save_row(all_keys, car, first_write=False)
        
        page_num += 1
    
    return new_cars_buffer, seen_fields

def fetch_car_html(car_info):
    try:
        url = car_info["url"]
        title = car_info["title"]
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        return {
            "url": url,
            "title": title,
            "html": response.text,
            "status": "success"
        }
        
    except Exception as e:
        title = car_info.get("title", "Неизвестно")
        return {
            "url": car_info["url"],
            "title": title,
            "html": None,
            "status": "error",
            "error": str(e)
        }

def parse_car_html_from_requests(html_data_item):
    try:
        car_url = html_data_item["url"]
        car_title = html_data_item["title"]
        html_content = html_data_item["html"]
        
        if not html_content or html_data_item["status"] != "success":
            return {
                "url": car_url,
                "title": car_title,
                "price_rub": "Ошибка загрузки",
                "brief_info": "Ошибка загрузки",
                "detailed_specs": "Ошибка загрузки"
            }
        
        soup = BeautifulSoup(html_content, 'html.parser')
        details = {"url": car_url, "title": car_title}
        
        try:
            price_element = soup.select_one("div.v")
            if price_element:
                details["price_rub"] = price_element.get_text(strip=True)
            else:
                details["price_rub"] = "Не найдено"
        except:
            details["price_rub"] = "Ошибка парсинга"
        
        try:
            brief_info_element = soup.select_one("div.params_table")
            if brief_info_element:
                details["brief_info"] = brief_info_element.get_text(strip=True)
            else:
                details["brief_info"] = "Не найдено"
        except:
            details["brief_info"] = "Ошибка парсинга"
        
        try:
            detailed_specs_element = soup.select_one("div.detail_auc__table.table.table_2")
            if detailed_specs_element:
                details["detailed_specs"] = detailed_specs_element.get_text(strip=True)
            else:
                details["detailed_specs"] = "Не найдено"
        except:
            details["detailed_specs"] = "Ошибка парсинга"
        
        return details
        
    except Exception as e:
        return {
            "url": html_data_item["url"],
            "title": html_data_item["title"],
            "price_rub": "Ошибка",
            "brief_info": "Ошибка",
            "detailed_specs": "Ошибка"
        }

def get_cars_details_parallel_requests(car_links):
    max_workers_fetch = min(24, len(car_links))
    html_data = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers_fetch) as executor:
        futures = [executor.submit(fetch_car_html, car) for car in car_links]
        
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                html_data.append(result)
            except Exception as e:
                pass
    
    max_workers_parse = min(24, len(html_data))
    results = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers_parse) as executor:
        futures = [executor.submit(parse_car_html_from_requests, html_item) for html_item in html_data]
        
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                pass
    
    return results
    


def extract_field_value(text, field_name):
    if not text or text == "Не найдено" or text == "Ошибка парсинга":
        return "Не найдено"
    
    stop_fields = [
        "Год", "Пробег", "КПП", "Цвет", "Привод", "Тип топлива", "Мощность", 
        "Аукцион", "Номер лота", "Цена в Китае", "Объем", "Кузов тип", 
        "Стандарты защиты окружающей среды", "Модель двигателя", "Смещение", "Количество передач", 
        "Максимальная скорость", "Объем топливного бака", "Информация о номерном знаке"
    ]
    
    field_pattern = rf"{re.escape(field_name)}\s*:?\s*([^\n\r]+)"
    match = re.search(field_pattern, text, re.IGNORECASE | re.MULTILINE)
    
    if not match:
        return "Не найдено"
    
    found_line = match.group(1).strip()
    found_line = re.sub(r'^[::\s]+', '', found_line)
    
    for stop_field in stop_fields:
        if stop_field != field_name:
            stop_pattern = rf"({re.escape(stop_field)})"
            stop_match = re.search(stop_pattern, found_line, re.IGNORECASE)
            if stop_match:
                found_line = found_line[:stop_match.start()].strip()
                break
    
    if field_name == "Цвет":
        color_match = re.match(r"(\w+)", found_line)
        if color_match:
            return color_match.group(1)
    
    elif field_name == "Год":
        year_match = re.search(r"(\d{4})", found_line)
        if year_match:
            return year_match.group(1)
    
    elif field_name == "Пробег":
        mileage_match = re.search(r"(\d+\s*км)", found_line)
        if mileage_match:
            return mileage_match.group(1)
    
    elif field_name == "КПП":
        if "AT" in found_line.upper():
            return "AT"
        elif "MT" in found_line.upper() or "МТ" in found_line.upper():
            return "MT"
        elif "CVT" in found_line.upper():
            return "CVT"
    
    elif field_name == "Привод":
        if "Передний" in found_line or "FWD" in found_line.upper():
            return "FWD"
        elif "Задний" in found_line or "RWD" in found_line.upper():
            return "RWD"
        elif "Полный" in found_line or "4WD" in found_line.upper() or "AWD" in found_line.upper():
            return "AWD"
    
    elif field_name == "Тип топлива":
        if "Бензин" in found_line:
            return "Бензин"
        elif "Дизел" in found_line or "Дизель" in found_line:
            return "Дизель"
        elif "Электр" in found_line:
            return "Электричество"
        elif "Гибрид" in found_line:
            return "Гибрид"
    
    elif field_name == "Мощность":
        power_match = re.search(r"(\d+\s*л\.с\.)", found_line)
        if power_match:
            return power_match.group(1)
    
    elif field_name == "Объем":
        volume_match = re.search(r"(\d+\s*см3|\d+[\.,]\d*\s*L)", found_line)
        if volume_match:
            return volume_match.group(1)
    
    elif field_name == "Цена в Китае":
        price_match = re.search(r"(\d+[\s\d,]*\s*¥)", found_line)
        if price_match:
            return price_match.group(1)
    
    elif field_name == "Аукцион":
        auction_parts = found_line.split("Номер лота")
        if auction_parts:
            result = auction_parts[0].strip()
            result = re.sub(r'^[::\s]+|[::\s]+$', '', result)
            return result if result else "Не найдено"
    
    elif field_name == "Стандарты защиты окружающей среды":
        euro_match = re.search(r"(Euro\s*[IVX\d\s]+)", found_line, re.IGNORECASE)
        if euro_match:
            euro_standard = euro_match.group(1).strip()
            euro_standard = re.sub(r'\s+', ' ', euro_standard)
            return euro_standard
        return "Не найдено"
    
    elif field_name == "Количество передач":
        gear_match = re.search(r"(\d+)", found_line)
        if gear_match:
            return gear_match.group(1)
        return "Не найдено"
    
    elif field_name == "Кузов тип":
        body_patterns = [
            r"седан", r"хэтчбек", r"универсал", r"внедорожник", r"кроссовер", 
            r"купе", r"кабриолет", r"минивэн", r"пикап", r"лифтбек", r"mpv"
        ]
        for pattern in body_patterns:
            body_match = re.search(pattern, found_line, re.IGNORECASE)
            if body_match:
                return body_match.group(0).lower()
        
        if "автомобиль" in found_line.lower():
            return "седан"
        
        if "информация о номерном знаке" in found_line.lower():
            return "Не найдено"
    
    elif field_name == "Модель двигателя":
        displacement_match = re.search(r"(.+?)(?:Смещение|$)", found_line, re.IGNORECASE)
        if displacement_match:
            result = displacement_match.group(1).strip()
            result = re.sub(r'^[::\s]+|[::\s]+$', '', result)
            result = result.replace(',', ' ')
            return result if result and result != '-' else "Не найдено"
        else:
            result = re.sub(r'^[::\s]+|[::\s]+$', '', found_line)
            result = result.replace(',', ' ')
            return result if result and result != '-' else "Не найдено"

    result = found_line[:50] if len(found_line) > 50 else found_line
    result = re.sub(r'^[::\s]+|[::\s]+$', '', result)
    return result if result else "Не найдено"

def parse_car_info(brief_info, detailed_specs):
    info = {}
    
    info["year"] = extract_field_value(brief_info, "Год")
    info["mileage"] = extract_field_value(brief_info, "Пробег")
    info["transmission"] = extract_field_value(brief_info, "КПП")
    info["color"] = extract_field_value(brief_info, "Цвет")
    info["drive_type"] = extract_field_value(brief_info, "Привод")
    info["fuel_type"] = extract_field_value(brief_info, "Тип топлива")
    info["power"] = extract_field_value(brief_info, "Мощность")
    info["auction"] = extract_field_value(brief_info, "Аукцион")
    info["china_price"] = extract_field_value(brief_info, "Цена в Китае")
    info["engine_volume"] = extract_field_value(brief_info, "Объем")
    
    info["body_type"] = extract_field_value(detailed_specs, "Кузов тип")
    info["environmental_standards"] = extract_field_value(detailed_specs, "Стандарты защиты окружающей среды")
    info["engine"] = extract_field_value(detailed_specs, "Модель двигателя")
    info["gear_count"] = extract_field_value(detailed_specs, "Количество передач")
    
    return info

def clean_car_title(title):
    if not title:
        return title
    
    year_patterns = [
        r'\b\d{4}\b',
        r'\(\d{4}\)',
        r'\[\d{4}\]',
        r'\d{4}г\.?',
        r'\d{4}\s*г\.?',
    ]
    
    cleaned_title = title
    for pattern in year_patterns:
        cleaned_title = re.sub(pattern, '', cleaned_title, flags=re.IGNORECASE)
    
    cleaned_title = re.sub(r'\s+', ' ', cleaned_title)
    cleaned_title = re.sub(r'[,\-]+\s*$', '', cleaned_title)
    cleaned_title = re.sub(r'^\s*[,\-]+\s*', '', cleaned_title)
    cleaned_title = cleaned_title.strip()
    
    return cleaned_title if cleaned_title else title

def main():
    cycle_count = 0
    
    while True:
        cycle_count += 1
        print(f"\n{'='*60}")
        print(f"ЦИКЛ ПАРСИНГА #{cycle_count}")
        print(f"Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
        # Если файл уже существует, проверяем новые объявления
        if os.path.exists(CSV_FILE):
            print("Проверяем новые объявления...")
            new_cars = check_for_new_cars()
            
            # Определяем все поля
            all_fields = set()
            if new_cars:
                for car in new_cars:
                    all_fields.update(car.keys())
            
            # Читаем существующие поля из основного CSV
            try:
                with open(CSV_FILE, "r", newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    if reader.fieldnames:
                        all_fields.update(reader.fieldnames)
            except:
                pass
            
            fieldnames = sorted(all_fields) if all_fields else []
            
            if new_cars:
                print(f"Найдено {len(new_cars)} новых объявлений!")
                # Сохраняем новые машины в отдельный файл
                save_new_cars_to_file(fieldnames, new_cars)
                # Добавляем новые машины в начало основного CSV файла
                prepend_new_cars_to_main_csv(fieldnames, new_cars)
                print(f"Новые объявления сохранены в файл {NEW_CARS_FILE} и добавлены в начало {CSV_FILE}!")
            else:
                print("Новых объявлений не найдено.")
                # Создаем пустой файл с заголовками
                save_new_cars_to_file(fieldnames, [])
                print(f"Файл {NEW_CARS_FILE} обновлен (пустой).")
        else:
            print("Первый запуск - начинаем полный парсинг...")
            
            new_cars_buffer, seen_fields = parse_all_pages()
            print(f"Первый парсинг завершён.")
            
            # Создаем пустой файл для новых машин
            fieldnames = sorted(seen_fields) if seen_fields else []
            save_new_cars_to_file(fieldnames, [])

        print(f"\nЦикл #{cycle_count} завершён.")
        
        # Показываем статистику файлов
        if os.path.exists(CSV_FILE):
            with open(CSV_FILE, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                total_cars = sum(1 for row in reader)
            print(f"Основной файл {CSV_FILE}: {total_cars} записей")
        
        if os.path.exists(NEW_CARS_FILE):
            with open(NEW_CARS_FILE, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                new_cars_count = sum(1 for row in reader)
            print(f"Файл новых машин {NEW_CARS_FILE}: {new_cars_count} записей")
        
        # Ждём 30 минут перед следующей проверкой
        print("Ожидание 30 минут до следующей проверки...")
        time.sleep(1800)  # 30 минут


if __name__ == "__main__":
    main()
