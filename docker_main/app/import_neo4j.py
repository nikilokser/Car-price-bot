import pandas as pd
from neo4j import GraphDatabase
import os
import sys

# Проверяем, используется ли Docker (dotenv только для локальной разработки)
if os.getenv('DOCKER_ENV') != 'true':
    from dotenv import load_dotenv
    load_dotenv()

# === Настройки подключения ===
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# === Инициализация драйвера ===
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

def import_car(tx, row):
    tx.run("""
        MERGE (c:Car {title: toLower($title), url: $url})
        MERGE (y:Year {value: $year})
        MERGE (t:Transmission {type: $transmission})
        MERGE (clr:Color {name: $color})
        MERGE (d:Drive {type: $drive})
        MERGE (e:Engine {code: $engine, volume: $volume})
        MERGE (b:BodyType {type: $body})
        MERGE (env:EnvStandard {standard: $standard})
        MERGE (f:Fuel {type: $fuel})
        MERGE (a:Auction {location: $auction})

        MERGE (m:Mileage {value: $mileage})
        MERGE (p:Power {value: $power})
        MERGE (cp:ChinaPrice {value: $china_price})
        MERGE (pr:PriceRUB {value: $price_rub})

        MERGE (c)-[:HAS_YEAR]->(y)
        MERGE (c)-[:HAS_TRANSMISSION]->(t)
        MERGE (c)-[:HAS_COLOR]->(clr)
        MERGE (c)-[:HAS_DRIVE]->(d)
        MERGE (c)-[:HAS_ENGINE]->(e)
        MERGE (c)-[:HAS_BODY]->(b)
        MERGE (c)-[:HAS_ENV_STANDARD]->(env)
        MERGE (c)-[:HAS_FUEL_TYPE]->(f)
        MERGE (c)-[:FROM_AUCTION]->(a)

        MERGE (c)-[:HAS_MILEAGE]->(m)
        MERGE (c)-[:HAS_POWER]->(p)
        MERGE (c)-[:HAS_CHINA_PRICE]->(cp)
        MERGE (c)-[:HAS_PRICE_RUB]->(pr)
    """, {
        "title": row["title"],
        "url": row["url"],
        "year": int(row["year"]),
        "transmission": row["transmission"],
        "color": row["color"],
        "drive": row["drive_type"],
        "engine": row["engine"],
        "volume": float(row["engine_volume"]),
        "body": row["body_type"],
        "standard": row["environmental_standards"],
        "fuel": row["fuel_type"],
        "auction": row["auction"],
        "mileage": int(row["mileage"]),
        "power": int(row["power"]),
        "china_price": int(row["china_price"]),
        "price_rub": int(row["price_rub"])
    })

def import_cars_from_csv(filename: str):
    print(f"📦 Импорт из файла {filename}...")
    df = pd.read_csv(filename)
    with driver.session() as session:
        for i, row in df.iterrows():
            print(f"Добавляется автомобиль {i + 1}: {row['title']}")
            session.execute_write(import_car, row)
    print(f"✅ Импорт из файла {filename} завершён!")

if __name__ == "__main__":
    # Опционально: принять флаг или имя файла из аргументов
    if len(sys.argv) > 1:
        filename = sys.argv[1]
    else:
        # Если флаг (например, true/false) не передан, по умолчанию загрузим clean_new_cars.csv
        filename = "clean_new_cars.csv"
    import_cars_from_csv(filename)
    driver.close()
