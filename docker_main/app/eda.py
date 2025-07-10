import pandas as pd
import numpy as np
import sys
import os

# === Получение имени входного файла ===
input_file = sys.argv[1] if len(sys.argv) > 1 else "new_cars.csv"

# === Загрузка данных ===
df = pd.read_csv(input_file)

# === Очистка от некорректных строк ===
df.drop(df[df['price_rub'] == 'Цену уточняйте'].index, inplace=True)
df.drop(df[(df['power'] == 'Не найдено') & (df['engine'] == 'Не найдено')].index, inplace=True)
df.drop(df[(df['fuel_type'] == 'Не найдено') & (df['engine'] == 'Не найдено')].index, inplace=True)

if 'gear_count' in df.columns:
    df.drop(columns=['gear_count'], inplace=True)

# === Функции очистки ===
def clean_title_simple(title):
    title = title.lower()
    for word in ['import', 'other']:
        title = title.replace(word, '')
    return ' '.join(title.split())

def price_to_int(price_str):
    price_str = str(price_str).replace(' ', '').replace('₽', '')
    try:
        return int(price_str)
    except ValueError:
        return None

def extract_power(value):
    if isinstance(value, str) and value != 'Не найдено':
        return int(''.join(filter(str.isdigit, value)))
    return np.nan

# === Применение очистки ===
df['title'] = df['title'].apply(clean_title_simple)
df['environmental_standards'] = df['environmental_standards'].apply(lambda x: x.replace('Не найдено', 'Euro v I').lower())
df['transmission'] = df['transmission'].apply(lambda x: x.replace('МТ', 'MT').replace('АТ', 'AT'))
df['price_rub'] = df['price_rub'].apply(price_to_int)

df['fuel_type'] = df['fuel_type'].replace('Не найдено', 'Бензин')

df['power'] = df['power'].apply(extract_power)
# Вычисляем среднее значение мощности, игнорируя NaN
mean_power = df['power'].mean()
if pd.isna(mean_power):
    mean_power = 150  # Значение по умолчанию, если все значения NaN
else:
    mean_power = int(mean_power)
df['power'] = df['power'].fillna(mean_power).astype(int)

df['mileage'] = df['mileage'].str.replace('км', '', regex=False).str.replace(' ', '', regex=False).astype(int)

df['engine_volume'] = df['engine_volume'] \
    .str.replace('см3', '', regex=False) \
    .str.replace('cм3', '', regex=False) \
    .str.replace(' ', '', regex=False) \
    .astype(int)

df['china_price'] = df['china_price'].str.replace('¥', '', regex=False).str.replace(' ', '', regex=False).astype(int)

# === Заполнение пропущенных значений наиболее частыми ===
# Для engine
engine_mode = df.loc[df['engine'] != 'Не найдено', 'engine'].mode()
most_common_engine = engine_mode.iloc[0] if len(engine_mode) > 0 else 'Unknown'
df['engine'] = df['engine'].replace('Не найдено', most_common_engine)

# Для drive_type
drive_mode = df.loc[df['drive_type'] != 'Не найдено', 'drive_type'].mode()
most_common_drive = drive_mode.iloc[0] if len(drive_mode) > 0 else 'Unknown'
df['drive_type'] = df['drive_type'].replace('Не найдено', most_common_drive)

# Для body_type
body_mode = df.loc[df['body_type'] != 'Не найдено', 'body_type'].mode()
most_common_body = body_mode.iloc[0] if len(body_mode) > 0 else 'Unknown'
df['body_type'] = df['body_type'].replace('Не найдено', most_common_body)

# === Сохранение очищенного файла ===
output_file = f"clean_{os.path.basename(input_file)}"
df.to_csv(output_file, index=False, encoding='utf-8')
print(f"✅ Очищенные данные сохранены в {output_file}")
