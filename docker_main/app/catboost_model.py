import pandas as pd
from catboost import CatBoostRegressor

model = CatBoostRegressor()
model.load_model("catboost_model.cbm")

feature_order = [
    "auction", "body_type", "color", "drive_type", "engine", "engine_volume",
    "environmental_standards", "fuel_type", "mileage", "power",
    "title", "transmission", "year"
]

def predict_car_price(df: pd.DataFrame) -> int:
    df_model = df[feature_order].copy()
    predicted_price = int(model.predict(df_model).mean())
    return predicted_price
