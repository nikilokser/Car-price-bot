import os
import requests

# Проверяем, используется ли Docker (dotenv только для локальной разработки)
if os.getenv('DOCKER_ENV') != 'true':
    from dotenv import load_dotenv
    load_dotenv()

API_KEY = os.getenv("API_KEY")
FOLDER_ID = os.getenv("FOLDER_ID")
URL = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"

def get_liquidity_analysis(state, predicted_price):
    characteristics = "\n".join(f"{k}: {v}" for k, v in state.items())
    prompt = (
        f"На основе следующих характеристик автомобиля:\n"
        f"{characteristics}\n"
        f"и прогнозируемой цены: {predicted_price} ¥,\n"
        f"оцени ликвидность этого автомобиля на российском рынке в 2025/2026 годах.\n"
        f"Насколько это выгодная покупка по текущей цене?\n"
        f"Ответ представь в виде краткого, но аналитического заключения."
    )

    payload = {
        "modelUri": f"gpt://{FOLDER_ID}/llama",
        "completionOptions": {
            "temperature": 0.4,
            "maxTokens": 800
        },
        "messages": [
            {"role": "system", "text": (
                "Ты — автоаналитик. Сформулируй краткую аналитическую справку в 5 предложениях. Оцени, насколько целесообразна покупка автомобиля при указанной цене. Укажи ликвидность модели на российском рынке. Дай сдержанную, но объективную рекомендацию. Не добавляй вводных слов вроде: пользователь. Стиль — деловой, без лишней воды. Предложения небольшие по обьему. Информацию давай тезисно, без лишней информации. Можешь использователь статистические характеристики для описания. Не дублируй характеристики модели в рекомендации."
                "Дополнительно переведи цену в рубли. Укажи средную цену на автомобиль на российском рынке. Посчитай насколько актуально приобритать этот автомобиль на китайской аукционе, учитывая что его нужно будет привезти в россию (учитывай размер утилизационного сбора и пошлины). В самом выводе не используй подобные выражения."
                )
            },
            {"role": "user", "text": prompt}
        ]
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Api-Key {API_KEY}"
    }

    response = requests.post(URL, headers=headers, json=payload)

    if response.ok:
        return response.json()['result']['alternatives'][0]['message']['text']
    else:
        return f"❌ Ошибка LLaMA: {response.status_code}\n{response.text}"
