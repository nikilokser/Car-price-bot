# Car Price Bot

## Настройка

Перед запуском необходимо создать файл с переменными окружения:

```bash
cd docker_main/secrets
cp .env.example .env
```

Заполните файл `.env` следующими переменными:

```env
# Telegram Bot Token (получить у @BotFather)
BOT_TOKEN=your_telegram_bot_token_here

# Yandex Cloud API (для LLaMA анализа)
API_KEY=your_yandex_cloud_api_key
FOLDER_ID=your_yandex_cloud_folder_id

# Neo4j Database Password
NEO4J_PASSWORD=password_neo4j
```

## Запуск

```bash
cd docker_main
docker-compose up -d
```

## Остановка

```bash
docker-compose down
```

## Управление контейнерами

### Просмотр логов

```bash
# Логи всех сервисов
docker-compose logs

# Логи конкретного сервиса
docker-compose logs app
docker-compose logs neo4j
```

### Перезапуск

```bash
# Перезапуск всех сервисов
docker-compose restart

# Перезапуск конкретного сервиса
docker-compose restart app
```

### Пересборка

```bash
# Пересборка и запуск (при изменении кода)
docker-compose up -d --build
```

### Доступ к базе данных

Neo4j веб-интерфейс: [http://localhost:7474](http://localhost:7474)
