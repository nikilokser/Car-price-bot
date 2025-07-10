FROM python:3.12-slim

# Установка системных зависимостей
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Создание рабочего пользователя
RUN groupadd -g 1001 appgroup && \
    useradd -m -u 1001 -g appgroup appuser

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

COPY app/ .

RUN chown -R appuser:appgroup /app

USER appuser

ENV PYTHONUNBUFFERED=1
ENV DOCKER_ENV=true

CMD ["python", "main.py"]