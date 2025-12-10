# Telegram-бот для аналитики по видео

Тестовое задание: Telegram-бот, который по запросам на естественном русском языке считает метрики по базе статистики видео.

---

## Стек

* Python 3.12
* PostgreSQL
* aiogram 3
* psycopg2
* OpenAI API (LLM для интерпретации текстовых запросов)

---

## Структура проекта

```bash
.
├── bot.py                 # Telegram-бот: обработка сообщений и запросов к БД
├── nlp.py                 # "естественный язык → формальное описание запроса"
├── load_data.py           # загрузка JSON в PostgreSQL
├── migrations/
│   └── 001_init.sql       # схема БД (videos, video_snapshots)
├── data/
│   └── videos.json        # исходные данные (массив videos со снапшотами)
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Быстрый запуск (локально, без Docker)

### 0. Предусловия

* Установлен Python 3.12+
* Установлен PostgreSQL (доступна команда `psql`, `createdb`)
* Есть токен Telegram-бота (от @BotFather)
* Есть OpenAI API ключ

---

### 1. Клонировать репозиторий

```bash
git clone https://github.com/yourname/video_analytics_bot.git
cd video_analytics_bot
```

(подставь свой URL репозитория)

---

### 2. Создать и активировать виртуальное окружение

```bash
python -m venv venv
source venv/bin/activate    

pip install -r requirements.txt
```

---

### 3. Подготовить PostgreSQL

Создать базу данных:

```bash
createdb video_analytics
```

(если нужно, можно так: `sudo -u postgres createdb video_analytics`)

Накатить миграции (создать таблицы):

```bash
psql -d video_analytics -f migrations/001_init.sql
```

Проверить, что таблицы создались:

```bash
psql -d video_analytics -c "\dt"
```

---

### 4. Загрузить данные из JSON

Файл `data/videos.json` содержит массив объектов `videos` со вложенными `snapshots`.

Запуск скрипта загрузки:

```bash
python load_data.py
```

Проверить, что данные загрузились:

```bash
psql -d video_analytics -c "SELECT COUNT(*) FROM videos;"
psql -d video_analytics -c "SELECT COUNT(*) FROM video_snapshots;"
```

---

### 5. Настроить переменные окружения

Создайте файл `.env` в корне проекта рядом с `bot.py`, например:

```env
BOT_TOKEN=твой_telegram_bot_token
OPENAI_API_KEY=твой_openai_api_key
OPENAI_MODEL=gpt-4.1-mini

# локальный DSN для PostgreSQL
DB_DSN=dbname=video_analytics
```


### 6. Запустить бота

Из активированного venv:

```bash
python bot.py
```


