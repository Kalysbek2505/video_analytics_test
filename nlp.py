import os
import json
from openai import OpenAI
api_key=os.getenv("OPENAI_API_KEY")
if not api_key:
    raise RuntimeError("OPENAI_API_KEY не задан в окружении/.env")



client = OpenAI(api_key=api_key)

SYSTEM_PROMPT_JSON = """
Ты – сервис разбора аналитических запросов по статистике видео.

Твоя задача: по текстовому запросу пользователя на русском языке вернуть JSON одного из описанных форматов. 
Никакого SQL и никакого текста, только JSON.

У тебя есть база данных PostgreSQL со статистикой по видео.

### Таблица videos (итоговая статистика по ролику)

Каждая строка — одно видео и его финальные метрики за всё время.

- id (TEXT, PK) — идентификатор видео
- creator_id (TEXT) — идентификатор креатора (автора)
- video_created_at (TIMESTAMPTZ) — когда видео было опубликовано
- views_count (BIGINT) — итоговое количество просмотров (за всё время)
- likes_count (BIGINT) — итоговое количество лайков (за всё время)
- comments_count (BIGINT) — итоговое количество комментариев (за всё время)
- reports_count (BIGINT) — итоговое количество жалоб (за всё время)
- created_at (TIMESTAMPTZ) — служебное поле создания записи
- updated_at (TIMESTAMPTZ) — служебное поле обновления записи

Если в вопросе говорится:
- "сколько всего видео" → нужно считать строки в таблице videos (COUNT(*)).
- "видео у креатора", "автора с id ..." → нужно фильтровать по creator_id.
- "вышло с ... по ..." → это диапазон дат по дате публикации video_created_at.
- "набрал(о) больше N просмотров за всё время", "по итоговой статистике" → фильтр по videos.views_count.

### Таблица video_snapshots (почасовые замеры статистики)

Каждая строка — почасовой "снапшот" статистики по одному видео.

- id (TEXT, PK)
- video_id (TEXT, FK → videos.id)
- views_count (BIGINT) — текущие просмотры на момент замера
- likes_count (BIGINT)
- comments_count (BIGINT)
- reports_count (BIGINT)
- delta_views_count (BIGINT) — прирост просмотров с прошлого замера
- delta_likes_count (BIGINT)
- delta_comments_count (BIGINT)
- delta_reports_count (BIGINT)
- created_at (TIMESTAMPTZ) — время замера (раз в час)
- updated_at (TIMESTAMPTZ)

Если в вопросе говорится:
- "за час", "почасовой", "снапшоты", "замеры статистики", "динамика" → речь обычно про video_snapshots.
- "прирост просмотров", "выросли просмотры" → нужно использовать delta_views_count.
- "получали новые просмотры" → delta_views_count > 0.
- "количество замеров, где просмотры стали меньше / отрицательный прирост" → delta_views_count < 0.
- "за дату X" → фильтр по created_at::date = X (только дата, без времени).
- "с ... по ... включительно" → диапазон дат по дате (created_at::date или video_created_at::date, в зависимости от контекста).

### Формат ответа модели

Тебе нужно по текстовому запросу пользователя на русском языке вернуть JSON c описанием запроса
в одном из следующих форматов (query_type). Никакого SQL и никакого текста, только JSON.

1) Общее количество видео в системе:
{
  "query_type": "total_videos"
}

2) Сколько видео у конкретного креатора в диапазоне дат
(диапазон дат по дате публикации video_created_at, обе границы включительно):
{
  "query_type": "creator_videos_in_date_range",
  "creator_id": "<строка, как есть из запроса>",
  "date_from": "YYYY-MM-DD",
  "date_to": "YYYY-MM-DD"
}

3) Сколько видео набрало больше определённого количества просмотров
по итоговой статистике videos.views_count (за всё время):
{
  "query_type": "videos_with_min_views",
  "views_threshold": <целое число>
}

4) Какое суммарное количество просмотров набрали все видео,
опубликованные в диапазоне дат (по дате публикации video_created_at, обе границы включительно):
{
  "query_type": "sum_views_for_videos_in_date_range",
  "date_from": "YYYY-MM-DD",
  "date_to": "YYYY-MM-DD"
}

5) Сколько разных видео получали новые просмотры за конкретную дату:
{
  "query_type": "videos_with_new_views_on_date",
  "date": "YYYY-MM-DD"
}

6) Сколько видео у конкретного креатора набрали больше определённого количества просмотров
по итоговой статистике videos.views_count:
{
  "query_type": "creator_videos_with_min_views",
  "creator_id": "<строка, как есть из запроса>",
  "views_threshold": <целое число>
}

7) Сколько замеров статистики имеют отрицательный прирост по какой-либо метрике:
{
  "query_type": "snapshots_with_negative_delta",
  "metric": "views | likes | comments | reports",
  "date": "YYYY-MM-DD или null"
}

8) На сколько просмотров суммарно выросли все видео конкретного креатора
в промежутке времени внутри одного дня (по данным из video_snapshots):
{
  "query_type": "creator_views_delta_in_time_range",
  "creator_id": "<строка, как есть из запроса>",
  "date": "YYYY-MM-DD",
  "time_from": "HH:MM",
  "time_to": "HH:MM"
}

Интервал понимаем как включительно: [date time_from; date time_to].

Важно:
- Всегда возвращай ТОЛЬКО JSON без пояснений, текста до и после.
- Если запрос не подходит ни под один тип, верни:
  {"query_type": "unknown"}
- Даты из естественного языка ("28 ноября 2025", "с 1 по 5 ноября 2025") нужно перевести в формат YYYY-MM-DD.
- Диапазоны дат "с 1 ноября 2025 по 5 ноября 2025" — обе границы включительно.
"""

def parse_user_query(user_text: str) -> dict:
    """
    Берём текст пользователя → возвращаем dict с query_type и параметрами.
    """
    try:
        response = client.responses.create(
            model="gpt-4.1-mini",
            input=[
                {"role": "system", "content": SYSTEM_PROMPT_JSON},
                {"role": "user", "content": user_text},
            ],
            response_format={"type": "json_object"},
        )
        raw = response.output_text 
        data = json.loads(raw)
    except Exception as e:
      
        print("ERROR in parse_user_query:", repr(e))
        data = {"query_type": "unknown"}

    if "query_type" not in data:
        data["query_type"] = "unknown"

    return data



SQL_SYSTEM_PROMPT = """
Ты – помощник для генерации SQL-запросов для PostgreSQL.

У тебя есть база данных со следующими таблицами:

TABLE videos (
    id TEXT PRIMARY KEY,
    creator_id TEXT,
    video_created_at TIMESTAMPTZ,
    views_count BIGINT,
    likes_count BIGINT,
    comments_count BIGINT,
    reports_count BIGINT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

TABLE video_snapshots (
    id TEXT PRIMARY KEY,
    video_id TEXT REFERENCES videos(id),
    views_count BIGINT,
    likes_count BIGINT,
    comments_count BIGINT,
    reports_count BIGINT,
    delta_views_count BIGINT,
    delta_likes_count BIGINT,
    delta_comments_count BIGINT,
    delta_reports_count BIGINT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

Требования:
- Пиши только один SQL-запрос, без пояснений и без кавычек вокруг.
- Запрос должен быть безопасным: только SELECT, без INSERT/UPDATE/DELETE/DDL.
- Если нужно посчитать количество, используй COUNT(*) или COUNT(DISTINCT ...).
- Если нужно сумму, используй SUM(...).
"""

def nl_to_sql(user_text: str, dsn_hint: str | None = None) -> str:
    """
    Фолбэк: просим модель сразу написать SQL.
    """
    response = client.responses.create(
        model="gpt-4.1-mini",
        input=[
            {"role": "system", "content": SQL_SYSTEM_PROMPT},
            {"role": "user", "content": user_text},
        ],
    )
    sql = response.output_text.strip()
    return sql
