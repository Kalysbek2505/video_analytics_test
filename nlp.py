import os
import json
from typing import Any, Dict

from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

if not OPENAI_API_KEY:
    raise RuntimeError("Не задан OPENAI_API_KEY в .env")

client = OpenAI(api_key=OPENAI_API_KEY)


SCHEMA_DESCRIPTION = """
У тебя есть база данных PostgreSQL со следующими таблицами:

Таблица videos (итоговая статистика по ролику):
- id (TEXT, первичный ключ)
- creator_id (TEXT) — идентификатор креатора
- video_created_at (TIMESTAMPTZ) — когда видео было опубликовано
- views_count (BIGINT) — итоговое количество просмотров (за всё время)
- likes_count (BIGINT) — итоговое количество лайков
- comments_count (BIGINT) — итоговое количество комментариев
- reports_count (BIGINT) — итоговое количество жалоб
- created_at (TIMESTAMPTZ)
- updated_at (TIMESTAMPTZ)

Таблица video_snapshots (почасовые замеры):
- id (TEXT, первичный ключ)
- video_id (TEXT, внешний ключ на videos.id)
- views_count (BIGINT) — текущее количество просмотров на момент замера
- likes_count (BIGINT)
- comments_count (BIGINT)
- reports_count (BIGINT)
- delta_views_count (BIGINT) — прирост просмотров с прошлого замера
- delta_likes_count (BIGINT)
- delta_comments_count (BIGINT)
- delta_reports_count (BIGINT)
- created_at (TIMESTAMPTZ) — время замера (раз в час)
- updated_at (TIMESTAMPTZ)

Нужно по текстовому запросу пользователя на русском языке вернуть JSON c описанием запроса
в одном из следующих форматов (query_type):

1) Общее количество видео в системе:
{
  "query_type": "total_videos"
}

2) Сколько видео у конкретного креатора в диапазоне дат (даты по публикации видео video_created_at, включительно):
{
  "query_type": "creator_videos_in_date_range",
  "creator_id": "<строка, как есть из запроса>",
  "date_from": "YYYY-MM-DD",
  "date_to": "YYYY-MM-DD"
}

3) Сколько видео набрало больше определённого количества просмотров (по полю videos.views_count):
{
  "query_type": "videos_with_min_views",
  "views_threshold": <целое число>
}

4) На сколько просмотров в сумме выросли все видео за конкретную дату:
нужно использовать video_snapshots и суммировать delta_views_count за указанную дату
(по полю created_at по дате, независимо от времени):
{
  "query_type": "total_views_delta_on_date",
  "date": "YYYY-MM-DD"
}

5) Сколько разных видео получали новые просмотры за конкретную дату:
нужно посчитать количество уникальных video_id в video_snapshots за эту дату, где delta_views_count > 0:
{
  "query_type": "videos_with_new_views_on_date",
  "date": "YYYY-MM-DD"
}

Важно:
- Всегда возвращай ТОЛЬКО JSON без пояснений, текста до и после.
- Если запрос не подходит ни под один тип, верни:
  {"query_type": "unknown"}
- Даты из естественного языка ("28 ноября 2025", "с 1 по 5 ноября 2025") нужно перевести в формат YYYY-MM-DD.
- Диапазоны дат в стиле "с 1 ноября 2025 по 5 ноября 2025" — обе границы включительные.
"""


def parse_user_query(text: str) -> Dict[str, Any]:
    """
    Отправляет текст пользователя в LLM и возвращает dict с описанием запроса.
    """
    prompt = (
        SCHEMA_DESCRIPTION
        + "\n\nПримеры:\n\n"
        + 'Пользователь: "Сколько всего видео есть в системе?"\n'
          'Ответ:\n'
          '{"query_type": "total_videos"}\n\n'
        + 'Пользователь: "Сколько видео у креатора с id aca1061a9d324ecf8c3fa2bb32d7be63 '
          'вышло с 1 ноября 2025 по 5 ноября 2025 включительно?"\n'
          'Ответ:\n'
          '{"query_type": "creator_videos_in_date_range", "creator_id": "aca1061a9d324ecf8c3fa2bb32d7be63", '
          '"date_from": "2025-11-01", "date_to": "2025-11-05"}\n\n'
        + 'Пользователь: "Сколько видео набрало больше 100 000 просмотров за всё время?"\n'
          'Ответ:\n'
          '{"query_type": "videos_with_min_views", "views_threshold": 100000}\n\n'
        + 'Пользователь: "На сколько просмотров в сумме выросли все видео 28 ноября 2025?"\n'
          'Ответ:\n'
          '{"query_type": "total_views_delta_on_date", "date": "2025-11-28"}\n\n'
        + 'Пользователь: "Сколько разных видео получали новые просмотры 27 ноября 2025?"\n'
          'Ответ:\n'
          '{"query_type": "videos_with_new_views_on_date", "date": "2025-11-27"}\n\n'
        + "Теперь обработай следующий запрос пользователя и верни только JSON:\n"
        + f"Пользователь: \"{text}\""
    )

    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {
                "role": "system",
                "content": "Ты помощник, который переводит запросы на естественном русском языке "
                           "в строгие JSON-описания запросов к базе данных.",
            },
            {"role": "user", "content": prompt},
        ],
        temperature=0.0,
    )

    raw = resp.choices[0].message.content.strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        raw = raw.strip().strip("`")
        raw = raw.replace("json", "").strip()
        data = json.loads(raw)

    return data
