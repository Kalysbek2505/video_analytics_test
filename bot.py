import os
import asyncio

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message

import psycopg2
from dotenv import load_dotenv

from nlp import parse_user_query

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_DSN = os.getenv("DB_DSN", "dbname=video_analytics")

if not BOT_TOKEN:
    raise RuntimeError("Не задан BOT_TOKEN в .env")


def get_conn():
    return psycopg2.connect(DB_DSN)


bot = Bot(BOT_TOKEN)
dp = Dispatcher()


@dp.message(CommandStart())
async def cmd_start(message: Message):
    await message.answer(
        "Привет! Я бот для аналитики по видео.\n"
        "Задавай вопросы вроде:\n"
        "• Сколько всего видео есть в системе?\n"
        "• Сколько видео у креатора с id ... вышло с 1 ноября 2025 по 5 ноября 2025 включительно?\n"
        "• На сколько просмотров в сумме выросли все видео 28 ноября 2025?\n"
    )


def execute_query(query_desc: dict) -> int:
    qt = query_desc.get("query_type")

    with get_conn() as conn:
        with conn.cursor() as cur:
            if qt == "total_videos":
                cur.execute("SELECT COUNT(*) FROM videos;")
                count = cur.fetchone()[0]
                print("[DEBUG total_videos] count =", count)
                return count


            elif qt == "total_videos":
                cur.execute("SELECT COUNT(*) FROM videos;")
                return cur.fetchone()[0]

            elif qt == "creator_videos_in_date_range":
                creator_id = query_desc["creator_id"]
                date_from = query_desc["date_from"]
                date_to = query_desc["date_to"]

                print(f"[DEBUG] creator_videos_in_date_range creator_id={creator_id}, "
                      f"date_from={date_from}, date_to={date_to}")

                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM videos
                    WHERE creator_id = %s
                      AND video_created_at::date BETWEEN %s AND %s;
                    """,
                    (creator_id, date_from, date_to),
                )
                count = cur.fetchone()[0]
                print("[DEBUG] count =", count)
                return count

            elif qt == "videos_with_min_views":
                threshold = query_desc["views_threshold"]
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM videos
                    WHERE views_count > %s;
                    """,
                    (threshold,),
                )
                return cur.fetchone()[0]

            elif qt == "total_views_delta_on_date":
                date = query_desc["date"]
                cur.execute(
                    """
                    SELECT COALESCE(SUM(delta_views_count), 0)
                    FROM video_snapshots
                    WHERE created_at::date = %s;
                    """,
                    (date,),
                )
                return cur.fetchone()[0]

            elif qt == "videos_with_new_views_on_date":
                date = query_desc["date"]
                cur.execute(
                    """
                    SELECT COUNT(DISTINCT video_id)
                    FROM video_snapshots
                    WHERE created_at::date = %s
                      AND delta_views_count > 0;
                    """,
                    (date,),
                )
                return cur.fetchone()[0]



@dp.message(F.text)
async def handle_any_text(message: Message):
    user_text = message.text.strip()
    print(f"[USER] {user_text}")

    try:
        query_desc = parse_user_query(user_text)
    except Exception as e:
        print("[ERROR parse_user_query]", repr(e))
        await message.answer("Не смог разобрать запрос, попробуй переформулировать.")
        return

    print("[QUERY_DESC]", query_desc)

    qt = query_desc.get("query_type")
    if not qt or qt == "unknown":
        print("[INFO] unknown query_type")
        await message.answer("Я не понял запрос, попробуй сформулировать иначе.")
        return

    try:
        result = execute_query(query_desc)
    except Exception as e:
        print("[DB ERROR]", repr(e))
        await message.answer("Ошибка при выполнении запроса к базе.")
        return

    print(f"[RESULT] {result}")
    await message.answer(str(result))

async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
