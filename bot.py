import os
import asyncio

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message

import psycopg2
from dotenv import load_dotenv

load_dotenv()


from nlp import parse_user_query, nl_to_sql 

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

            elif qt == "creator_videos_with_min_views":
                creator_id = query_desc["creator_id"]
                threshold = query_desc["views_threshold"]
                print(f"[DEBUG creator_videos_with_min_views] creator_id={creator_id}, threshold={threshold}")

                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM videos
                    WHERE creator_id = %s
                      AND views_count > %s;
                    """,
                    (creator_id, threshold),
                )
                count = cur.fetchone()[0]
                print("[DEBUG creator_videos_with_min_views] count =", count)
                return count

            elif qt == "creator_videos_in_date_range":
                creator_id = query_desc["creator_id"]
                date_from = query_desc["date_from"]
                date_to = query_desc["date_to"]

                print(
                    f"[DEBUG] creator_videos_in_date_range creator_id={creator_id}, "
                    f"date_from={date_from}, date_to={date_to}"
                )

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

            elif qt == "sum_views_for_videos_in_date_range":
                date_from = query_desc["date_from"]
                date_to = query_desc["date_to"]
                print(f"[DEBUG sum_views_for_videos_in_date_range] {date_from}..{date_to}")

                cur.execute(
                    """
                    SELECT COALESCE(SUM(views_count), 0)
                    FROM videos
                    WHERE video_created_at::date BETWEEN %s AND %s;
                    """,
                    (date_from, date_to),
                )
                total = cur.fetchone()[0]
                print("[DEBUG sum_views_for_videos_in_date_range] total =", total)
                return total

            elif qt == "snapshots_with_negative_delta":
                metric = query_desc["metric"]
                date = query_desc.get("date")

                column_map = {
                    "views": "delta_views_count",
                    "likes": "delta_likes_count",
                    "comments": "delta_comments_count",
                    "reports": "delta_reports_count",
                }
                col = column_map.get(metric)
                if not col:
                    return 0

                if date:
                    query = f"""
                        SELECT COUNT(*)
                        FROM video_snapshots
                        WHERE {col} < 0
                          AND created_at::date = %s;
                    """
                    params = (date,)
                else:
                    query = f"""
                        SELECT COUNT(*)
                        FROM video_snapshots
                        WHERE {col} < 0;
                    """
                    params = ()

                cur.execute(query, params)
                count = cur.fetchone()[0]
                print("[DEBUG snapshots_with_negative_delta] count =", count)
                return count

            elif qt == "creator_views_delta_in_time_range":
                creator_id = query_desc["creator_id"]
                date = query_desc["date"]
                time_from = query_desc["time_from"]
                time_to = query_desc["time_to"]

                dt_from = f"{date} {time_from}:00"
                dt_to = f"{date} {time_to}:00"

                print(
                    f"[DEBUG creator_views_delta_in_time_range] creator_id={creator_id}, "
                    f"dt_from={dt_from}, dt_to={dt_to}"
                )

                cur.execute(
                    """
                    SELECT COALESCE(SUM(s.delta_views_count), 0)
                    FROM video_snapshots AS s
                    JOIN videos AS v ON v.id = s.video_id
                    WHERE v.creator_id = %s
                      AND s.created_at >= %s
                      AND s.created_at <= %s;
                    """,
                    (creator_id, dt_from, dt_to),
                )
                total = cur.fetchone()[0]
                print("[DEBUG creator_views_delta_in_time_range] total =", total)
                return total

            else:
                print("[WARN] unknown query_type:", qt)
                return 0


@dp.message(F.text)
async def handle_any_text(message: Message):
    user_text = message.text.strip()
    print("[USER]", user_text)

    # 1. Пытаемся через query_type
    try:
        query_desc = parse_user_query(user_text)
        print("[QUERY_DESC]", query_desc)
    except Exception as e:
        print("ERROR parse_user_query:", repr(e))
        query_desc = None

    if query_desc and query_desc.get("query_type") not in (None, "unknown"):
        try:
            result = execute_query(query_desc)
            print("[RESULT]", result)
            await message.answer(str(result))
            return
        except Exception as e:
            print("ERROR execute_query:", repr(e))
            await message.answer("Ошибка при выполнении запроса к базе.")
            return

    try:
        sql = nl_to_sql(user_text, DB_DSN)
        print("[FALLBACK SQL]", sql)
    except Exception as e:
        print("ERROR nl_to_sql:", repr(e))
        await message.answer("Не смог разобрать запрос, попробуй переформулировать.")
        return

    if not sql.lower().lstrip().startswith("select"):
        await message.answer("Не смог составить безопасный SQL-запрос для этого вопроса.")
        return

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                value = row[0] if row and row[0] is not None else 0
    except Exception as e:
        print("ERROR executing fallback SQL:", repr(e))
        await message.answer("Ошибка при выполнении запроса к базе.")
        return

    print("[RESULT/FALLBACK]", value)
    await message.answer(str(value))


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
