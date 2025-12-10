import json
import psycopg2
from pathlib import Path

DB_DSN = "dbname=video_analytics"
JSON_PATH = Path("data/videos.json")


def load_data():
    with JSON_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    videos = data["videos"]  

    videos_rows = []
    snapshots_rows = []

    for v in videos:
        videos_rows.append(
            (
                v["id"],
                v["creator_id"],
                v["video_created_at"],
                v["views_count"],
                v["likes_count"],
                v["comments_count"],
                v["reports_count"],
                v["created_at"],
                v["updated_at"],
            )
        )

        for s in v["snapshots"]:
            snapshots_rows.append(
                (
                    s["id"],
                    s["video_id"],  
                    s["views_count"],
                    s["likes_count"],
                    s["comments_count"],
                    s["reports_count"],
                    s["delta_views_count"],
                    s["delta_likes_count"],
                    s["delta_comments_count"],
                    s["delta_reports_count"],
                    s["created_at"],
                    s["updated_at"],
                )
            )

    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()

    cur.executemany(
        """
        INSERT INTO videos (
            id, creator_id, video_created_at,
            views_count, likes_count, comments_count, reports_count,
            created_at, updated_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (id) DO NOTHING
        """,
        videos_rows,
    )

    cur.executemany(
        """
        INSERT INTO video_snapshots (
            id, video_id,
            views_count, likes_count, comments_count, reports_count,
            delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count,
            created_at, updated_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (id) DO NOTHING
        """,
        snapshots_rows,
    )

    conn.commit()
    cur.close()
    conn.close()

    print(f"Загружено videos: {len(videos_rows)}, snapshots: {len(snapshots_rows)}")


if __name__ == "__main__":
    load_data()