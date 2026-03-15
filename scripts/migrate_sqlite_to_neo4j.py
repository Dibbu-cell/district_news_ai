import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from config.config import SQLITE_MIGRATION_URL
from database.db import create_app_engine
from database.news_store import append_articles, ensure_data_store_ready


def migrate_sqlite_to_neo4j(chunk_size: int = 2000):
    source_engine = create_app_engine(SQLITE_MIGRATION_URL)

    ensure_data_store_ready()

    count_query = text("SELECT COUNT(*) AS count FROM news_articles")

    with source_engine.begin() as conn:
        total_rows = int(conn.execute(count_query).scalar() or 0)

    if total_rows == 0:
        print("No rows found in SQLite source.")
        return

    print(f"Migrating {total_rows} rows from SQLite to Neo4j...")

    migrated = 0

    for offset in range(0, total_rows, chunk_size):
        chunk_df = pd.read_sql(
            text(
                """
                SELECT title, content, url, source, state, state_confidence,
                       district, district_confidence, embedding, published_at
                FROM news_articles
                ORDER BY rowid
                LIMIT :limit OFFSET :offset
                """
            ),
            source_engine,
            params={"limit": chunk_size, "offset": offset},
        )

        inserted = append_articles(chunk_df)
        migrated += inserted
        print(f"Migrated {migrated}/{total_rows} rows...")

    print(f"Migration complete. Migrated {migrated} rows from SQLite to Neo4j")


if __name__ == "__main__":
    migrate_sqlite_to_neo4j()
