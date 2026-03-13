import os
import sys
from pathlib import Path

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from config.config import DATABASE_URL, SQLITE_MIGRATION_URL
from database.db import create_app_engine
from database.schema import ensure_schema


def migrate_sqlite_to_postgres():

    source_engine = create_app_engine(SQLITE_MIGRATION_URL)
    target_engine = create_app_engine(DATABASE_URL)

    ensure_schema(target_engine)

    articles_df = pd.read_sql("SELECT title, content, url, source, state, district, embedding, published_at FROM news_articles", source_engine)

    if articles_df.empty:
        print("No rows found in SQLite source.")
        return

    articles_df["published_at"] = pd.to_datetime(articles_df["published_at"], utc=True, errors="coerce")
    articles_df["ingested_at"] = pd.Timestamp.utcnow()

    with target_engine.begin() as conn:
        conn.exec_driver_sql("TRUNCATE TABLE news_articles RESTART IDENTITY")

    articles_df.to_sql(
        "news_articles",
        target_engine,
        if_exists="append",
        index=False,
        chunksize=1000,
        method="multi",
    )

    print(f"Migrated {len(articles_df)} rows from SQLite to PostgreSQL")


if __name__ == "__main__":
    migrate_sqlite_to_postgres()