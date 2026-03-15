import sys
from pathlib import Path

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from config.config import BASE_DIR
from database.news_store import append_articles, ensure_data_store_ready

CSV_PATH = Path(BASE_DIR) / "data" / "reports" / "news_articles_export.csv"
CHUNK_SIZE = 1000


def migrate_csv_to_neo4j():
    if not CSV_PATH.exists():
        print(f"CSV not found at {CSV_PATH}")
        return

    ensure_data_store_ready()

    total_rows = sum(1 for _ in open(CSV_PATH, encoding="utf-8")) - 1  # subtract header
    print(f"Migrating {total_rows} rows from CSV to Neo4j...")

    migrated = 0
    for chunk_df in pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE):
        inserted = append_articles(chunk_df)
        migrated += inserted
        print(f"  Migrated {migrated}/{total_rows}...")

    print(f"Done. Migrated {migrated} rows into Neo4j.")


if __name__ == "__main__":
    migrate_csv_to_neo4j()
