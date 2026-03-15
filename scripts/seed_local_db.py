import sys
from pathlib import Path

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from database.news_store import append_articles, ensure_data_store_ready, get_existing_urls


SEED_ROWS = [
    {
        "title": "Water shortage protests in Hyderabad",
        "content": "Residents in hyderabad reported severe water shortage and civic protests in multiple wards.",
        "url": "https://example.com/hyd-1",
        "source": "seed",
        "state": "andhra pradesh",
        "district": "hyderabad",
        "embedding": [0.91, 0.08, 0.01],
        "published_at": "2026-03-12 09:00:00",
    },
    {
        "title": "Hyderabad tanker supply disrupted",
        "content": "Several hyderabad neighborhoods faced tanker delays and drinking water disruption.",
        "url": "https://example.com/hyd-2",
        "source": "seed",
        "state": "andhra pradesh",
        "district": "hyderabad",
        "embedding": [0.89, 0.10, 0.01],
        "published_at": "2026-03-12 10:30:00",
    },
    {
        "title": "Drainage complaints rise in Hyderabad",
        "content": "Citizens in hyderabad raised repeated drainage and sanitation complaints after local rainfall.",
        "url": "https://example.com/hyd-3",
        "source": "seed",
        "state": "andhra pradesh",
        "district": "hyderabad",
        "embedding": [0.92, 0.07, 0.01],
        "published_at": "2026-03-12 12:15:00",
    },
    {
        "title": "Road repair demand in Guntur",
        "content": "Commuters in guntur demanded urgent road repair and pothole maintenance on key stretches.",
        "url": "https://example.com/gun-1",
        "source": "seed",
        "state": "andhra pradesh",
        "district": "guntur",
        "embedding": [0.10, 0.88, 0.02],
        "published_at": "2026-03-12 11:00:00",
    },
]


def seed_local_db():
    ensure_data_store_ready()

    existing_urls = get_existing_urls()
    pending_rows = [row for row in SEED_ROWS if row["url"] not in existing_urls]

    if not pending_rows:
        print("Seed rows already present; no new rows inserted.")
        return

    inserted = append_articles(pd.DataFrame(pending_rows))
    print(f"Inserted {inserted} seed rows into news_articles")


if __name__ == "__main__":
    seed_local_db()
