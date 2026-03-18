import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from neo4j import GraphDatabase
from pymongo import MongoClient, ReplaceOne
from pymongo.errors import PyMongoError

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from config.config import NEO4J_DATABASE, NEO4J_PASSWORD, NEO4J_URI, NEO4J_USER


def _chunked(iterable, size: int):
    bucket = []

    for item in iterable:
        bucket.append(item)

        if len(bucket) >= size:
            yield bucket
            bucket = []

    if bucket:
        yield bucket


def _require_mongo_uri(explicit_uri: str | None) -> str:
    uri = explicit_uri or os.getenv("MONGODB_URI")

    if not uri:
        raise ValueError("Missing MongoDB URI. Pass --mongo-uri or set MONGODB_URI.")

    if "query.mongodb.net" in uri:
        raise ValueError(
            "Provided URI points to Atlas SQL/BI endpoint (query.mongodb.net), which is read-only for this script. "
            "Use the regular Atlas cluster URI (mongodb+srv://...mongodb.net/...) from Atlas Connect dialog."
        )

    return uri


def _replace_by_key(records: list[dict], key_name: str) -> list[ReplaceOne]:
    operations = []

    for record in records:
        key_value = record.get(key_name)

        if key_value is None:
            continue

        operations.append(ReplaceOne({key_name: key_value}, record, upsert=True))

    return operations


def _migrate_articles(neo_session, mongo_db, batch_size: int) -> int:
    cursor = neo_session.run(
        """
        MATCH (a:Article)
        RETURN a.article_key AS article_key,
               a.title AS title,
               a.content AS content,
               a.url AS url,
               a.source AS source,
               a.state AS state,
               a.district AS district,
               a.state_confidence AS state_confidence,
               a.district_confidence AS district_confidence,
               a.embedding AS embedding,
               a.published_at AS published_at,
               a.ingested_at AS ingested_at
        """
    )

    total = 0
    collection = mongo_db["news_articles"]

    for batch in _chunked((record.data() for record in cursor), batch_size):
        operations = _replace_by_key(batch, "article_key")

        if operations:
            collection.bulk_write(operations, ordered=False)
            total += len(operations)

    collection.create_index("article_key", unique=True)
    collection.create_index("url")
    collection.create_index("state")
    collection.create_index("district")
    collection.create_index("published_at")

    return total


def _migrate_issue_history(neo_session, mongo_db, batch_size: int) -> int:
    cursor = neo_session.run(
        """
        MATCH (h:IssueDailyCount)
        RETURN h.count_key AS count_key,
               h.date AS date,
               h.state AS state,
               h.district AS district,
               h.issue AS issue,
               h.count AS count,
               h.updated_at AS updated_at
        """
    )

    total = 0
    collection = mongo_db["issue_daily_history"]

    for batch in _chunked((record.data() for record in cursor), batch_size):
        operations = _replace_by_key(batch, "count_key")

        if operations:
            collection.bulk_write(operations, ordered=False)
            total += len(operations)

    collection.create_index("count_key", unique=True)
    collection.create_index("date")
    collection.create_index([("state", 1), ("district", 1)])
    collection.create_index("issue")

    return total


def _migrate_pipeline_status(neo_session, mongo_db, batch_size: int) -> int:
    cursor = neo_session.run(
        """
        MATCH (p:PipelineStatus)
        RETURN p.service AS service,
               p.last_successful_run_at AS last_successful_run_at,
               p.last_inserted_article_count AS last_inserted_article_count,
               p.last_collected_count AS last_collected_count,
               p.last_unique_count AS last_unique_count,
               p.last_backfilled_count AS last_backfilled_count,
               p.last_run_result AS last_run_result,
               p.updated_at AS updated_at
        """
    )

    total = 0
    collection = mongo_db["pipeline_status"]

    for batch in _chunked((record.data() for record in cursor), batch_size):
        operations = _replace_by_key(batch, "service")

        if operations:
            collection.bulk_write(operations, ordered=False)
            total += len(operations)

    collection.create_index("service", unique=True)

    return total


def _migrate_states(neo_session, mongo_db, batch_size: int) -> int:
    cursor = neo_session.run(
        """
        MATCH (s:State)
        RETURN s.name AS name
        """
    )

    total = 0
    collection = mongo_db["states"]

    for batch in _chunked((record.data() for record in cursor), batch_size):
        operations = _replace_by_key(batch, "name")

        if operations:
            collection.bulk_write(operations, ordered=False)
            total += len(operations)

    collection.create_index("name", unique=True)

    return total


def _migrate_districts(neo_session, mongo_db, batch_size: int) -> int:
    cursor = neo_session.run(
        """
        MATCH (d:District)
        RETURN d.full_key AS full_key,
               d.name AS name,
               d.state AS state
        """
    )

    total = 0
    collection = mongo_db["districts"]

    for batch in _chunked((record.data() for record in cursor), batch_size):
        operations = _replace_by_key(batch, "full_key")

        if operations:
            collection.bulk_write(operations, ordered=False)
            total += len(operations)

    collection.create_index("full_key", unique=True)
    collection.create_index([("state", 1), ("name", 1)])

    return total


def migrate_neo4j_to_mongodb(
    *,
    mongo_uri: str | None = None,
    mongo_db_name: str | None = None,
    neo4j_uri: str | None = None,
    neo4j_user: str | None = None,
    neo4j_password: str | None = None,
    neo4j_database: str | None = None,
    batch_size: int = 1000,
    clear_target: bool = False,
) -> dict:
    target_uri = _require_mongo_uri(mongo_uri)
    target_db_name = mongo_db_name or os.getenv("MONGODB_DB_NAME") or "district_news_ai"

    source_uri = neo4j_uri or NEO4J_URI
    source_user = neo4j_user or NEO4J_USER
    source_password = neo4j_password or NEO4J_PASSWORD
    source_database = neo4j_database or NEO4J_DATABASE

    neo_driver = GraphDatabase.driver(source_uri, auth=(source_user, source_password))
    mongo_client = MongoClient(target_uri, appname="district-news-neo4j-migration")
    mongo_db = mongo_client[target_db_name]

    try:
        mongo_client.admin.command("ping")

        if clear_target:
            for name in ["news_articles", "issue_daily_history", "pipeline_status", "states", "districts"]:
                mongo_db[name].delete_many({})

        with neo_driver.session(database=source_database) as neo_session:
            result = {
                "news_articles": _migrate_articles(neo_session, mongo_db, batch_size),
                "issue_daily_history": _migrate_issue_history(neo_session, mongo_db, batch_size),
                "pipeline_status": _migrate_pipeline_status(neo_session, mongo_db, batch_size),
                "states": _migrate_states(neo_session, mongo_db, batch_size),
                "districts": _migrate_districts(neo_session, mongo_db, batch_size),
                "migrated_at_utc": datetime.now(timezone.utc).isoformat(),
                "mongo_database": target_db_name,
            }
    finally:
        neo_driver.close()
        mongo_client.close()

    return result


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate Neo4j data to MongoDB Atlas.")
    parser.add_argument("--mongo-uri", default=None, help="MongoDB connection string. Defaults to MONGODB_URI env var.")
    parser.add_argument("--mongo-db", default=None, help="Target MongoDB database name. Defaults to MONGODB_DB_NAME or district_news_ai.")
    parser.add_argument("--neo4j-uri", default=None, help="Neo4j URI override.")
    parser.add_argument("--neo4j-user", default=None, help="Neo4j user override.")
    parser.add_argument("--neo4j-password", default=None, help="Neo4j password override.")
    parser.add_argument("--neo4j-database", default=None, help="Neo4j database override.")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for bulk writes.")
    parser.add_argument("--clear-target", action="store_true", help="Delete existing docs in target collections before migration.")
    return parser.parse_args()


if __name__ == "__main__":
    options = _parse_args()

    try:
        summary = migrate_neo4j_to_mongodb(
            mongo_uri=options.mongo_uri,
            mongo_db_name=options.mongo_db,
            neo4j_uri=options.neo4j_uri,
            neo4j_user=options.neo4j_user,
            neo4j_password=options.neo4j_password,
            neo4j_database=options.neo4j_database,
            batch_size=max(100, options.batch_size),
            clear_target=options.clear_target,
        )
    except (ValueError, PyMongoError) as exc:
        raise SystemExit(f"Migration failed: {exc}") from exc

    print("Neo4j to MongoDB migration completed.")
    for key, value in summary.items():
        print(f"- {key}: {value}")