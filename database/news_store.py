from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Iterable

import pandas as pd
from neo4j import GraphDatabase
from pandas.errors import DatabaseError
from pymongo import MongoClient, UpdateOne
from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError

from config.config import (
    DATABASE_URL,
    DB_BACKEND,
    MONGODB_DB_NAME,
    MONGODB_URI,
    NEO4J_DATABASE,
    NEO4J_PASSWORD,
    NEO4J_URI,
    NEO4J_USER,
)
from database.db import create_app_engine
from database.schema import ensure_schema as ensure_sql_schema


_SQL_ENGINE = create_app_engine(DATABASE_URL)
_NEO4J_DRIVER = None
_MONGO_CLIENT = None


def using_neo4j_backend() -> bool:
    return DB_BACKEND == "neo4j"


def using_mongodb_backend() -> bool:
    return DB_BACKEND == "mongodb"


def _get_driver():
    global _NEO4J_DRIVER

    if _NEO4J_DRIVER is None:
        _NEO4J_DRIVER = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    return _NEO4J_DRIVER


def _get_mongo_client() -> MongoClient:
    global _MONGO_CLIENT

    if _MONGO_CLIENT is None:
        _MONGO_CLIENT = MongoClient(MONGODB_URI, appname="district-news-ai")

    return _MONGO_CLIENT


def _get_mongo_db():
    return _get_mongo_client()[MONGODB_DB_NAME]


def ensure_data_store_ready() -> None:
    if using_mongodb_backend():
        mongo_db = _get_mongo_db()

        mongo_db["news_articles"].create_index("article_key", unique=True)
        mongo_db["news_articles"].create_index("url")
        mongo_db["news_articles"].create_index("state")
        mongo_db["news_articles"].create_index("district")
        mongo_db["news_articles"].create_index("published_at")
        mongo_db["news_articles"].create_index([("state", 1), ("district", 1)])

        mongo_db["issue_daily_history"].create_index("count_key", unique=True)
        mongo_db["issue_daily_history"].create_index("date")
        mongo_db["issue_daily_history"].create_index([("state", 1), ("district", 1)])
        mongo_db["issue_daily_history"].create_index("issue")

        mongo_db["pipeline_status"].create_index("service", unique=True)
        return

    if not using_neo4j_backend():
        ensure_sql_schema(_SQL_ENGINE)
        return

    driver = _get_driver()

    with driver.session(database=NEO4J_DATABASE) as session:
        session.run(
            """
            CREATE CONSTRAINT article_key_unique IF NOT EXISTS
            FOR (a:Article)
            REQUIRE a.article_key IS UNIQUE
            """
        )
        session.run(
            """
            CREATE INDEX article_published_at IF NOT EXISTS
            FOR (a:Article)
            ON (a.published_at)
            """
        )
        session.run(
            """
            CREATE INDEX article_state IF NOT EXISTS
            FOR (a:Article)
            ON (a.state)
            """
        )
        session.run(
            """
            CREATE INDEX article_district IF NOT EXISTS
            FOR (a:Article)
            ON (a.district)
            """
        )
        session.run(
            """
            CREATE CONSTRAINT state_name_unique IF NOT EXISTS
            FOR (s:State)
            REQUIRE s.name IS UNIQUE
            """
        )
        session.run(
            """
            CREATE CONSTRAINT district_name_state_unique IF NOT EXISTS
            FOR (d:District)
            REQUIRE d.full_key IS UNIQUE
            """
        )
        session.run(
            """
            CREATE CONSTRAINT issue_daily_count_key_unique IF NOT EXISTS
            FOR (h:IssueDailyCount)
            REQUIRE h.count_key IS UNIQUE
            """
        )
        session.run(
            """
            CREATE INDEX issue_daily_count_date IF NOT EXISTS
            FOR (h:IssueDailyCount)
            ON (h.date)
            """
        )
        session.run(
            """
            CREATE INDEX issue_daily_count_state_district IF NOT EXISTS
            FOR (h:IssueDailyCount)
            ON (h.state, h.district)
            """
        )
        session.run(
            """
            CREATE CONSTRAINT pipeline_status_service_unique IF NOT EXISTS
            FOR (p:PipelineStatus)
            REQUIRE p.service IS UNIQUE
            """
        )


def get_existing_urls() -> set[str]:
    if using_mongodb_backend():
        values = _get_mongo_db()["news_articles"].distinct("url", {"url": {"$ne": None}})
        return {value for value in values if value}

    if not using_neo4j_backend():
        try:
            existing = pd.read_sql("SELECT url FROM news_articles", _SQL_ENGINE)
        except (SQLAlchemyError, DatabaseError):
            return set()

        return {value for value in existing["url"].dropna().tolist() if value}

    driver = _get_driver()

    with driver.session(database=NEO4J_DATABASE) as session:
        records = session.run("MATCH (a:Article) WHERE a.url IS NOT NULL RETURN a.url AS url")
        return {record["url"] for record in records if record["url"]}


def _normalize_timestamp(value):
    if value is None:
        return None

    ts = pd.to_datetime(value, utc=True, errors="coerce")

    if pd.isna(ts):
        return None

    return ts.isoformat()


def _article_key(row: dict) -> str:
    url = (row.get("url") or "").strip()
    source = (row.get("source") or "unknown").strip()
    title = (row.get("title") or "").strip()

    if url:
        return f"{source}::{url}"

    return f"{source}::title::{title[:180]}"


def append_articles(df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    write_df = df.copy()
    write_df["published_at"] = write_df["published_at"].apply(_normalize_timestamp)

    if using_mongodb_backend():
        collection = _get_mongo_db()["news_articles"]
        operations: list[UpdateOne] = []

        for row in write_df.to_dict(orient="records"):
            embedding_value = row.get("embedding")

            if embedding_value is not None and not isinstance(embedding_value, str):
                embedding_value = json.dumps(embedding_value)

            raw_state = row.get("state")
            raw_district = row.get("district")
            state = (str(raw_state).strip() if raw_state is not None and raw_state == raw_state else None) or None
            district = (str(raw_district).strip() if raw_district is not None and raw_district == raw_district else None) or None

            payload = {
                "article_key": _article_key(row),
                "title": row.get("title"),
                "content": row.get("content"),
                "url": row.get("url"),
                "source": row.get("source"),
                "state": state,
                "district": district,
                "state_confidence": row.get("state_confidence"),
                "district_confidence": row.get("district_confidence"),
                "embedding": embedding_value,
                "published_at": row.get("published_at"),
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            }

            operations.append(
                UpdateOne(
                    {"article_key": payload["article_key"]},
                    {"$set": payload},
                    upsert=True,
                )
            )

        if operations:
            collection.bulk_write(operations, ordered=False)

        return len(operations)

    if not using_neo4j_backend():
        table_columns = {column["name"] for column in inspect(_SQL_ENGINE).get_columns("news_articles")}

        if "ingested_at" in table_columns:
            write_df["ingested_at"] = datetime.now(timezone.utc)

        if "embedding" in write_df.columns:
            write_df["embedding"] = write_df["embedding"].apply(
                lambda value: value if isinstance(value, str) else json.dumps(value)
            )

        write_df = write_df[[column for column in write_df.columns if column in table_columns]]
        write_df.to_sql("news_articles", _SQL_ENGINE, if_exists="append", index=False, chunksize=1000, method="multi")

        return len(write_df)

    records = []

    for row in write_df.to_dict(orient="records"):
        embedding_value = row.get("embedding")

        if embedding_value is not None and not isinstance(embedding_value, str):
            embedding_value = json.dumps(embedding_value)

        raw_state = row.get("state")
        raw_district = row.get("district")
        state = (str(raw_state).strip() if raw_state is not None and raw_state == raw_state else None) or None
        district = (str(raw_district).strip() if raw_district is not None and raw_district == raw_district else None) or None

        records.append(
            {
                "article_key": _article_key(row),
                "title": row.get("title"),
                "content": row.get("content"),
                "url": row.get("url"),
                "source": row.get("source"),
                "state": state,
                "district": district,
                "state_confidence": row.get("state_confidence"),
                "district_confidence": row.get("district_confidence"),
                "embedding": embedding_value,
                "published_at": row.get("published_at"),
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "district_key": f"{state}::{district}" if state and district else None,
            }
        )

    driver = _get_driver()

    with driver.session(database=NEO4J_DATABASE) as session:
        session.run(
            """
            UNWIND $rows AS row
            MERGE (a:Article {article_key: row.article_key})
            SET a.title = row.title,
                a.content = row.content,
                a.url = row.url,
                a.source = row.source,
                a.state = row.state,
                a.district = row.district,
                a.state_confidence = row.state_confidence,
                a.district_confidence = row.district_confidence,
                a.embedding = row.embedding,
                a.published_at = row.published_at,
                a.ingested_at = row.ingested_at
            FOREACH (_ IN CASE WHEN row.state IS NULL THEN [] ELSE [1] END |
                MERGE (s:State {name: row.state})
                MERGE (a)-[:MENTIONS_STATE]->(s)
            )
            FOREACH (_ IN CASE WHEN row.district_key IS NULL THEN [] ELSE [1] END |
                MERGE (d:District {full_key: row.district_key})
                SET d.name = row.district,
                    d.state = row.state
                MERGE (a)-[:MENTIONS_DISTRICT]->(d)
                FOREACH (__ IN CASE WHEN row.state IS NULL THEN [] ELSE [1] END |
                    MERGE (s2:State {name: row.state})
                    MERGE (d)-[:IN_STATE]->(s2)
                )
            )
            """,
            rows=records,
        )

    return len(records)


def delete_expired_news(retention_days: int) -> int:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).isoformat()

    if using_mongodb_backend():
        result = _get_mongo_db()["news_articles"].delete_many(
            {"published_at": {"$ne": None, "$lt": cutoff}}
        )
        return int(result.deleted_count)

    if not using_neo4j_backend():
        try:
            with _SQL_ENGINE.begin() as conn:
                result = conn.execute(
                    text("DELETE FROM news_articles WHERE published_at IS NOT NULL AND published_at < :cutoff"),
                    {"cutoff": cutoff},
                )
                return result.rowcount or 0
        except SQLAlchemyError:
            return 0

    driver = _get_driver()

    with driver.session(database=NEO4J_DATABASE) as session:
        deleted = session.run(
            """
            MATCH (a:Article)
            WHERE a.published_at IS NOT NULL
              AND datetime(a.published_at) < datetime($cutoff)
            WITH a
            DETACH DELETE a
            RETURN count(a) AS deleted
            """,
            cutoff=cutoff,
        ).single()
        session.run(
            """
            MATCH (d:District)
            WHERE NOT (d)<-[:MENTIONS_DISTRICT]-(:Article)
            DETACH DELETE d
            """
        )
        session.run(
            """
            MATCH (s:State)
            WHERE NOT (s)<-[:MENTIONS_STATE]-(:Article)
              AND NOT (s)<-[:IN_STATE]-(:District)
            DETACH DELETE s
            """
        )

    return int(deleted["deleted"] if deleted else 0)


def get_pending_location_rows() -> pd.DataFrame:
    if using_mongodb_backend():
        records = list(
            _get_mongo_db()["news_articles"].find(
                {
                    "$or": [
                        {"state": {"$exists": False}},
                        {"district": {"$exists": False}},
                        {"state": None},
                        {"district": None},
                    ]
                },
                {
                    "_id": 0,
                    "url": 1,
                    "source": 1,
                    "title": 1,
                    "content": 1,
                    "state": 1,
                    "district": 1,
                    "state_confidence": 1,
                    "district_confidence": 1,
                },
            )
        )
        return pd.DataFrame(records)

    if not using_neo4j_backend():
        try:
            return pd.read_sql(
                "SELECT url, source, title, content, state, district, state_confidence, district_confidence FROM news_articles WHERE state IS NULL OR district IS NULL",
                _SQL_ENGINE,
            )
        except (SQLAlchemyError, DatabaseError):
            return pd.DataFrame()

    driver = _get_driver()

    with driver.session(database=NEO4J_DATABASE) as session:
        records = session.run(
            """
            MATCH (a:Article)
            WHERE a.state IS NULL OR a.district IS NULL
            RETURN a.url AS url,
                   a.source AS source,
                   a.title AS title,
                   a.content AS content,
                   a.state AS state,
                   a.district AS district,
                   a.state_confidence AS state_confidence,
                   a.district_confidence AS district_confidence
            """
        )
        rows = [record.data() for record in records]

    return pd.DataFrame(rows)


def update_article_location(
    *,
    url: str,
    source: str,
    state: str | None,
    district: str | None,
    state_confidence: float | None,
    district_confidence: float | None,
) -> None:
    if using_mongodb_backend():
        collection = _get_mongo_db()["news_articles"]
        match = {"url": url, "source": source}

        if state is not None:
            collection.update_many(
                {
                    **match,
                    "$or": [
                        {"state": {"$exists": False}},
                        {"state": None},
                        {"state": ""},
                    ],
                },
                {"$set": {"state": state, "state_confidence": state_confidence}},
            )

        if district is not None:
            collection.update_many(
                {
                    **match,
                    "$or": [
                        {"district": {"$exists": False}},
                        {"district": None},
                        {"district": ""},
                    ],
                },
                {"$set": {"district": district, "district_confidence": district_confidence}},
            )
        return

    if not using_neo4j_backend():
        with _SQL_ENGINE.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE news_articles
                    SET state = COALESCE(state, :state),
                        district = COALESCE(district, :district),
                        state_confidence = CASE
                            WHEN state IS NULL AND :state IS NOT NULL THEN :state_confidence
                            ELSE state_confidence
                        END,
                        district_confidence = CASE
                            WHEN district IS NULL AND :district IS NOT NULL THEN :district_confidence
                            ELSE district_confidence
                        END
                    WHERE url = :url AND source = :source
                    """
                ),
                {
                    "state": state,
                    "district": district,
                    "state_confidence": state_confidence,
                    "district_confidence": district_confidence,
                    "url": url,
                    "source": source,
                },
            )
        return

    district_key = f"{state}::{district}" if state and district else None
    driver = _get_driver()

    with driver.session(database=NEO4J_DATABASE) as session:
        session.run(
            """
            MATCH (a:Article)
            WHERE a.url = $url AND a.source = $source
            SET a.state = coalesce(a.state, $state),
                a.district = coalesce(a.district, $district),
                a.state_confidence = CASE
                    WHEN a.state IS NULL AND $state IS NOT NULL THEN $state_confidence
                    ELSE a.state_confidence
                END,
                a.district_confidence = CASE
                    WHEN a.district IS NULL AND $district IS NOT NULL THEN $district_confidence
                    ELSE a.district_confidence
                END
            FOREACH (_ IN CASE WHEN $state IS NULL THEN [] ELSE [1] END |
                MERGE (s:State {name: $state})
                MERGE (a)-[:MENTIONS_STATE]->(s)
            )
            FOREACH (_ IN CASE WHEN $district_key IS NULL THEN [] ELSE [1] END |
                MERGE (d:District {full_key: $district_key})
                SET d.name = $district,
                    d.state = $state
                MERGE (a)-[:MENTIONS_DISTRICT]->(d)
                FOREACH (__ IN CASE WHEN $state IS NULL THEN [] ELSE [1] END |
                    MERGE (s2:State {name: $state})
                    MERGE (d)-[:IN_STATE]->(s2)
                )
            )
            """,
            url=url,
            source=source,
            state=state,
            district=district,
            state_confidence=state_confidence,
            district_confidence=district_confidence,
            district_key=district_key,
        )


def _records_to_df(records: Iterable[dict]) -> pd.DataFrame:
    rows = list(records)

    if not rows:
        return pd.DataFrame(
            columns=[
                "title",
                "content",
                "url",
                "source",
                "state",
                "district",
                "state_confidence",
                "district_confidence",
                "published_at",
            ]
        )

    frame = pd.DataFrame(rows)
    expected_columns = [
        "title",
        "content",
        "url",
        "source",
        "state",
        "district",
        "state_confidence",
        "district_confidence",
        "published_at",
    ]

    for column in expected_columns:
        if column not in frame.columns:
            frame[column] = None

    return frame[expected_columns]


def _normalize_history_row(row: dict) -> dict | None:
    date_value = str(row.get("date") or "").strip()
    state = str(row.get("state") or "").strip().lower()
    district = str(row.get("district") or "").strip().lower()
    issue = str(row.get("issue") or "").strip().lower()

    if not date_value or not state or not district or not issue:
        return None

    return {
        "count_key": f"{date_value}::{state}::{district}::{issue}",
        "date": date_value,
        "state": state,
        "district": district,
        "issue": issue,
        "count": int(row.get("count", 0) or 0),
    }


def upsert_issue_history(rows: list[dict], retention_days: int) -> int:
    normalized_rows = []

    for row in rows:
        normalized = _normalize_history_row(row)

        if normalized is not None:
            normalized_rows.append(normalized)

    if not normalized_rows:
        return 0

    retained_cutoff = (datetime.now(timezone.utc).date() - timedelta(days=retention_days)).isoformat()

    if using_mongodb_backend():
        collection = _get_mongo_db()["issue_daily_history"]
        collection.delete_many({"date": {"$lt": retained_cutoff}})

        operations = [
            UpdateOne(
                {"count_key": row["count_key"]},
                {
                    "$set": {
                        **row,
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }
                },
                upsert=True,
            )
            for row in normalized_rows
        ]

        if operations:
            collection.bulk_write(operations, ordered=False)

        return len(operations)

    if not using_neo4j_backend():
        with _SQL_ENGINE.begin() as conn:
            conn.execute(
                text("DELETE FROM issue_daily_history WHERE date < :cutoff"),
                {"cutoff": retained_cutoff},
            )

            for row in normalized_rows:
                conn.execute(
                    text("DELETE FROM issue_daily_history WHERE count_key = :count_key"),
                    {"count_key": row["count_key"]},
                )

            pd.DataFrame(normalized_rows).to_sql(
                "issue_daily_history",
                _SQL_ENGINE,
                if_exists="append",
                index=False,
                chunksize=1000,
                method="multi",
            )

        return len(normalized_rows)

    driver = _get_driver()

    with driver.session(database=NEO4J_DATABASE) as session:
        session.run(
            """
            MATCH (h:IssueDailyCount)
            WHERE h.date < $cutoff
            DETACH DELETE h
            """,
            cutoff=retained_cutoff,
        )
        session.run(
            """
            UNWIND $rows AS row
            MERGE (h:IssueDailyCount {count_key: row.count_key})
            SET h.date = row.date,
                h.state = row.state,
                h.district = row.district,
                h.issue = row.issue,
                h.count = row.count,
                h.updated_at = $updated_at
            """,
            rows=normalized_rows,
            updated_at=datetime.now(timezone.utc).isoformat(),
        )

    return len(normalized_rows)


def load_issue_history(state: str, district: str, days: int = 30) -> list[dict]:
    normalized_state = str(state or "").strip().lower()
    normalized_district = str(district or "").strip().lower()

    if not normalized_state or not normalized_district:
        return []

    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=days)).isoformat()

    if using_mongodb_backend():
        records = (
            _get_mongo_db()["issue_daily_history"]
            .find(
                {
                    "state": normalized_state,
                    "district": normalized_district,
                    "date": {"$gte": cutoff},
                },
                {"_id": 0, "date": 1, "state": 1, "district": 1, "issue": 1, "count": 1},
            )
            .sort("date", 1)
        )
        return list(records)

    if not using_neo4j_backend():
        try:
            rows = pd.read_sql(
                text(
                    """
                    SELECT date, state, district, issue, count
                    FROM issue_daily_history
                    WHERE lower(trim(state)) = :state
                      AND lower(trim(district)) = :district
                      AND date >= :cutoff
                    ORDER BY date ASC
                    """
                ),
                _SQL_ENGINE,
                params={"state": normalized_state, "district": normalized_district, "cutoff": cutoff},
            )
        except (SQLAlchemyError, DatabaseError):
            return []

        return rows.to_dict(orient="records")

    driver = _get_driver()

    with driver.session(database=NEO4J_DATABASE) as session:
        records = [
            record.data()
            for record in session.run(
                """
                MATCH (h:IssueDailyCount)
                WHERE toLower(h.state) = toLower($state)
                  AND toLower(h.district) = toLower($district)
                  AND h.date >= $cutoff
                RETURN h.date AS date,
                       h.state AS state,
                       h.district AS district,
                       h.issue AS issue,
                       h.count AS count
                ORDER BY h.date ASC
                """,
                state=normalized_state,
                district=normalized_district,
                cutoff=cutoff,
            )
        ]

    return records


def upsert_pipeline_status(
    *,
    service: str,
    last_successful_run_at: str,
    last_inserted_article_count: int,
    last_collected_count: int,
    last_unique_count: int,
    last_backfilled_count: int,
    last_run_result: dict | None = None,
) -> None:
    result_json = None if last_run_result is None else json.dumps(last_run_result)
    updated_at = datetime.now(timezone.utc).isoformat()

    if using_mongodb_backend():
        _get_mongo_db()["pipeline_status"].update_one(
            {"service": service},
            {
                "$set": {
                    "service": service,
                    "last_successful_run_at": last_successful_run_at,
                    "last_inserted_article_count": int(last_inserted_article_count),
                    "last_collected_count": int(last_collected_count),
                    "last_unique_count": int(last_unique_count),
                    "last_backfilled_count": int(last_backfilled_count),
                    "last_run_result": result_json,
                    "updated_at": updated_at,
                }
            },
            upsert=True,
        )
        return

    if not using_neo4j_backend():
        with _SQL_ENGINE.begin() as conn:
            conn.execute(text("DELETE FROM pipeline_status WHERE service = :service"), {"service": service})
            conn.execute(
                text(
                    """
                    INSERT INTO pipeline_status (
                        service,
                        last_successful_run_at,
                        last_inserted_article_count,
                        last_collected_count,
                        last_unique_count,
                        last_backfilled_count,
                        last_run_result,
                        updated_at
                    )
                    VALUES (
                        :service,
                        :last_successful_run_at,
                        :last_inserted_article_count,
                        :last_collected_count,
                        :last_unique_count,
                        :last_backfilled_count,
                        :last_run_result,
                        :updated_at
                    )
                    """
                ),
                {
                    "service": service,
                    "last_successful_run_at": last_successful_run_at,
                    "last_inserted_article_count": int(last_inserted_article_count),
                    "last_collected_count": int(last_collected_count),
                    "last_unique_count": int(last_unique_count),
                    "last_backfilled_count": int(last_backfilled_count),
                    "last_run_result": result_json,
                    "updated_at": updated_at,
                },
            )
        return

    driver = _get_driver()

    with driver.session(database=NEO4J_DATABASE) as session:
        session.run(
            """
            MERGE (p:PipelineStatus {service: $service})
            SET p.last_successful_run_at = $last_successful_run_at,
                p.last_inserted_article_count = $last_inserted_article_count,
                p.last_collected_count = $last_collected_count,
                p.last_unique_count = $last_unique_count,
                p.last_backfilled_count = $last_backfilled_count,
                p.last_run_result = $last_run_result,
                p.updated_at = $updated_at
            """,
            service=service,
            last_successful_run_at=last_successful_run_at,
            last_inserted_article_count=int(last_inserted_article_count),
            last_collected_count=int(last_collected_count),
            last_unique_count=int(last_unique_count),
            last_backfilled_count=int(last_backfilled_count),
            last_run_result=result_json,
            updated_at=updated_at,
        )


def get_pipeline_status(service: str = "scheduler") -> dict | None:
    if using_mongodb_backend():
        payload = _get_mongo_db()["pipeline_status"].find_one({"service": service}, {"_id": 0})

        if payload is None:
            return None

        if payload.get("last_run_result"):
            try:
                payload["last_run_result"] = json.loads(payload["last_run_result"])
            except (TypeError, json.JSONDecodeError):
                pass

        return payload

    if not using_neo4j_backend():
        try:
            rows = pd.read_sql(
                text(
                    """
                    SELECT service,
                           last_successful_run_at,
                           last_inserted_article_count,
                           last_collected_count,
                           last_unique_count,
                           last_backfilled_count,
                           last_run_result,
                           updated_at
                    FROM pipeline_status
                    WHERE service = :service
                    LIMIT 1
                    """
                ),
                _SQL_ENGINE,
                params={"service": service},
            )
        except (SQLAlchemyError, DatabaseError):
            return None

        if rows.empty:
            return None

        row = rows.iloc[0].to_dict()

        if row.get("last_run_result"):
            try:
                row["last_run_result"] = json.loads(row["last_run_result"])
            except (TypeError, json.JSONDecodeError):
                pass

        return row

    driver = _get_driver()

    with driver.session(database=NEO4J_DATABASE) as session:
        record = session.run(
            """
            MATCH (p:PipelineStatus {service: $service})
            RETURN p.service AS service,
                   p.last_successful_run_at AS last_successful_run_at,
                   p.last_inserted_article_count AS last_inserted_article_count,
                   p.last_collected_count AS last_collected_count,
                   p.last_unique_count AS last_unique_count,
                   p.last_backfilled_count AS last_backfilled_count,
                   p.last_run_result AS last_run_result,
                   p.updated_at AS updated_at
            LIMIT 1
            """,
            service=service,
        ).single()

    if record is None:
        return None

    payload = record.data()

    if payload.get("last_run_result"):
        try:
            payload["last_run_result"] = json.loads(payload["last_run_result"])
        except (TypeError, json.JSONDecodeError):
            pass

    return payload


def load_recent_articles(retention_days: int, state: str | None = None) -> pd.DataFrame:
    cutoff = (datetime.utcnow() - timedelta(days=retention_days)).strftime("%Y-%m-%d %H:%M:%S")

    if using_mongodb_backend():
        cutoff_iso = pd.Timestamp(cutoff, tz="UTC").isoformat()
        date_filter = {
            "$or": [
                {"published_at": None},
                {"published_at": {"$gt": cutoff_iso}},
            ]
        }
        query = dict(date_filter)

        if state:
            query["state"] = {"$regex": f"^{re.escape(state)}$", "$options": "i"}

        records = _get_mongo_db()["news_articles"].find(
            query,
            {
                "_id": 0,
                "title": 1,
                "content": 1,
                "url": 1,
                "source": 1,
                "state": 1,
                "district": 1,
                "state_confidence": 1,
                "district_confidence": 1,
                "published_at": 1,
            },
        )
        return _records_to_df(records)

    if not using_neo4j_backend():
        query = text(
            """
            SELECT title, content, url, source, state, district, state_confidence, district_confidence, published_at
            FROM news_articles
            WHERE (published_at IS NULL OR published_at > :cutoff)
            """
        )
        return pd.read_sql(query, _SQL_ENGINE, params={"cutoff": cutoff})

    driver = _get_driver()
    params = {"cutoff": pd.Timestamp(cutoff, tz="UTC").isoformat()}
    state_clause = ""

    if state:
        params["state"] = state
        state_clause = "AND toLower(coalesce(a.state, '')) = toLower($state)"

    cypher = f"""
        MATCH (a:Article)
        WHERE (a.published_at IS NULL OR datetime(a.published_at) > datetime($cutoff))
        {state_clause}
        RETURN a.title AS title,
               a.content AS content,
               a.url AS url,
               a.source AS source,
               a.state AS state,
               a.district AS district,
             a.state_confidence AS state_confidence,
             a.district_confidence AS district_confidence,
               a.published_at AS published_at
    """

    with driver.session(database=NEO4J_DATABASE) as session:
        records = [record.data() for record in session.run(cypher, **params)]

    return _records_to_df(records)


def load_district_articles(retention_days: int, state: str, district: str, variants: list[str]) -> pd.DataFrame:
    cutoff = pd.Timestamp(datetime.utcnow() - timedelta(days=retention_days), tz="UTC").isoformat()

    if using_mongodb_backend():
        collection = _get_mongo_db()["news_articles"]
        state_exact = {"$regex": f"^{re.escape(state)}$", "$options": "i"}
        district_exact = {"$regex": f"^{re.escape(district)}$", "$options": "i"}
        date_filter = {"$or": [{"published_at": None}, {"published_at": {"$gt": cutoff}}]}

        direct_records = collection.find(
            {
                "$and": [
                    date_filter,
                    {"state": state_exact},
                    {"district": district_exact},
                ]
            },
            {
                "_id": 0,
                "title": 1,
                "content": 1,
                "url": 1,
                "source": 1,
                "state": 1,
                "district": 1,
                "state_confidence": 1,
                "district_confidence": 1,
                "published_at": 1,
            },
        )
        direct_rows = list(direct_records)

        if direct_rows:
            return _records_to_df(direct_rows)

        if variants:
            variant_patterns = [re.compile(f"^{re.escape(value)}$", re.IGNORECASE) for value in variants]
            variant_records = collection.find(
                {
                    "$and": [
                        date_filter,
                        {"state": state_exact},
                        {"district": {"$in": variant_patterns}},
                    ]
                },
                {
                    "_id": 0,
                    "title": 1,
                    "content": 1,
                    "url": 1,
                    "source": 1,
                    "state": 1,
                    "district": 1,
                    "state_confidence": 1,
                    "district_confidence": 1,
                    "published_at": 1,
                },
            )
            variant_rows = list(variant_records)

            if variant_rows:
                return _records_to_df(variant_rows)

        district_contains = {"$regex": re.escape(district), "$options": "i"}
        fallback_rows = list(
            collection.find(
                {
                    "$and": [
                        date_filter,
                        {
                            "$or": [
                                {"state": state_exact},
                                {"state": None},
                                {"state": ""},
                                {"state": {"$exists": False}},
                            ]
                        },
                        {
                            "$or": [
                                {"title": district_contains},
                                {"content": district_contains},
                            ]
                        },
                    ]
                },
                {
                    "_id": 0,
                    "title": 1,
                    "content": 1,
                    "url": 1,
                    "source": 1,
                    "state": 1,
                    "district": 1,
                    "state_confidence": 1,
                    "district_confidence": 1,
                    "published_at": 1,
                },
            )
        )

        if not fallback_rows:
            return _records_to_df([])

        for row in fallback_rows:
            row["district"] = district

        return _records_to_df(fallback_rows)

    if not using_neo4j_backend():
        direct_query = text(
            """
            SELECT title, content, url, source, state, district, state_confidence, district_confidence, published_at
            FROM news_articles
            WHERE district=:district
            AND lower(trim(state))=:state
            AND (published_at IS NULL OR published_at > :cutoff)
            """
        )
        direct_df = pd.read_sql(direct_query, _SQL_ENGINE, params={"district": district, "state": state, "cutoff": cutoff})

        if not direct_df.empty:
            return direct_df

        if variants:
            params = {"state": state, "cutoff": cutoff}
            conditions = []

            for idx, variant in enumerate(variants):
                key = f"district_{idx}"
                conditions.append(f"district = :{key}")
                params[key] = variant

            variant_query = text(
                f"""
                SELECT title, content, url, source, state, district, published_at
                                         , state_confidence, district_confidence
                FROM news_articles
                WHERE lower(trim(state))=:state
                  AND ({' OR '.join(conditions)})
                  AND (published_at IS NULL OR published_at > :cutoff)
                """
            )
            variant_df = pd.read_sql(variant_query, _SQL_ENGINE, params=params)

            if not variant_df.empty:
                return variant_df

        fallback_query = text(
            """
            SELECT title, content, url, source, state, district, published_at
                                 , state_confidence, district_confidence
            FROM news_articles
            WHERE (lower(trim(state))=:state OR state IS NULL OR trim(state)='')
              AND (
                lower(title) LIKE :district_like
                OR lower(content) LIKE :district_like
              )
              AND (published_at IS NULL OR published_at > :cutoff)
            """
        )

        fallback_df = pd.read_sql(
            fallback_query,
            _SQL_ENGINE,
            params={"state": state, "district_like": f"%{district}%", "cutoff": cutoff},
        )

        if fallback_df.empty:
            return fallback_df

        fallback_df = fallback_df.copy()
        fallback_df["district"] = district

        return fallback_df

    driver = _get_driver()

    with driver.session(database=NEO4J_DATABASE) as session:
        direct_records = [
            record.data()
            for record in session.run(
                """
                MATCH (a:Article)
                WHERE (a.published_at IS NULL OR datetime(a.published_at) > datetime($cutoff))
                  AND toLower(coalesce(a.state, '')) = toLower($state)
                  AND toLower(coalesce(a.district, '')) = toLower($district)
                RETURN a.title AS title,
                       a.content AS content,
                       a.url AS url,
                       a.source AS source,
                       a.state AS state,
                       a.district AS district,
                      a.state_confidence AS state_confidence,
                      a.district_confidence AS district_confidence,
                       a.published_at AS published_at
                """,
                cutoff=cutoff,
                state=state,
                district=district,
            )
        ]

        if direct_records:
            return _records_to_df(direct_records)

        if variants:
            variant_records = [
                record.data()
                for record in session.run(
                    """
                    MATCH (a:Article)
                    WHERE (a.published_at IS NULL OR datetime(a.published_at) > datetime($cutoff))
                      AND toLower(coalesce(a.state, '')) = toLower($state)
                      AND toLower(coalesce(a.district, '')) IN [value IN $variants | toLower(value)]
                    RETURN a.title AS title,
                           a.content AS content,
                           a.url AS url,
                           a.source AS source,
                           a.state AS state,
                           a.district AS district,
                              a.state_confidence AS state_confidence,
                              a.district_confidence AS district_confidence,
                           a.published_at AS published_at
                    """,
                    cutoff=cutoff,
                    state=state,
                    variants=variants,
                )
            ]

            if variant_records:
                return _records_to_df(variant_records)

        fallback_records = [
            record.data()
            for record in session.run(
                """
                MATCH (a:Article)
                WHERE (a.published_at IS NULL OR datetime(a.published_at) > datetime($cutoff))
                  AND (a.state IS NULL OR trim(a.state) = '' OR toLower(a.state) = toLower($state))
                  AND (
                    toLower(coalesce(a.title, '')) CONTAINS toLower($district)
                    OR toLower(coalesce(a.content, '')) CONTAINS toLower($district)
                  )
                RETURN a.title AS title,
                       a.content AS content,
                       a.url AS url,
                       a.source AS source,
                       a.state AS state,
                       $district AS district,
                      a.state_confidence AS state_confidence,
                      a.district_confidence AS district_confidence,
                       a.published_at AS published_at
                """,
                cutoff=cutoff,
                state=state,
                district=district,
            )
        ]

    return _records_to_df(fallback_records)


def get_assigned_state_district_pairs() -> pd.DataFrame:
    if using_mongodb_backend():
        records = list(
            _get_mongo_db()["news_articles"].aggregate(
                [
                    {
                        "$match": {
                            "state": {"$exists": True, "$nin": [None, ""]},
                            "district": {"$exists": True, "$nin": [None, ""]},
                        }
                    },
                    {
                        "$project": {
                            "state": {"$toLower": "$state"},
                            "district": {"$toLower": "$district"},
                        }
                    },
                    {"$group": {"_id": {"state": "$state", "district": "$district"}}},
                    {
                        "$project": {
                            "_id": 0,
                            "state": "$_id.state",
                            "district": "$_id.district",
                        }
                    },
                ]
            )
        )
        return pd.DataFrame(records)

    if not using_neo4j_backend():
        query = text(
            """
            SELECT lower(trim(state)) AS state, lower(trim(district)) AS district
            FROM news_articles
            WHERE state IS NOT NULL AND trim(state) <> ''
              AND district IS NOT NULL AND trim(district) <> ''
            GROUP BY lower(trim(state)), lower(trim(district))
            """
        )
        return pd.read_sql(query, _SQL_ENGINE)

    driver = _get_driver()

    with driver.session(database=NEO4J_DATABASE) as session:
        records = [
            record.data()
            for record in session.run(
                """
                MATCH (a:Article)
                WHERE a.state IS NOT NULL AND trim(a.state) <> ''
                  AND a.district IS NOT NULL AND trim(a.district) <> ''
                RETURN DISTINCT toLower(trim(a.state)) AS state,
                                toLower(trim(a.district)) AS district
                """
            )
        ]

    return pd.DataFrame(records)
