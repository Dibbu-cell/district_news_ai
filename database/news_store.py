from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Iterable

import pandas as pd
from neo4j import GraphDatabase
from pandas.errors import DatabaseError
from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError

from config.config import DATABASE_URL, DB_BACKEND, NEO4J_DATABASE, NEO4J_PASSWORD, NEO4J_URI, NEO4J_USER
from database.db import create_app_engine
from database.schema import ensure_schema as ensure_sql_schema


_SQL_ENGINE = create_app_engine(DATABASE_URL)
_NEO4J_DRIVER = None


def using_neo4j_backend() -> bool:
    return DB_BACKEND == "neo4j"


def _get_driver():
    global _NEO4J_DRIVER

    if _NEO4J_DRIVER is None:
        _NEO4J_DRIVER = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    return _NEO4J_DRIVER


def ensure_data_store_ready() -> None:
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


def get_existing_urls() -> set[str]:
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
        return pd.DataFrame(columns=["title", "content", "url", "source", "state", "district", "published_at"])

    frame = pd.DataFrame(rows)
    expected_columns = ["title", "content", "url", "source", "state", "district", "published_at"]

    for column in expected_columns:
        if column not in frame.columns:
            frame[column] = None

    return frame[expected_columns]


def load_recent_articles(retention_days: int, state: str | None = None) -> pd.DataFrame:
    cutoff = (datetime.utcnow() - timedelta(days=retention_days)).strftime("%Y-%m-%d %H:%M:%S")

    if not using_neo4j_backend():
        query = text(
            """
            SELECT title, content, url, source, state, district, published_at
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
               a.published_at AS published_at
    """

    with driver.session(database=NEO4J_DATABASE) as session:
        records = [record.data() for record in session.run(cypher, **params)]

    return _records_to_df(records)


def load_district_articles(retention_days: int, state: str, district: str, variants: list[str]) -> pd.DataFrame:
    cutoff = pd.Timestamp(datetime.utcnow() - timedelta(days=retention_days), tz="UTC").isoformat()

    if not using_neo4j_backend():
        direct_query = text(
            """
            SELECT title, content, url, source, state, district, published_at
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
                       a.published_at AS published_at
                """,
                cutoff=cutoff,
                state=state,
                district=district,
            )
        ]

    return _records_to_df(fallback_records)


def get_assigned_state_district_pairs() -> pd.DataFrame:
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
