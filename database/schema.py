from sqlalchemy import Column, DateTime, Float, Index, Integer, MetaData, Table, Text, inspect, text


metadata = MetaData()

news_articles = Table(
    "news_articles",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("title", Text),
    Column("content", Text),
    Column("url", Text),
    Column("source", Text),
    Column("state", Text),
    Column("state_confidence", Float),
    Column("district", Text),
    Column("district_confidence", Float),
    Column("embedding", Text),
    Column("published_at", DateTime(timezone=True), nullable=True),
    Column("ingested_at", DateTime(timezone=True), nullable=True),
)

pipeline_status = Table(
    "pipeline_status",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("service", Text, nullable=False),
    Column("last_successful_run_at", Text, nullable=True),
    Column("last_inserted_article_count", Integer, nullable=False, default=0),
    Column("last_collected_count", Integer, nullable=False, default=0),
    Column("last_unique_count", Integer, nullable=False, default=0),
    Column("last_backfilled_count", Integer, nullable=False, default=0),
    Column("last_run_result", Text, nullable=True),
    Column("updated_at", Text, nullable=True),
)

issue_daily_history = Table(
    "issue_daily_history",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("count_key", Text, nullable=False),
    Column("date", Text, nullable=False),
    Column("state", Text, nullable=False),
    Column("district", Text, nullable=False),
    Column("issue", Text, nullable=False),
    Column("count", Integer, nullable=False),
)

Index("ix_news_articles_url", news_articles.c.url)
Index("ix_news_articles_source", news_articles.c.source)
Index("ix_news_articles_state", news_articles.c.state)
Index("ix_news_articles_district", news_articles.c.district)
Index("ix_news_articles_published_at", news_articles.c.published_at)
Index("ix_news_articles_state_district", news_articles.c.state, news_articles.c.district)
Index("ix_pipeline_status_service", pipeline_status.c.service, unique=True)
Index("ix_issue_daily_history_count_key", issue_daily_history.c.count_key, unique=True)
Index("ix_issue_daily_history_date", issue_daily_history.c.date)
Index("ix_issue_daily_history_state_district", issue_daily_history.c.state, issue_daily_history.c.district)
Index("ix_issue_daily_history_issue", issue_daily_history.c.issue)


def ensure_schema(engine):

    metadata.create_all(engine)

    inspector = inspect(engine)
    column_names = {column["name"] for column in inspector.get_columns("news_articles")}

    alter_statements = []

    if "state_confidence" not in column_names:
        alter_statements.append("ALTER TABLE news_articles ADD COLUMN state_confidence FLOAT")

    if "district_confidence" not in column_names:
        alter_statements.append("ALTER TABLE news_articles ADD COLUMN district_confidence FLOAT")

    if alter_statements:
        with engine.begin() as conn:
            for statement in alter_statements:
                conn.execute(text(statement))