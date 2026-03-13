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

Index("ix_news_articles_url", news_articles.c.url)
Index("ix_news_articles_source", news_articles.c.source)
Index("ix_news_articles_state", news_articles.c.state)
Index("ix_news_articles_district", news_articles.c.district)
Index("ix_news_articles_published_at", news_articles.c.published_at)
Index("ix_news_articles_state_district", news_articles.c.state, news_articles.c.district)


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