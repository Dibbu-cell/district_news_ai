from sqlalchemy import create_engine

from config.config import DATABASE_URL


def create_app_engine(database_url):

    engine_kwargs = {
        "pool_pre_ping": True,
        "future": True,
    }

    if database_url.startswith("sqlite"):
        engine_kwargs["connect_args"] = {"check_same_thread": False}
    else:
        engine_kwargs["pool_recycle"] = 3600

    return create_engine(database_url, **engine_kwargs)


engine = create_app_engine(DATABASE_URL)


def get_connection():
    return engine.connect()