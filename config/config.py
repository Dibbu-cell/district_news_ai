import os
import socket

from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))
DEFAULT_SQLITE_PATH = os.path.join(BASE_DIR, "data", "district_news.db")
DEFAULT_REPORT_PATH = os.path.join(BASE_DIR, "data", "reports", "daily_summary.json")
DEFAULT_POSTGRES_URL = "postgresql+psycopg2://postgres:postgres@localhost:5432/district_news_ai"
DEFAULT_NEO4J_URI = "bolt://localhost:7687"
DEFAULT_MONGODB_URI = "mongodb+srv://<username>:<password>@cluster0.von1va3.mongodb.net/?appName=Cluster0"
DEFAULT_SQLITE_URL = f"sqlite:///{DEFAULT_SQLITE_PATH}"
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "5"))
PIPELINE_SCHEDULE_HOUR = int(os.getenv("PIPELINE_SCHEDULE_HOUR", "6"))
PIPELINE_RUN_EVERY_MINUTES = int(os.getenv("PIPELINE_RUN_EVERY_MINUTES", "60"))
DAILY_REPORT_PATH = os.getenv("DAILY_REPORT_PATH", DEFAULT_REPORT_PATH)
ISSUE_BASELINE_RETENTION_DAYS = int(os.getenv("ISSUE_BASELINE_RETENTION_DAYS", "90"))
NEWSAPI_MAX_PAGES = int(os.getenv("NEWSAPI_MAX_PAGES", "5"))
MAX_STATE_QUERIES = int(os.getenv("MAX_STATE_QUERIES", "36"))
MAX_DISTRICT_QUERIES = int(os.getenv("MAX_DISTRICT_QUERIES", "700"))
GDELT_MAX_RECORDS = int(os.getenv("GDELT_MAX_RECORDS", "250"))
GOOGLE_STATE_QUERIES = int(os.getenv("GOOGLE_STATE_QUERIES", "20"))
GOOGLE_DISTRICT_QUERIES = int(os.getenv("GOOGLE_DISTRICT_QUERIES", "300"))
GOOGLE_LOOKBACK_DAYS = int(os.getenv("GOOGLE_LOOKBACK_DAYS", "1"))
GOOGLE_USE_DAY_SLICES = os.getenv("GOOGLE_USE_DAY_SLICES", "true").lower() in {"1", "true", "yes", "on"}
GOOGLE_DAY_SLICE_DAYS = int(os.getenv("GOOGLE_DAY_SLICE_DAYS", "1"))
GOOGLE_FETCH_ALL_DISTRICT_TARGETS = os.getenv("GOOGLE_FETCH_ALL_DISTRICT_TARGETS", "true").lower() in {"1", "true", "yes", "on"}
FOCUS_BACKFILL_MAX_DISTRICTS = int(os.getenv("FOCUS_BACKFILL_MAX_DISTRICTS", "180"))
FOCUS_BACKFILL_STATE_BATCH = int(os.getenv("FOCUS_BACKFILL_STATE_BATCH", "12"))
DAILY_FOCUS_DISTRICT_BATCH = int(os.getenv("DAILY_FOCUS_DISTRICT_BATCH", "40"))
DAILY_FOCUS_STATE_BATCH = int(os.getenv("DAILY_FOCUS_STATE_BATCH", "6"))
PRIORITY_DISTRICTS = [
	district.strip().lower()
	for district in os.getenv(
		"PRIORITY_DISTRICTS",
		"lucknow,kanpur,fatehpur,anantapur,hyderabad,guntur,varanasi,prayagraj",
	).split(",")
	if district.strip()
]
PRIORITY_STATES = [
	state.strip().lower()
	for state in os.getenv(
		"PRIORITY_STATES",
		"uttar pradesh,andhra pradesh,telangana,maharashtra,delhi",
	).split(",")
	if state.strip()
]
GDELT_STATE_QUERIES = int(os.getenv("GDELT_STATE_QUERIES", "30"))
GDELT_DISTRICT_QUERIES = int(os.getenv("GDELT_DISTRICT_QUERIES", "40"))
COLLECTOR_WORKERS = int(os.getenv("COLLECTOR_WORKERS", "3"))
REQUEST_WORKERS = int(os.getenv("REQUEST_WORKERS", "8"))
GDELT_WINDOW_HOURS = int(os.getenv("GDELT_WINDOW_HOURS", "6"))
PIPELINE_BATCH_SIZE = int(os.getenv("PIPELINE_BATCH_SIZE", "128"))
DB_WRITE_CHUNK_SIZE = int(os.getenv("DB_WRITE_CHUNK_SIZE", "1000"))
STATE_CONFIDENCE_THRESHOLD = float(os.getenv("STATE_CONFIDENCE_THRESHOLD", "0.45"))
DISTRICT_CONFIDENCE_THRESHOLD = float(os.getenv("DISTRICT_CONFIDENCE_THRESHOLD", "0.45"))
SQLITE_MIGRATION_URL = os.getenv("SQLITE_MIGRATION_URL", DEFAULT_SQLITE_URL)

_configured_backend = os.getenv("DB_BACKEND")
_configured_mongodb_uri = os.getenv("MONGODB_URI")


def _neo4j_reachable(host: str = "localhost", port: int = 7687, timeout: float = 1.5) -> bool:
    """Return True if the Neo4j bolt port is open (Docker Desktop is running)."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


if _configured_backend:
    DB_BACKEND = _configured_backend.strip().lower()
elif _configured_mongodb_uri:
	DB_BACKEND = "mongodb"
elif _neo4j_reachable():
    # Docker Desktop is up — Neo4j container is running
    DB_BACKEND = "neo4j"
elif os.path.exists(DEFAULT_SQLITE_PATH):
    # Docker not available, fall back to local SQLite
    DB_BACKEND = "sqlite"
else:
    DB_BACKEND = "neo4j"

if DB_BACKEND == "sqlite":
	DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_SQLITE_URL)
else:
	DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_POSTGRES_URL)

NEO4J_URI = os.getenv("NEO4J_URI", DEFAULT_NEO4J_URI)
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")
MONGODB_URI = os.getenv("MONGODB_URI", DEFAULT_MONGODB_URI)
MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME", "district_news_ai")

NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")