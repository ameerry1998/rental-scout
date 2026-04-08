import os
from dotenv import load_dotenv

load_dotenv()


def _list(val: str) -> list[str]:
    return [v.strip() for v in val.split(",") if v.strip()]


def _bool(val: str) -> bool:
    return val.lower() in ("true", "1", "yes")


# --- Required ---
APIFY_API_TOKEN = os.environ["APIFY_API_TOKEN"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
CRON_SECRET = os.environ.get("CRON_SECRET", "change_me")

# --- Database ---
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./rental_scout.db")
# Railway Postgres URLs start with postgres:// but SQLAlchemy needs postgresql://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# --- Renter profile ---
RENTER_NAME = os.environ.get("RENTER_NAME", "Ameer")
RENTER_BIO = os.environ.get(
    "RENTER_BIO",
    "Software engineer, stable income, clean, quiet tenant",
)

# --- Search criteria ---
MAX_PRICE = int(os.environ.get("MAX_PRICE", "2800"))
BEDROOMS = int(os.environ.get("BEDROOMS", "1"))
TARGET_MOVE_IN = os.environ.get("TARGET_MOVE_IN", "2026-09-01")
ALT_MOVE_IN = os.environ.get("ALT_MOVE_IN", "2026-08-01")
SEARCH_AREA = os.environ.get("SEARCH_AREA", "Cambridge, MA")
TARGET_NEIGHBORHOODS = _list(
    os.environ.get(
        "TARGET_NEIGHBORHOODS",
        "Cambridge,Somerville,Allston,Brighton,Inman Square,Central Square,"
        "Harvard Square,Porter Square,Davis Square,Kendall Square,"
        "East Cambridge,East Somerville,Union Square",
    )
)

# --- Apify actor IDs (verified against Apify Store 2026-04) ---
CRAIGSLIST_ACTOR = os.environ.get("CRAIGSLIST_ACTOR", "easyapi/craigslist-search-results-scraper")
ZILLOW_ACTOR = os.environ.get("ZILLOW_ACTOR", "crawlerbros/zillow-scraper")
APARTMENTS_ACTOR = os.environ.get("APARTMENTS_ACTOR", "tri_angle/real-estate-aggregator")
REALTOR_ACTOR = os.environ.get("REALTOR_ACTOR", "crawlerbros/realtor-scraper")
FACEBOOK_ACTOR = os.environ.get("FACEBOOK_ACTOR", "webdatalabs/facebook-marketplace-deal-finder")
RENT_ACTOR = os.environ.get("RENT_ACTOR", "benthepythondev/rent-com-scraper")

# --- Enable/disable scrapers ---
ENABLE_CRAIGSLIST = _bool(os.environ.get("ENABLE_CRAIGSLIST", "true"))
ENABLE_ZILLOW = _bool(os.environ.get("ENABLE_ZILLOW", "true"))
ENABLE_APARTMENTS = _bool(os.environ.get("ENABLE_APARTMENTS", "true"))
ENABLE_REALTOR = _bool(os.environ.get("ENABLE_REALTOR", "true"))
ENABLE_FACEBOOK = _bool(os.environ.get("ENABLE_FACEBOOK", "true"))
ENABLE_RENT = _bool(os.environ.get("ENABLE_RENT", "true"))
