"""
Apify-based scrapers for all rental platforms.

Each scraper defines:
  - actor_id: the Apify actor to run
  - build_input(): returns the actor input dict
  - transform(item): normalizes one actor result → dict ready for Listing model

Actor IDs and input schemas change over time. If a scraper fails, check the
Apify Store for the current actor version and adjust the input/transform.
"""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from typing import Any, Callable

from apify_client import ApifyClient

from app import config

log = logging.getLogger(__name__)


@dataclass
class ScraperResult:
    source: str
    source_id: str
    url: str | None = None
    title: str | None = None
    price: int | None = None
    bedrooms: float | None = None
    bathrooms: float | None = None
    sqft: int | None = None
    address: str | None = None
    neighborhood: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    description: str | None = None
    images: list[str] = field(default_factory=list)
    raw_data: dict = field(default_factory=dict)
    contact_info: str | None = None


def _safe_int(val: Any) -> int | None:
    if val is None:
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def _safe_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _hash_id(source: str, *parts: str) -> str:
    raw = f"{source}:{'|'.join(str(p) for p in parts)}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Transform functions — one per platform
# Each takes a raw Apify dataset item and returns a ScraperResult
# ---------------------------------------------------------------------------

def _transform_craigslist(item: dict) -> ScraperResult | None:
    title = item.get("title") or item.get("name", "")
    price = _safe_int(
        item.get("price", "").replace("$", "").replace(",", "")
        if isinstance(item.get("price"), str) else item.get("price")
    )
    desc = item.get("description") or item.get("body") or ""
    url = item.get("url") or item.get("link", "")
    posting_id = item.get("postingId") or item.get("id") or _hash_id("craigslist", url, title)

    beds_raw = item.get("bedrooms") or item.get("housing", "")
    beds = _safe_float(beds_raw)

    return ScraperResult(
        source="craigslist",
        source_id=str(posting_id),
        url=url,
        title=title,
        price=price,
        bedrooms=beds,
        bathrooms=_safe_float(item.get("bathrooms")),
        sqft=_safe_int(item.get("sqft") or item.get("area")),
        address=item.get("address") or item.get("location", ""),
        neighborhood=item.get("neighborhood") or item.get("hood", ""),
        latitude=_safe_float(item.get("latitude") or item.get("lat")),
        longitude=_safe_float(item.get("longitude") or item.get("lng")),
        description=desc,
        images=item.get("images", []) or [],
        raw_data=item,
    )


def _transform_zillow(item: dict) -> ScraperResult | None:
    zpid = item.get("zpid") or item.get("id") or ""
    price = _safe_int(item.get("price") or item.get("unformattedPrice"))
    address_data = item.get("address", {})
    if isinstance(address_data, dict):
        address = address_data.get("streetAddress", "")
        neighborhood = address_data.get("neighborhood", "") or address_data.get("city", "")
    else:
        address = str(address_data)
        neighborhood = ""

    return ScraperResult(
        source="zillow",
        source_id=str(zpid),
        url=item.get("detailUrl") or item.get("url", ""),
        title=item.get("title") or item.get("statusText") or address,
        price=price,
        bedrooms=_safe_float(item.get("bedrooms") or item.get("beds")),
        bathrooms=_safe_float(item.get("bathrooms") or item.get("baths")),
        sqft=_safe_int(item.get("livingArea") or item.get("area")),
        address=address,
        neighborhood=neighborhood,
        latitude=_safe_float(item.get("latitude") or item.get("lat")),
        longitude=_safe_float(item.get("longitude") or item.get("lng")),
        description=item.get("description") or "",
        images=item.get("images", []) or item.get("photos", []) or [],
        raw_data=item,
    )


def _transform_apartments(item: dict) -> ScraperResult | None:
    listing_id = item.get("id") or item.get("listingId") or ""
    name = item.get("name") or item.get("propertyName") or ""
    price_text = item.get("price") or item.get("rent") or ""
    price = _safe_int(
        str(price_text).replace("$", "").replace(",", "").split("-")[0].split("/")[0].strip()
    )

    return ScraperResult(
        source="apartments",
        source_id=str(listing_id) or _hash_id("apartments", name, str(price)),
        url=item.get("url") or item.get("link", ""),
        title=name,
        price=price,
        bedrooms=_safe_float(item.get("bedrooms") or item.get("beds")),
        bathrooms=_safe_float(item.get("bathrooms") or item.get("baths")),
        sqft=_safe_int(item.get("sqft") or item.get("squareFeet")),
        address=item.get("address") or item.get("streetAddress", ""),
        neighborhood=item.get("neighborhood") or "",
        latitude=_safe_float(item.get("latitude")),
        longitude=_safe_float(item.get("longitude")),
        description=item.get("description") or "",
        images=item.get("images", []) or item.get("photos", []) or [],
        raw_data=item,
        contact_info=item.get("phone") or item.get("contactPhone", ""),
    )


def _transform_realtor(item: dict) -> ScraperResult | None:
    prop_id = item.get("property_id") or item.get("listing_id") or ""
    location = item.get("location", {}) or {}
    address_data = location.get("address", {}) or {}
    coord = location.get("coordinate", {}) or {}

    desc = item.get("description", {})
    if isinstance(desc, dict):
        desc_text = desc.get("text", "")
        beds = _safe_float(desc.get("beds"))
        baths = _safe_float(desc.get("baths"))
        sqft = _safe_int(desc.get("sqft"))
    else:
        desc_text = str(desc) if desc else ""
        beds = _safe_float(item.get("beds"))
        baths = _safe_float(item.get("baths"))
        sqft = _safe_int(item.get("sqft"))

    full_address = " ".join(filter(None, [
        address_data.get("line", ""),
        address_data.get("city", ""),
        address_data.get("state_code", ""),
        address_data.get("postal_code", ""),
    ]))

    return ScraperResult(
        source="realtor",
        source_id=str(prop_id) or _hash_id("realtor", full_address),
        url=item.get("href") or item.get("url", ""),
        title=item.get("title") or full_address,
        price=_safe_int(item.get("list_price") or item.get("price")),
        bedrooms=beds,
        bathrooms=baths,
        sqft=sqft,
        address=full_address,
        neighborhood=address_data.get("neighborhood_name", ""),
        latitude=_safe_float(coord.get("lat")),
        longitude=_safe_float(coord.get("lon")),
        description=desc_text,
        images=[p.get("href", "") for p in (item.get("photos", []) or [])[:5]],
        raw_data=item,
    )


def _transform_redfin(item: dict) -> ScraperResult | None:
    prop_id = item.get("propertyId") or item.get("mlsId") or item.get("listingId") or ""
    return ScraperResult(
        source="redfin",
        source_id=str(prop_id) or _hash_id("redfin", item.get("address", "")),
        url=item.get("url") or "",
        title=item.get("title") or item.get("address", ""),
        price=_safe_int(item.get("price") or item.get("listPrice")),
        bedrooms=_safe_float(item.get("beds") or item.get("bedrooms")),
        bathrooms=_safe_float(item.get("baths") or item.get("bathrooms")),
        sqft=_safe_int(item.get("sqFt") or item.get("sqft")),
        address=item.get("address") or "",
        neighborhood=item.get("neighborhood") or "",
        latitude=_safe_float(item.get("latitude") or item.get("lat")),
        longitude=_safe_float(item.get("longitude") or item.get("lng")),
        description=item.get("description") or "",
        images=item.get("images", []) or item.get("photos", []) or [],
        raw_data=item,
    )


def _transform_facebook(item: dict) -> ScraperResult | None:
    post_id = item.get("id") or item.get("postId") or ""
    price_raw = item.get("price") or ""
    price = _safe_int(
        str(price_raw).replace("$", "").replace(",", "").replace("/mo", "").strip()
    )

    return ScraperResult(
        source="facebook",
        source_id=str(post_id) or _hash_id("facebook", item.get("title", "")),
        url=item.get("url") or item.get("link", ""),
        title=item.get("title") or item.get("name", ""),
        price=price,
        bedrooms=_safe_float(item.get("bedrooms")),
        bathrooms=_safe_float(item.get("bathrooms")),
        sqft=_safe_int(item.get("squareFeet")),
        address=item.get("location") or item.get("address", ""),
        neighborhood="",
        latitude=_safe_float(item.get("latitude")),
        longitude=_safe_float(item.get("longitude")),
        description=item.get("description") or item.get("body") or "",
        images=item.get("images", []) or [],
        raw_data=item,
    )


def _transform_rent(item: dict) -> ScraperResult | None:
    prop_id = item.get("id") or item.get("propertyId") or ""
    return ScraperResult(
        source="rent",
        source_id=str(prop_id) or _hash_id("rent", item.get("name", "")),
        url=item.get("url") or "",
        title=item.get("name") or item.get("title", ""),
        price=_safe_int(item.get("price") or item.get("rent")),
        bedrooms=_safe_float(item.get("bedrooms") or item.get("beds")),
        bathrooms=_safe_float(item.get("bathrooms") or item.get("baths")),
        sqft=_safe_int(item.get("sqft")),
        address=item.get("address") or "",
        neighborhood=item.get("neighborhood") or "",
        latitude=_safe_float(item.get("latitude")),
        longitude=_safe_float(item.get("longitude")),
        description=item.get("description") or "",
        images=item.get("images", []) or item.get("photos", []) or [],
        raw_data=item,
    )


# ---------------------------------------------------------------------------
# Scraper registry
# ---------------------------------------------------------------------------

@dataclass
class ScraperDef:
    name: str
    actor_id: str
    enabled: bool
    build_input: Callable[[], dict]
    transform: Callable[[dict], ScraperResult | None]


def _cl_input() -> dict:
    return {
        "startUrls": [
            f"https://boston.craigslist.org/search/gbs/apa"
            f"?min_price=0&max_price={config.MAX_PRICE}"
            f"&min_bedrooms={config.BEDROOMS}&max_bedrooms={config.BEDROOMS}"
        ],
        "maxItems": 200,
    }


def _zillow_input() -> dict:
    return {
        "searchUrls": [
            f"https://www.zillow.com/cambridge-ma/rentals/"
            f"{config.BEDROOMS}-_beds/"
            f"?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C"
            f"%22mapBounds%22%3A%7B%7D%2C"
            f"%22filterState%22%3A%7B%22price%22%3A%7B%22max%22%3A{config.MAX_PRICE}%7D%2C"
            f"%22beds%22%3A%7B%22min%22%3A{config.BEDROOMS}%2C%22max%22%3A{config.BEDROOMS}%7D%2C"
            f"%22fr%22%3A%7B%22value%22%3Atrue%7D%2C%22fsba%22%3A%7B%22value%22%3Afalse%7D%2C"
            f"%22fsbo%22%3A%7B%22value%22%3Afalse%7D%2C%22nc%22%3A%7B%22value%22%3Afalse%7D%2C"
            f"%22cmsn%22%3A%7B%22value%22%3Afalse%7D%2C%22auc%22%3A%7B%22value%22%3Afalse%7D%2C"
            f"%22fore%22%3A%7B%22value%22%3Afalse%7D%7D%7D"
        ],
        "maxItems": 200,
    }


def _apartments_input() -> dict:
    # tri_angle/real-estate-aggregator covers Apartments.com, Zumper, and more
    return {
        "sources": ["apartments.com", "zumper"],
        "location": "Cambridge, MA",
        "listingType": "rent",
        "minBeds": config.BEDROOMS,
        "maxBeds": config.BEDROOMS,
        "maxPrice": config.MAX_PRICE,
        "maxItems": 200,
    }


def _realtor_input() -> dict:
    # crawlerbros/realtor-scraper — pass Realtor.com search URL
    return {
        "startUrls": [
            f"https://www.realtor.com/apartments/Cambridge_MA"
            f"/beds-{config.BEDROOMS}"
            f"/price-na-{config.MAX_PRICE}"
            f"/type-apartment"
        ],
        "maxItems": 200,
    }


def _redfin_input() -> dict:
    # tri_angle/redfin-search
    return {
        "searchUrl": (
            "https://www.redfin.com/city/3312/MA/Cambridge/apartments-for-rent"
            f"/filter/max-price={config.MAX_PRICE},min-beds={config.BEDROOMS},max-beds={config.BEDROOMS}"
        ),
        "maxItems": 200,
    }


def _facebook_input() -> dict:
    return {
        "searchQuery": f"{config.BEDROOMS} bedroom apartment Cambridge MA",
        "location": "Cambridge, Massachusetts",
        "category": "propertyrentals",
        "maxPrice": config.MAX_PRICE,
        "maxItems": 100,
    }


def _rent_input() -> dict:
    return {
        "startUrls": [
            f"https://www.rent.com/massachusetts/cambridge-apartments"
            f"/beds-{config.BEDROOMS}"
            f"/prices-less-than-{config.MAX_PRICE}"
        ],
        "maxItems": 200,
    }


SCRAPERS: list[ScraperDef] = [
    ScraperDef("craigslist", config.CRAIGSLIST_ACTOR, config.ENABLE_CRAIGSLIST, _cl_input, _transform_craigslist),
    ScraperDef("zillow", config.ZILLOW_ACTOR, config.ENABLE_ZILLOW, _zillow_input, _transform_zillow),
    ScraperDef("apartments", config.APARTMENTS_ACTOR, config.ENABLE_APARTMENTS, _apartments_input, _transform_apartments),
    ScraperDef("realtor", config.REALTOR_ACTOR, config.ENABLE_REALTOR, _realtor_input, _transform_realtor),
    ScraperDef("redfin", config.REDFIN_ACTOR, config.ENABLE_REDFIN, _redfin_input, _transform_redfin),
    ScraperDef("facebook", config.FACEBOOK_ACTOR, config.ENABLE_FACEBOOK, _facebook_input, _transform_facebook),
    ScraperDef("rent", config.RENT_ACTOR, config.ENABLE_RENT, _rent_input, _transform_rent),
]


def run_all_scrapers() -> list[ScraperResult]:
    """Run every enabled Apify scraper and return normalized results."""
    client = ApifyClient(config.APIFY_API_TOKEN)
    all_results: list[ScraperResult] = []

    for scraper in SCRAPERS:
        if not scraper.enabled:
            log.info(f"Skipping disabled scraper: {scraper.name}")
            continue

        log.info(f"Running {scraper.name} (actor: {scraper.actor_id})")
        try:
            actor_input = scraper.build_input()
            run = client.actor(scraper.actor_id).call(run_input=actor_input)
            dataset_items = client.dataset(run["defaultDatasetId"]).list_items().items

            log.info(f"  {scraper.name}: got {len(dataset_items)} raw items")
            for item in dataset_items:
                try:
                    result = scraper.transform(item)
                    if result:
                        all_results.append(result)
                except Exception as e:
                    log.warning(f"  {scraper.name} transform error: {e}")
                    continue

        except Exception as e:
            log.error(f"  {scraper.name} actor failed: {e}")
            continue

    log.info(f"Total scraped results: {len(all_results)}")
    return all_results
