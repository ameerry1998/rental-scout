"""
Scrapers for rental listing platforms.

Craigslist: direct HTTP scraper (fast, free, gets full descriptions).
Everything else: Apify actors.

Actor IDs and input schemas change over time. If an Apify scraper fails,
check the Apify Store for the current actor version and adjust the input/transform.
"""
from __future__ import annotations

import hashlib
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable

import httpx
from bs4 import BeautifulSoup
from apify_client import ApifyClient

from app import config

log = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}


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
        return int(float(str(val).replace("$", "").replace(",", "").strip()))
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
# Craigslist — Direct HTTP scraper
# ---------------------------------------------------------------------------

def _scrape_craigslist(known_ids: set[str] | None = None) -> list[ScraperResult]:
    """Scrape Craigslist Boston apartments directly via HTTP.
    known_ids: source_ids already in the DB — skip detail page fetches for these."""
    known_ids = known_ids or set()
    search_url = (
        f"https://boston.craigslist.org/search/gbs/apa"
        f"?min_price=0&max_price={config.MAX_PRICE}"
        f"&min_bedrooms={config.BEDROOMS}&max_bedrooms={config.BEDROOMS}"
    )

    log.info(f"Craigslist: fetching search results from {search_url}")
    r = httpx.get(search_url, follow_redirects=True, headers=HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    items = soup.select("li.cl-static-search-result")
    log.info(f"Craigslist: found {len(items)} search results")

    # Parse search cards and pre-filter before fetching detail pages
    target_hoods_lower = [n.lower() for n in config.TARGET_NEIGHBORHOODS]
    listings: list[dict] = []
    skipped = 0

    for item in items:
        link = item.select_one("a")
        if not link:
            continue
        href = link.get("href", "")
        title = item.get("title", "") or (item.select_one(".title") or link).get_text(strip=True)
        price_el = item.select_one(".price")
        price_text = price_el.get_text(strip=True) if price_el else ""
        location_el = item.select_one(".location")
        location = location_el.get_text(strip=True) if location_el else ""

        post_id_match = re.search(r"/(\d{8,12})\.html", href)
        post_id = post_id_match.group(1) if post_id_match else _hash_id("craigslist", href)

        # --- Card-level filters (skip obvious mismatches) ---
        price = _safe_int(price_text)
        if price and price > config.MAX_PRICE:
            skipped += 1
            continue

        # Check location against target neighborhoods
        loc_lower = location.lower()
        title_lower = title.lower()
        location_match = (
            not location  # no location listed = don't filter out
            or any(hood in loc_lower or hood in title_lower for hood in target_hoods_lower)
        )
        if not location_match:
            skipped += 1
            continue

        listings.append({
            "post_id": post_id,
            "url": href,
            "title": title,
            "price": price_text,
            "location": location,
        })

    # Skip listings we already have in the DB
    new_listings = [l for l in listings if l["post_id"] not in known_ids]
    already_seen = len(listings) - len(new_listings)
    log.info(f"Craigslist: {len(listings)} passed card filter, {skipped} skipped, {already_seen} already in DB, {len(new_listings)} new to fetch")

    # Only fetch detail pages for genuinely new listings
    results: list[ScraperResult] = []
    batch_size = 5
    for batch_start in range(0, len(new_listings), batch_size):
        batch = new_listings[batch_start:batch_start + batch_size]
        for listing in batch:
            try:
                detail = _fetch_craigslist_detail(listing)
                if detail:
                    results.append(detail)
            except Exception as e:
                log.warning(f"  CL detail fetch failed for {listing['post_id']}: {e}")
                results.append(ScraperResult(
                    source="craigslist",
                    source_id=listing["post_id"],
                    url=listing["url"],
                    title=listing["title"],
                    price=_safe_int(listing["price"]),
                    neighborhood=listing["location"],
                    raw_data=listing,
                ))
        time.sleep(0.5)

    log.info(f"Craigslist: got {len(results)} listings with details")
    return results


def _fetch_craigslist_detail(listing: dict) -> ScraperResult | None:
    """Fetch a single Craigslist listing page for full description + attributes."""
    url = listing["url"]
    r = httpx.get(url, follow_redirects=True, headers=HEADERS, timeout=15)
    if r.status_code == 404:
        return None
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    # Description
    body = soup.select_one("#postingbody")
    if body:
        # Remove the QR code link text that CL injects
        for qr in body.select(".print-information"):
            qr.decompose()
        description = body.get_text(strip=True)
    else:
        description = ""

    # Attributes (beds, baths, available date, pets, laundry, etc.)
    attrs_text = []
    beds = None
    baths = None
    for group in soup.select(".attrgroup"):
        for span in group.select("span"):
            text = span.get_text(strip=True)
            attrs_text.append(text)
            # Parse "1BR / 1Ba"
            br_match = re.match(r"(\d+)BR", text)
            ba_match = re.search(r"(\d+)Ba", text)
            if br_match:
                beds = float(br_match.group(1))
            if ba_match:
                baths = float(ba_match.group(1))

    # Coordinates from map
    mapbox = soup.select_one("#map")
    lat = _safe_float(mapbox.get("data-latitude")) if mapbox else None
    lng = _safe_float(mapbox.get("data-longitude")) if mapbox else None

    # Images
    images = []
    for img in soup.select("#thumbs a"):
        href = img.get("href", "")
        if href:
            images.append(href)

    # Combine description with attributes for richer context
    full_description = description
    if attrs_text:
        full_description += "\n\nAttributes: " + " | ".join(attrs_text)

    return ScraperResult(
        source="craigslist",
        source_id=listing["post_id"],
        url=url,
        title=listing["title"],
        price=_safe_int(listing["price"]),
        bedrooms=beds,
        bathrooms=baths,
        address="",
        neighborhood=listing["location"],
        latitude=lat,
        longitude=lng,
        description=full_description,
        images=images[:6],
        raw_data={"listing": listing, "attrs": attrs_text},
    )


# ---------------------------------------------------------------------------
# Apify transform functions — one per platform
# ---------------------------------------------------------------------------

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


def _transform_aggregator(item: dict) -> ScraperResult | None:
    """Transform for tri_angle/real-estate-aggregator (covers Apartments.com, Zumper, etc.)."""
    prop_id = item.get("id") or item.get("listingId") or item.get("propertyId") or ""
    source_site = item.get("source", "apartments").lower()
    name = item.get("name") or item.get("propertyName") or item.get("title") or ""
    price = _safe_int(item.get("price") or item.get("rent"))

    return ScraperResult(
        source=f"apartments ({source_site})" if source_site else "apartments",
        source_id=str(prop_id) or _hash_id("apartments", name, str(price)),
        url=item.get("url") or item.get("link", ""),
        title=name,
        price=price,
        bedrooms=_safe_float(item.get("bedrooms") or item.get("beds")),
        bathrooms=_safe_float(item.get("bathrooms") or item.get("baths")),
        sqft=_safe_int(item.get("sqft") or item.get("squareFeet") or item.get("area")),
        address=item.get("address") or item.get("streetAddress", ""),
        neighborhood=item.get("neighborhood") or item.get("city", ""),
        latitude=_safe_float(item.get("latitude") or item.get("lat")),
        longitude=_safe_float(item.get("longitude") or item.get("lng")),
        description=item.get("description") or "",
        images=item.get("images", []) or item.get("photos", []) or [],
        raw_data=item,
        contact_info=item.get("phone") or item.get("contactPhone", ""),
    )


def _transform_realtor(item: dict) -> ScraperResult | None:
    prop_id = item.get("property_id") or item.get("listing_id") or item.get("id") or ""
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
        beds = _safe_float(item.get("beds") or item.get("bedrooms"))
        baths = _safe_float(item.get("baths") or item.get("bathrooms"))
        sqft = _safe_int(item.get("sqft"))

    full_address = " ".join(filter(None, [
        address_data.get("line", "") or item.get("address", ""),
        address_data.get("city", "") or item.get("city", ""),
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
        latitude=_safe_float(coord.get("lat") or item.get("latitude")),
        longitude=_safe_float(coord.get("lon") or item.get("longitude")),
        description=desc_text,
        images=[p.get("href", "") for p in (item.get("photos", []) or [])[:5]],
        raw_data=item,
    )


def _transform_redfin(item: dict) -> ScraperResult | None:
    prop_id = item.get("propertyId") or item.get("mlsId") or item.get("listingId") or item.get("id") or ""
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
# Apify actor input builders
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Scraper registry
# ---------------------------------------------------------------------------

@dataclass
class ApifyScraperDef:
    name: str
    actor_id: str
    enabled: bool
    build_input: Callable[[], dict]
    transform: Callable[[dict], ScraperResult | None]


APIFY_SCRAPERS: list[ApifyScraperDef] = [
    ApifyScraperDef("zillow", config.ZILLOW_ACTOR, config.ENABLE_ZILLOW, _zillow_input, _transform_zillow),
    ApifyScraperDef("apartments", config.APARTMENTS_ACTOR, config.ENABLE_APARTMENTS, _apartments_input, _transform_aggregator),
    ApifyScraperDef("realtor", config.REALTOR_ACTOR, config.ENABLE_REALTOR, _realtor_input, _transform_realtor),
    ApifyScraperDef("redfin", config.REDFIN_ACTOR, config.ENABLE_REDFIN, _redfin_input, _transform_redfin),
    ApifyScraperDef("facebook", config.FACEBOOK_ACTOR, config.ENABLE_FACEBOOK, _facebook_input, _transform_facebook),
    ApifyScraperDef("rent", config.RENT_ACTOR, config.ENABLE_RENT, _rent_input, _transform_rent),
]


def run_all_scrapers(known_ids_by_source: dict[str, set[str]] | None = None) -> list[ScraperResult]:
    """Run every enabled scraper and return only NEW normalized results.

    known_ids_by_source: {"craigslist": {"123", "456"}, "zillow": {"zpid1"}, ...}
    Listings with these source_ids are skipped entirely — no detail fetches, no transforms.
    """
    known = known_ids_by_source or {}
    all_results: list[ScraperResult] = []

    # 1. Craigslist — direct HTTP scraper (knows how to skip detail fetches)
    if config.ENABLE_CRAIGSLIST:
        try:
            cl_results = _scrape_craigslist(known_ids=known.get("craigslist", set()))
            all_results.extend(cl_results)
        except Exception as e:
            log.error(f"Craigslist scraper failed: {e}")
    else:
        log.info("Skipping disabled scraper: craigslist")

    # 2. Apify-based scrapers
    client = ApifyClient(config.APIFY_API_TOKEN)

    for scraper in APIFY_SCRAPERS:
        if not scraper.enabled:
            log.info(f"Skipping disabled scraper: {scraper.name}")
            continue

        scraper_known = known.get(scraper.name, set())
        log.info(f"Running {scraper.name} (actor: {scraper.actor_id})")
        try:
            actor_input = scraper.build_input()
            run = client.actor(scraper.actor_id).call(run_input=actor_input)
            dataset_items = client.dataset(run["defaultDatasetId"]).list_items().items

            log.info(f"  {scraper.name}: got {len(dataset_items)} raw items")
            new_count = 0
            for item in dataset_items:
                try:
                    result = scraper.transform(item)
                    if not result:
                        continue
                    # Skip if we already have this listing
                    if result.source_id in scraper_known:
                        continue
                    all_results.append(result)
                    new_count += 1
                except Exception as e:
                    log.warning(f"  {scraper.name} transform error: {e}")
                    continue
            log.info(f"  {scraper.name}: {new_count} new, {len(dataset_items) - new_count} already known")

        except Exception as e:
            log.error(f"  {scraper.name} actor failed: {e}")
            continue

    log.info(f"Total new scraped results: {len(all_results)}")
    return all_results
