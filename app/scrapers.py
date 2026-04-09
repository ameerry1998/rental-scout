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

    # Extract contact info — emails and phone numbers from description
    contact_parts = []
    emails = re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', description)
    phones = re.findall(r'(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}', description)
    contact_parts.extend(emails)
    contact_parts.extend(phones)
    # Also check for reply link on CL
    reply_link = soup.select_one("a.reply-button, a[href*='reply']")
    contact_info = ", ".join(contact_parts) if contact_parts else None

    # Combine description with attributes for richer context
    full_description = description
    if attrs_text:
        full_description += "\n\nAttributes: " + " | ".join(attrs_text)

    return ScraperResult(
        source="craigslist",
        contact_info=contact_info,
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
    # crawlerbros/zillow-scraper output shape
    # Skip building-level listings — they have multiple units with different prices/dates
    if item.get("propertyType") == "BUILDING":
        return None

    zpid = item.get("zpid") or item.get("id") or ""
    price = _safe_int(item.get("price") or item.get("unformattedPrice"))
    address = item.get("address") or ""
    city = item.get("city") or ""
    neighborhood = city

    return ScraperResult(
        source="zillow",
        source_id=str(zpid) or _hash_id("zillow", address),
        url=item.get("url") or item.get("detailUrl", ""),
        title=item.get("name") or item.get("title") or address,
        price=price,
        bedrooms=_safe_float(item.get("beds") or item.get("bedrooms")),
        bathrooms=_safe_float(item.get("baths") or item.get("bathrooms")),
        sqft=_safe_int(item.get("sqft") or item.get("livingArea")),
        address=address,
        neighborhood=neighborhood,
        latitude=_safe_float(item.get("latitude")),
        longitude=_safe_float(item.get("longitude")),
        description=item.get("description") or "",
        images=item.get("photos", []) or item.get("images", []) or [],
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
    # crawlerbros/realtor-scraper output shape
    prop_id = item.get("id") or item.get("listingId") or ""
    addr = item.get("address", {}) or {}
    coords = item.get("coordinates", {}) or {}

    full_address = " ".join(filter(None, [
        addr.get("street", ""),
        addr.get("city", ""),
        addr.get("state", ""),
        addr.get("postalCode", ""),
    ])) if isinstance(addr, dict) else str(addr)

    return ScraperResult(
        source="realtor",
        source_id=str(prop_id) or _hash_id("realtor", full_address),
        url=item.get("url", ""),
        title=item.get("name") or full_address,
        price=_safe_int(item.get("listPrice") or item.get("price")),
        bedrooms=_safe_float(item.get("beds")),
        bathrooms=_safe_float(item.get("baths") or item.get("bathsFull")),
        sqft=_safe_int(item.get("sqft")),
        address=full_address,
        neighborhood=addr.get("city", "") if isinstance(addr, dict) else "",
        latitude=_safe_float(coords.get("lat")),
        longitude=_safe_float(coords.get("lng")),
        description=item.get("description") or "",
        images=[p if isinstance(p, str) else p.get("href", "") for p in (item.get("photos", []) or [])[:5]],
        raw_data=item,
    )


def _transform_facebook(item: dict) -> ScraperResult | None:
    # apify/facebook-marketplace-scraper output shape
    post_id = item.get("id") or ""
    price_data = item.get("listing_price", {}) or {}
    price = _safe_int(price_data.get("amount") or price_data.get("formatted_amount", "").replace("$", "").replace(",", ""))

    location = item.get("location", {}) or {}
    reverse = location.get("reverse_geocode", {}) or {}
    city = reverse.get("city", "")
    state = reverse.get("state", "")
    city_page = reverse.get("city_page", {}) or {}
    neighborhood = city_page.get("display_name", "") or f"{city}, {state}"

    title = item.get("marketplace_listing_title") or item.get("custom_title") or ""
    photo = item.get("primary_listing_photo", {}) or {}

    return ScraperResult(
        source="facebook",
        source_id=str(post_id) or _hash_id("facebook", title),
        url=item.get("listingUrl") or item.get("url", ""),
        title=title,
        price=price,
        neighborhood=neighborhood,
        description=title,  # FB listings have minimal description
        images=[photo.get("photo_image_url", "")] if photo.get("photo_image_url") else [],
        raw_data=item,
    )


def enrich_zillow_details(results: list[ScraperResult]) -> list[ScraperResult]:
    """Fetch full listing details for Zillow listings via maxcopell/zillow-detail-scraper.
    Only called for listings that passed the pre-filter. ~$0.01 per listing."""
    urls = [r.url for r in results if r.url and "zillow.com" in r.url]
    if not urls:
        return results

    log.info(f"Zillow: fetching full details for {len(urls)} filtered listings")
    client = ApifyClient(config.APIFY_API_TOKEN)

    try:
        run = client.actor("maxcopell/zillow-detail-scraper").call(
            run_input={"startUrls": [{"url": u} for u in urls], "maxItems": len(urls)},
            timeout_secs=300,
        )
        detail_items = client.dataset(run["defaultDatasetId"]).list_items().items
        log.info(f"Zillow: got {len(detail_items)} detail pages")

        # Build lookup by address (zpid isn't always in detail output)
        details_by_address = {}
        for item in detail_items:
            addr = item.get("streetAddress") or item.get("address", {}).get("streetAddress", "")
            if addr:
                details_by_address[addr.lower().strip()] = item

        for result in results:
            addr_key = (result.address or "").lower().strip()
            detail = details_by_address.get(addr_key)
            if not detail:
                # Try matching by URL
                for item in detail_items:
                    if result.url and result.url in str(item.get("url", "")):
                        detail = item
                        break
            if not detail:
                continue

            desc = detail.get("description") or ""
            if desc:
                result.description = desc

            # Contact info
            broker = detail.get("brokerName") or detail.get("buildingName") or ""
            phone = detail.get("phone") or detail.get("brokerPhone") or ""
            if broker or phone:
                result.contact_info = ", ".join(filter(None, [broker, phone]))

            # Better photos
            photos = detail.get("photos") or detail.get("responsivePhotos") or []
            if photos and isinstance(photos[0], dict):
                result.images = [p.get("url") or p.get("mixedSources", {}).get("jpeg", [{}])[0].get("url", "") for p in photos[:6]]
            elif photos and isinstance(photos[0], str):
                result.images = photos[:6]

        log.info(f"Zillow: enriched {len(details_by_address)} listings with full descriptions")
    except Exception as e:
        log.warning(f"Zillow detail enrichment failed: {e}")

    return results


def enrich_facebook_details(results: list[ScraperResult]) -> list[ScraperResult]:
    """Fetch full listing details for Facebook listings via Apify detail scrape.
    Only called for listings that passed the pre-filter (~16 out of 200).
    Costs ~$0.005 per listing detail."""
    urls = [r.url for r in results if r.url]
    if not urls:
        return results

    log.info(f"Facebook: fetching full details for {len(urls)} filtered listings (~${len(urls) * 0.005:.2f})")
    client = ApifyClient(config.APIFY_API_TOKEN)

    try:
        run = client.actor(config.FACEBOOK_ACTOR).call(
            run_input={
                "startUrls": [{"url": u} for u in urls],
                "maxItems": len(urls),
            },
            timeout_secs=300,
        )
        detail_items = client.dataset(run["defaultDatasetId"]).list_items().items
        log.info(f"Facebook: got {len(detail_items)} detail pages")

        # Build lookup by listing ID
        details_by_id = {}
        for item in detail_items:
            lid = item.get("id", "")
            if lid:
                details_by_id[lid] = item

        # Enrich each result with the full detail data
        for result in results:
            detail = details_by_id.get(result.source_id)
            if not detail:
                continue

            # Full description text
            desc_data = detail.get("description", {}) or {}
            if isinstance(desc_data, dict):
                desc_text = desc_data.get("text", "")
            else:
                desc_text = str(desc_data)
            if desc_text:
                result.description = desc_text

            # Better title
            result.title = detail.get("listingTitle") or result.title

            # Location
            location = detail.get("location", {}) or {}
            reverse = location.get("reverse_geocode", {}) or {}
            city = reverse.get("city", "")
            if city:
                result.neighborhood = f"{city}, {reverse.get('state', '')}"

            # Photos
            photos = detail.get("listingPhotos") or []
            result.images = [
                p.get("image", {}).get("uri", "")
                for p in photos if p.get("image", {}).get("uri")
            ][:6]

            # Details (beds, baths, etc. from the detail page)
            details_list = detail.get("details") or []
            if details_list:
                result.description += "\n\nDetails: " + " | ".join(str(d) for d in details_list)

        log.info(f"Facebook: enriched {len(details_by_id)} listings with full descriptions")
    except Exception as e:
        log.warning(f"Facebook detail enrichment failed: {e}")

    return results


def _transform_rent(item: dict) -> ScraperResult | None:
    # benthepythondev/rent-com-scraper output shape
    prop_id = item.get("id") or item.get("propertyId") or ""
    return ScraperResult(
        source="rent",
        source_id=str(prop_id) or _hash_id("rent", item.get("property_name", "")),
        url=item.get("url") or "",
        title=item.get("property_name") or item.get("name") or item.get("title", ""),
        price=_safe_int(item.get("price") or item.get("price_text", "").replace("$", "").replace(",", "")),
        bedrooms=_safe_float(item.get("bedrooms") or item.get("beds")),
        bathrooms=_safe_float(item.get("bathrooms") or item.get("baths")),
        sqft=_safe_int(item.get("sqft")),
        address=item.get("address") or "",
        neighborhood=item.get("neighborhood") or "",
        latitude=_safe_float(item.get("latitude")),
        longitude=_safe_float(item.get("longitude")),
        description=item.get("description") or item.get("availability") or "",
        images=item.get("photos", []) or item.get("images", []) or [],
        raw_data=item,
        contact_info=item.get("phone") or "",
    )


# ---------------------------------------------------------------------------
# Apify actor input builders
# ---------------------------------------------------------------------------

def _zillow_input() -> dict:
    return {
        "startUrls": [{"url": f"https://www.zillow.com/cambridge-ma/rentals/{config.BEDROOMS}-_beds/"}],
        "maxItems": 200,
    }


def _apartments_input() -> dict:
    return {
        "providers": ["zumper", "apartments"],
        "location": "Cambridge, MA",
        "listingType": "rent",
        "maxItems": 200,
    }


def _realtor_input() -> dict:
    return {
        "startUrls": [{"url": f"https://www.realtor.com/apartments/Cambridge_MA/beds-{config.BEDROOMS}/price-na-{config.MAX_PRICE}"}],
        "maxItems": 200,
    }


def _facebook_input() -> dict:
    return {
        "startUrls": [{"url": f"https://www.facebook.com/marketplace/boston/propertyrentals?minPrice=0&maxPrice={config.MAX_PRICE}"}],
        "maxItems": 100,
    }


def _rent_input() -> dict:
    return {
        "location": "Cambridge, MA",
        "maxPrice": config.MAX_PRICE,
        "minBeds": config.BEDROOMS,
        "maxBeds": config.BEDROOMS,
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


APIFY_SCRAPERS: dict[str, ApifyScraperDef] = {
    "zillow": ApifyScraperDef("zillow", config.ZILLOW_ACTOR, config.ENABLE_ZILLOW, _zillow_input, _transform_zillow),
    "apartments": ApifyScraperDef("apartments", config.APARTMENTS_ACTOR, config.ENABLE_APARTMENTS, _apartments_input, _transform_aggregator),
    "realtor": ApifyScraperDef("realtor", config.REALTOR_ACTOR, config.ENABLE_REALTOR, _realtor_input, _transform_realtor),
    "facebook": ApifyScraperDef("facebook", config.FACEBOOK_ACTOR, config.ENABLE_FACEBOOK, _facebook_input, _transform_facebook),
    "rent": ApifyScraperDef("rent", config.RENT_ACTOR, config.ENABLE_RENT, _rent_input, _transform_rent),
}

# Direct scrapers (like Craigslist) — these don't use Apify
DIRECT_SCRAPERS = ["craigslist", "bostonpads"]

# All available scraper names (for the dashboard + routes)
SCRAPER_NAMES: list[str] = DIRECT_SCRAPERS + list(APIFY_SCRAPERS.keys())


# ---------------------------------------------------------------------------
# BostonPads — Direct HTTP scraper
# ---------------------------------------------------------------------------

def _scrape_bostonpads(known_ids: set[str] | None = None) -> list[ScraperResult]:
    """Scrape BostonPads Cambridge 1BR listings directly."""
    known_ids = known_ids or set()
    search_url = f"https://bostonpads.com/cambridge-ma-apartments/?beds={config.BEDROOMS}&maxprice={config.MAX_PRICE}"

    log.info(f"BostonPads: fetching {search_url}")
    r = httpx.get(search_url, follow_redirects=True, headers=HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    cards = soup.select(".bpo-listing-block-outer")
    log.info(f"BostonPads: found {len(cards)} listing cards")

    results: list[ScraperResult] = []
    for card in cards:
        try:
            link = card.select_one("a[href*='cambridge-ma-apartments/cambridge-']")
            if not link:
                continue

            url = link.get("href", "")
            id_match = re.search(r"cambridge-(\d+)", url)
            listing_id = id_match.group(1) if id_match else _hash_id("bostonpads", url)

            if listing_id in known_ids:
                continue

            text = card.get_text(" | ", strip=True)

            price_m = re.search(r"\$([\d,]+)", text)
            avail_m = re.search(r"Available:?\s*([\d-]+)", text)
            beds_m = re.search(r"(\d+)\s*Beds?", text)
            baths_m = re.search(r"(\d+)\s*Baths?", text)
            loc_m = re.search(r"at\s+(.+?),\s*(Cambridge|Somerville)", text)

            neighborhood = loc_m.group(1).strip() + ", " + loc_m.group(2) if loc_m else "Cambridge"

            results.append(ScraperResult(
                source="bostonpads",
                source_id=listing_id,
                url=url,
                title=f"{neighborhood} - {beds_m.group(1) if beds_m else '1'}BR" if loc_m else f"Cambridge listing {listing_id}",
                price=_safe_int(price_m.group(1).replace(",", "")) if price_m else None,
                bedrooms=_safe_float(beds_m.group(1)) if beds_m else None,
                bathrooms=_safe_float(baths_m.group(1)) if baths_m else None,
                neighborhood=neighborhood,
                description=text[:1000],
                raw_data={"url": url, "card_text": text[:500]},
            ))
        except Exception as e:
            log.warning(f"  BostonPads card parse error: {e}")

    # Fetch detail pages for contact info (first 15 only, with short timeouts)
    log.info(f"BostonPads: fetching details for {min(len(results), 15)} listings")
    for i, result in enumerate(results[:15]):
        try:
            detail_r = httpx.get(result.url, follow_redirects=True, headers=HEADERS, timeout=10)
            if detail_r.status_code != 200:
                continue
            detail_soup = BeautifulSoup(detail_r.text, "html.parser")

            desc = detail_soup.select_one("[class*=description], .bpo-listing-details-desc")
            if desc:
                result.description = desc.get_text(strip=True)[:2000]

            contact = detail_soup.select_one("[class*=contact], [class*=agent]")
            if contact:
                result.contact_info = contact.get_text(" | ", strip=True)[:200]

            imgs = detail_soup.select("img[src*='bostonpads.com/media']")
            result.images = [img["src"] for img in imgs[:5]]
        except Exception as e:
            log.warning(f"  BostonPads detail {i} failed: {e}")
        time.sleep(0.3)

    log.info(f"BostonPads: got {len(results)} listings")
    return results


def run_single_scraper(source: str, known_ids: set[str] | None = None) -> list[ScraperResult]:
    """Run one platform's scraper and return only NEW results."""
    known_ids = known_ids or set()

    if source == "craigslist":
        if not config.ENABLE_CRAIGSLIST:
            log.info("Craigslist is disabled")
            return []
        return _scrape_craigslist(known_ids=known_ids)

    if source == "bostonpads":
        return _scrape_bostonpads(known_ids=known_ids)

    scraper = APIFY_SCRAPERS.get(source)
    if not scraper:
        raise ValueError(f"Unknown source: {source}")
    if not scraper.enabled:
        log.info(f"{source} is disabled")
        return []

    client = ApifyClient(config.APIFY_API_TOKEN)

    # Pull from the most recent successful run's dataset.
    # NEVER starts a new Apify run — use "New Scrape" for that.
    # NOTE: stats.itemCount can be 0 even when the dataset has data (Apify outage bug).
    # So we check the actual dataset contents instead of trusting the stats.
    dataset_id = None
    try:
        recent_runs = client.actor(scraper.actor_id).runs().list(limit=10).items
        log.info(f"  {source}: checking {len(recent_runs)} recent runs")
        for recent in recent_runs:
            status = recent.get("status")
            if status not in ("SUCCEEDED", "TIMED-OUT"):
                continue
            ds_id = recent.get("defaultDatasetId")
            if not ds_id:
                continue
            # Actually check if the dataset has items
            sample = client.dataset(ds_id).list_items(limit=1).items
            if sample:
                dataset_id = ds_id
                log.info(f"  {source}: found dataset {ds_id[:12]} with data, importing")
                break
            else:
                log.info(f"    run {recent.get('id','?')[:12]}: dataset empty")
    except Exception as e:
        log.warning(f"  {source}: couldn't check recent runs: {e}")

    if not dataset_id:
        log.info(f"  {source}: no existing runs with data found. Use 'New Scrape' to trigger one.")
        return []

    dataset_items = client.dataset(dataset_id).list_items().items

    log.info(f"  {source}: got {len(dataset_items)} raw items")
    results: list[ScraperResult] = []
    for item in dataset_items:
        try:
            result = scraper.transform(item)
            if not result:
                continue
            if result.source_id in known_ids:
                continue
            results.append(result)
        except Exception as e:
            log.warning(f"  {source} transform error: {e}")

    log.info(f"  {source}: {len(results)} new, {len(dataset_items) - len(results)} skipped")
    return results


def trigger_new_scrape(source: str) -> None:
    """Explicitly trigger a NEW Apify actor run. Does NOT wait for results.
    Results get picked up on the next 'Run' click via run recovery."""
    scraper = APIFY_SCRAPERS.get(source)
    if not scraper:
        log.warning(f"No Apify scraper for {source}")
        return
    if not scraper.enabled:
        log.info(f"{source} is disabled")
        return

    client = ApifyClient(config.APIFY_API_TOKEN)
    actor_input = scraper.build_input()
    log.info(f"  {source}: triggering new Apify run (actor: {scraper.actor_id}) — NOT waiting for results")
    client.actor(scraper.actor_id).start(run_input=actor_input)
    log.info(f"  {source}: run started. Click 'Run' again in a few minutes to import results.")
