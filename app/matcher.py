"""
Claude-powered listing matcher and message drafter.

Uses Haiku for fast scoring of every listing, Sonnet for drafting outreach.
Scoring prompt is editable from the dashboard UI (stored in DB).
Injects recent user feedback as few-shot examples to improve matching over time.
"""
from __future__ import annotations

import json
import logging

import anthropic
from sqlalchemy.orm import Session

from app import config
from app.models import Listing, Setting

log = logging.getLogger(__name__)

client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)

DEFAULT_MATCH_PROMPT = f"""You are a rental listing analyst. You are scoring listings for {config.RENTER_NAME} who is looking for an apartment.

HARD REQUIREMENTS (a listing CANNOT score above 5 if any of these fail):
- Location must be in CAMBRIDGE, MA. This is non-negotiable. Cambridge is the priority.
- Move-in date must be September 1, 2026 OR August 1, 2026. No other dates.
- Must be {config.BEDROOMS} bedroom (not a studio, not a room share, not a sublet)
- Max rent: ${config.MAX_PRICE:,}/month

LOCATION SCORING TIERS:
- Cambridge (any part: Central Sq, Harvard Sq, Inman Sq, Kendall, Porter Sq, East Cambridge, Cambridgeport, Mid-Cambridge, etc.) = FULL POINTS
- Somerville (Davis Sq, Union Sq, Porter area, East Somerville) = ACCEPTABLE but cap at 7 max. It is nearby and fine, but Cambridge is strongly preferred.
- Allston/Brighton = cap at 5. Only if everything else is perfect.
- Anywhere else (Brookline, Medford, Waltham, Malden, Dorchester, etc.) = score 0-2 regardless of other factors.

AVAILABILITY SCORING:
- September 1, 2026 = perfect
- August 1, 2026 = also great
- "Available now" or any date before July 2026 = score 0-3, the timing is wrong
- Unknown/not stated = flag it but don't auto-reject, score based on other factors

YOUR TASK:
1. Extract the ACTUAL availability date from the description text. Look for: "available X", "move-in", "ready after", "lease starts", "Sept 1", "9/1", "avail sep", etc. The structured fields are often wrong — READ THE DESCRIPTION.
2. Determine the real neighborhood/city — not just what the listing says, check the address.
3. Score 0-10 strictly using the rules above.
4. Flag room shares, scams, or misleading listings.

Respond with ONLY valid JSON (no markdown fences, no explanation):
{{
  "score": <0-10>,
  "availability_date": "<YYYY-MM-DD or 'unknown'>",
  "availability_raw": "<exact text from listing about availability, or 'none found'>",
  "is_room_share": <true/false>,
  "neighborhood": "<specific neighborhood and city, e.g. 'Central Square, Cambridge'>",
  "match_reasons": ["<reason1>", "<reason2>"],
  "concerns": ["<concern1>", "<concern2>"],
  "summary": "<one-sentence summary>"
}}"""

DRAFT_SYSTEM = f"""You are drafting a rental inquiry message from {config.RENTER_NAME}.

RENTER CONTEXT:
- {config.RENTER_BIO}
- Looking for a {config.BEDROOMS}-bedroom in the {config.SEARCH_AREA} area
- Target move-in: {config.TARGET_MOVE_IN} (flexible to {config.ALT_MOVE_IN})

Write a concise, professional inquiry that:
- References 1-2 specific details from the listing (address, a feature, the neighborhood)
- Asks about availability for the target move-in date
- Is 3-4 sentences maximum
- Sounds natural and human — not like a form letter or AI-generated
- Does not oversell or sound desperate

Return ONLY the message text, no subject line or greeting prefix."""


def get_match_prompt(db: Session) -> str:
    """Get the scoring prompt — from DB if edited, otherwise the default."""
    row = db.query(Setting).filter_by(key="match_prompt").first()
    if row and row.value:
        return row.value
    return DEFAULT_MATCH_PROMPT


def save_match_prompt(db: Session, prompt: str) -> None:
    """Save an updated scoring prompt to the DB."""
    row = db.query(Setting).filter_by(key="match_prompt").first()
    if row:
        row.value = prompt
    else:
        db.add(Setting(key="match_prompt", value=prompt))
    db.commit()


def _get_feedback_examples(db: Session, limit: int = 10) -> str:
    reviewed = (
        db.query(Listing)
        .filter(Listing.feedback.isnot(None))
        .order_by(Listing.created_at.desc())
        .limit(limit)
        .all()
    )
    if not reviewed:
        return ""

    lines = ["RECENT FEEDBACK FROM RENTER (use to calibrate your scoring):"]
    for r in reviewed:
        lines.append(
            f"- [{r.feedback}] Score was {r.match_score}, "
            f"${r.price}, {r.neighborhood or 'unknown area'}: {r.summary or r.title}"
            + (f" — Note: {r.feedback_note}" if r.feedback_note else "")
        )
    return "\n".join(lines)


def _build_listing_text(listing: Listing) -> str:
    parts = []
    if listing.title:
        parts.append(f"Title: {listing.title}")
    if listing.price:
        parts.append(f"Price: ${listing.price}/month")
    if listing.bedrooms:
        parts.append(f"Bedrooms: {listing.bedrooms}")
    if listing.bathrooms:
        parts.append(f"Bathrooms: {listing.bathrooms}")
    if listing.sqft:
        parts.append(f"Sqft: {listing.sqft}")
    if listing.address:
        parts.append(f"Address: {listing.address}")
    if listing.neighborhood:
        parts.append(f"Neighborhood: {listing.neighborhood}")
    parts.append(f"Source: {listing.source}")
    if listing.url:
        parts.append(f"URL: {listing.url}")
    if listing.description:
        desc = listing.description[:3000]
        parts.append(f"\nFull description:\n{desc}")
    return "\n".join(parts)


def score_listing(listing: Listing, db: Session) -> dict:
    feedback_ctx = _get_feedback_examples(db)
    listing_text = _build_listing_text(listing)
    system_prompt = get_match_prompt(db)

    user_msg = listing_text
    if feedback_ctx:
        user_msg = f"{feedback_ctx}\n\n---\n\nLISTING TO EVALUATE:\n{listing_text}"

    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=500,
            system=system_prompt,
            messages=[{"role": "user", "content": user_msg}],
        )
        raw = response.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        return json.loads(raw)
    except (json.JSONDecodeError, IndexError, KeyError) as e:
        log.warning(f"Failed to parse matcher response for listing {listing.id}: {e}")
        return {
            "score": 0,
            "availability_date": "unknown",
            "availability_raw": "none found",
            "is_room_share": False,
            "neighborhood": "",
            "match_reasons": [],
            "concerns": ["AI scoring failed — review manually"],
            "summary": listing.title or "Unknown listing",
        }


def draft_message(listing: Listing) -> str:
    listing_text = _build_listing_text(listing)

    try:
        response = client.messages.create(
            model="claude-sonnet-4-6-20250514",
            max_tokens=300,
            system=DRAFT_SYSTEM,
            messages=[{"role": "user", "content": f"LISTING:\n{listing_text}"}],
        )
        return response.content[0].text.strip()
    except Exception as e:
        log.error(f"Failed to draft message for listing {listing.id}: {e}")
        return ""


def score_and_update(listing: Listing, db: Session) -> None:
    result = score_listing(listing, db)

    listing.match_score = result.get("score", 0)
    listing.availability_date = result.get("availability_date", "unknown")
    listing.is_room_share = result.get("is_room_share", False)
    listing.match_reasons = result.get("match_reasons", [])
    listing.match_concerns = result.get("concerns", [])
    listing.summary = result.get("summary", "")

    if not listing.neighborhood and result.get("neighborhood"):
        listing.neighborhood = result["neighborhood"]

    if listing.match_score >= 7 and not listing.is_room_share:
        listing.draft_message = draft_message(listing)

    db.commit()
