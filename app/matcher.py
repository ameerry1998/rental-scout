"""
Claude-powered listing matcher and message drafter.

Uses Haiku for fast scoring of every listing, Sonnet for drafting outreach.
Injects recent user feedback as few-shot examples to improve matching over time.
"""
from __future__ import annotations

import json
import logging

import anthropic
from sqlalchemy.orm import Session

from app import config
from app.models import Listing

log = logging.getLogger(__name__)

client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)

MATCH_SYSTEM = f"""You are a rental listing analyst for {config.RENTER_NAME} searching in {config.SEARCH_AREA}.

SEARCH CRITERIA:
- Bedrooms: {config.BEDROOMS}
- Max rent: ${config.MAX_PRICE:,}/month
- Target move-in: {config.TARGET_MOVE_IN} (preferred) or {config.ALT_MOVE_IN} (also acceptable)
- Acceptable neighborhoods: {', '.join(config.TARGET_NEIGHBORHOODS)}
- Must be an actual apartment/unit for rent — NOT a room share, sublet, parking spot, or storage unit

YOUR TASK:
1. Extract the ACTUAL availability date from the description text — look for phrases like "available X", "move-in", "ready after", "lease starts", "current lease ends", "Sept 1" etc. Do NOT just rely on structured fields.
2. Score the listing 0-10 based on how well it matches the criteria.
3. Flag any concerns (too far from target area, possible scam, room share disguised as apartment, etc.)

Scoring guide:
- 9-10: Perfect match — right area, right price, availability aligns with move-in dates
- 7-8: Strong match — most criteria met, minor concerns
- 5-6: Decent — one significant mismatch (e.g. price near max, uncertain availability)
- 3-4: Weak — multiple mismatches but worth a look
- 0-2: Not a match — wrong area, too expensive, room share, or spam

Respond with ONLY valid JSON (no markdown, no explanation):
{{
  "score": <0-10>,
  "availability_date": "<YYYY-MM-DD or 'unknown' or 'not_available'>",
  "availability_raw": "<exact quoted text from listing about availability, or 'none found'>",
  "is_room_share": <true/false>,
  "neighborhood": "<best guess neighborhood or area>",
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


def _get_feedback_examples(db: Session, limit: int = 10) -> str:
    """Pull recent user feedback to inject as few-shot context."""
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
        # Truncate very long descriptions to save tokens
        desc = listing.description[:3000]
        parts.append(f"\nFull description:\n{desc}")
    return "\n".join(parts)


def score_listing(listing: Listing, db: Session) -> dict:
    """Score a single listing using Claude. Returns the parsed JSON result."""
    feedback_ctx = _get_feedback_examples(db)
    listing_text = _build_listing_text(listing)

    user_msg = listing_text
    if feedback_ctx:
        user_msg = f"{feedback_ctx}\n\n---\n\nLISTING TO EVALUATE:\n{listing_text}"

    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=500,
            system=MATCH_SYSTEM,
            messages=[{"role": "user", "content": user_msg}],
        )
        raw = response.content[0].text.strip()
        # Handle potential markdown wrapping
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
    """Generate a draft outreach message for a matched listing."""
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
    """Score a listing and persist the results."""
    result = score_listing(listing, db)

    listing.match_score = result.get("score", 0)
    listing.availability_date = result.get("availability_date", "unknown")
    listing.is_room_share = result.get("is_room_share", False)
    listing.match_reasons = result.get("match_reasons", [])
    listing.match_concerns = result.get("concerns", [])
    listing.summary = result.get("summary", "")

    # Extract neighborhood if we didn't have one
    if not listing.neighborhood and result.get("neighborhood"):
        listing.neighborhood = result["neighborhood"]

    # Auto-draft message for strong matches
    if listing.match_score >= 7 and not listing.is_room_share:
        listing.draft_message = draft_message(listing)

    db.commit()
