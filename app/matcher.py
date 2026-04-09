"""
Claude-powered listing matcher and message drafter.

Batch scoring: sends 5 listings per API call for speed and rate limit avoidance.
Scoring prompt is editable from the dashboard UI (stored in DB).
Injects recent user feedback as few-shot examples to improve matching over time.
"""
from __future__ import annotations

import json
import logging
import time

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
"""

BATCH_FORMAT_INSTRUCTIONS = """You will receive multiple listings separated by "---LISTING N---" markers.

Respond with a JSON ARRAY containing one object per listing, in the same order.
Each object must have these fields:
{
  "id": <the listing number from the marker>,
  "score": <0-10>,
  "availability_date": "<YYYY-MM-DD or 'unknown'>",
  "availability_raw": "<exact text from listing about availability, or 'none found'>",
  "is_room_share": <true/false>,
  "neighborhood": "<specific neighborhood and city, e.g. 'Central Square, Cambridge'>",
  "contact_info": "<any phone numbers, emails, agent names, or office names found in the listing, comma-separated. Return 'none' if nothing found>",
  "match_reasons": ["<reason1>", "<reason2>"],
  "concerns": ["<concern1>", "<concern2>"],
  "summary": "<one-sentence summary>"
}

Respond with ONLY the JSON array (no markdown fences, no explanation)."""

DRAFT_SYSTEM = """You are personalizing a rental inquiry message for a specific listing.

Use this template as the base — keep the structure and tone, but personalize the first sentence to reference something specific about the listing (the address, a feature mentioned in the description, the neighborhood, etc.):

---
I'm reaching out to express interest in the [INSERT SPECIFIC ADDRESS OR UNIT DESCRIPTION] listing. I wanted to share some details upfront to make the process easier:

Who: Just myself and my fiancé, Ameer — no pets, no roommates.
Move-in: Flexible between August 1 and September 1, 2026.
Financials: I am employed full-time with an annual salary of $70,000 and a credit score above 800. My fiancé is currently between jobs. We have a cosigner — my mother — available if needed to meet income requirements.

We are a quiet, responsible couple looking to move into our own space together. We'd love to schedule a showing at your earliest convenience.

You can reach both of us at:
Caity: caity.enroth@gmail.com
Ameer: ameer.rayan@gmail.com

Thank you so much for your time — looking forward to hearing from you!
---

RULES:
- Replace [INSERT SPECIFIC ADDRESS OR UNIT DESCRIPTION] with actual details from the listing
- If the listing mentions a specific feature worth noting (e.g. "the unit with the private porch" or "the renovated 1BR on Elm St"), reference it naturally in the opening
- Do NOT change the financial details, contact info, or overall structure
- Keep it warm but professional — this is Caity's voice
- Return ONLY the message text, ready to copy-paste"""


def get_match_prompt(db: Session) -> str:
    row = db.query(Setting).filter_by(key="match_prompt").first()
    if row and row.value:
        return row.value
    return DEFAULT_MATCH_PROMPT


def save_match_prompt(db: Session, prompt: str) -> None:
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
        desc = listing.description[:2000]
        parts.append(f"\nFull description:\n{desc}")
    return "\n".join(parts)


def score_batch(listings: list[Listing], db: Session) -> list[dict]:
    """Score multiple listings in a single API call. Returns list of result dicts."""
    feedback_ctx = _get_feedback_examples(db)
    system_prompt = get_match_prompt(db) + "\n\n" + BATCH_FORMAT_INSTRUCTIONS

    # Build the multi-listing prompt
    parts = []
    if feedback_ctx:
        parts.append(feedback_ctx)
        parts.append("\n---\n")
    for i, listing in enumerate(listings):
        parts.append(f"---LISTING {i + 1}---")
        parts.append(_build_listing_text(listing))
        parts.append("")

    user_msg = "\n".join(parts)

    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300 * len(listings),
            system=system_prompt,
            messages=[{"role": "user", "content": user_msg}],
        )
        raw = response.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        results = json.loads(raw)
        if isinstance(results, list):
            return results
        return [results]
    except Exception as e:
        log.error(f"Batch scoring failed: {e}")
        # Return empty results so caller can fall back
        return []


def _default_result(listing: Listing) -> dict:
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
            model="claude-haiku-4-5-20251001",
            max_tokens=400,
            system=DRAFT_SYSTEM,
            messages=[{"role": "user", "content": f"LISTING:\n{listing_text}"}],
        )
        return response.content[0].text.strip()
    except Exception as e:
        log.error(f"Failed to draft message for listing {listing.id}: {e}")
        return ""


def _apply_result(listing: Listing, result: dict) -> None:
    """Apply a scoring result dict to a Listing model."""
    listing.match_score = result.get("score", 0)
    listing.availability_date = result.get("availability_date", "unknown")
    listing.is_room_share = result.get("is_room_share", False)
    listing.match_reasons = result.get("match_reasons", [])
    listing.match_concerns = result.get("concerns", [])
    listing.summary = result.get("summary", "")
    if not listing.neighborhood and result.get("neighborhood"):
        listing.neighborhood = result["neighborhood"]
    # AI-extracted contact info — overwrite if AI found something and we don't have any
    ai_contact = result.get("contact_info", "")
    if ai_contact and ai_contact != "none" and not listing.contact_info:
        listing.contact_info = ai_contact


def score_and_update(listing: Listing, db: Session) -> None:
    """Score a single listing (fallback, used for one-offs)."""
    results = score_batch([listing], db)
    result = results[0] if results else _default_result(listing)
    _apply_result(listing, result)

    if listing.match_score >= 7 and not listing.is_room_share:
        listing.draft_message = draft_message(listing)

    db.commit()


def score_and_update_batch(listings: list[Listing], db: Session, batch_size: int = 5) -> int:
    """Score listings in batches of batch_size. Returns total matches found."""
    matches = 0
    total = len(listings)

    for batch_start in range(0, total, batch_size):
        batch = listings[batch_start:batch_start + batch_size]
        batch_num = batch_start // batch_size + 1
        total_batches = (total + batch_size - 1) // batch_size
        log.info(f"  Scoring batch {batch_num}/{total_batches} ({len(batch)} listings)")

        try:
            results = score_batch(batch, db)

            # Match results to listings by position
            for j, listing in enumerate(batch):
                if j < len(results):
                    _apply_result(listing, results[j])
                else:
                    _apply_result(listing, _default_result(listing))

                if listing.match_score >= 7 and not listing.is_room_share:
                    listing.draft_message = draft_message(listing)
                    matches += 1

            db.commit()
            log.info(f"  Batch {batch_num} done — {matches} matches so far")

        except Exception as e:
            log.error(f"  Batch {batch_num} failed: {e}")
            # Apply defaults for the failed batch
            for listing in batch:
                _apply_result(listing, _default_result(listing))
            db.commit()

        # Small delay between batches to avoid rate limits
        time.sleep(0.5)

    return matches
