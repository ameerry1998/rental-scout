from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import BackgroundTasks, Depends, FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app import config
from app.db import get_db, init_db
from app.matcher import draft_message, score_and_update, get_match_prompt, save_match_prompt
from app.models import Listing, SearchRun
from app.scrapers import ScraperResult, run_single_scraper, SCRAPER_NAMES

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = FastAPI(title="Rental Scout", version="1.0.0")
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


@app.on_event("startup")
def startup():
    init_db()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _upsert_listing(db: Session, result: ScraperResult) -> Optional[Listing]:
    existing = (
        db.query(Listing)
        .filter_by(source=result.source, source_id=result.source_id)
        .first()
    )
    if existing:
        existing.last_seen_at = datetime.now(timezone.utc)
        return None

    listing = Listing(
        source=result.source,
        source_id=result.source_id,
        url=result.url,
        title=result.title,
        price=result.price,
        bedrooms=result.bedrooms,
        bathrooms=result.bathrooms,
        sqft=result.sqft,
        address=result.address,
        neighborhood=result.neighborhood,
        latitude=result.latitude,
        longitude=result.longitude,
        description=result.description,
        images=result.images,
        raw_data=result.raw_data,
        contact_info=result.contact_info,
    )
    db.add(listing)
    db.flush()
    return listing


def _passes_prefilter(r: ScraperResult) -> bool:
    if r.price and r.price > config.MAX_PRICE:
        return False
    if r.bedrooms is not None and r.bedrooms != config.BEDROOMS:
        return False
    if r.neighborhood or r.address:
        text = f"{r.neighborhood} {r.address} {r.title or ''}".lower()
        target_hoods = [n.lower() for n in config.TARGET_NEIGHBORHOODS]
        if not any(hood in text for hood in target_hoods):
            return False
    return True


def _get_known_ids(db: Session, source: str | None = None) -> dict[str, set[str]]:
    """Load known source_ids from DB, optionally filtered to one source."""
    query = db.query(Listing.source, Listing.source_id)
    if source:
        query = query.filter(Listing.source == source)
    known: dict[str, set[str]] = {}
    for src, sid in query.all():
        known.setdefault(src, set()).add(sid)
    return known


def _run_source(source: str, db: Session) -> dict:
    """Run a single platform: scrape → pre-filter → store → AI score."""
    run = SearchRun(status="running", sources_scraped=[source])
    db.add(run)
    db.commit()

    try:
        known = _get_known_ids(db, source)
        known_ids = known.get(source, set())

        results = run_single_scraper(source, known_ids)
        total = len(results)

        filtered = [r for r in results if _passes_prefilter(r)]
        rejected = total - len(filtered)
        log.info(f"[{source}] Pre-filter: {total} new → {len(filtered)} passed, {rejected} rejected")

        new_listings: list[Listing] = []
        for r in filtered:
            listing = _upsert_listing(db, r)
            if listing:
                new_listings.append(listing)
        db.commit()

        log.info(f"[{source}] Scoring {len(new_listings)} new listings")
        matches = 0
        for listing in new_listings:
            score_and_update(listing, db)
            if listing.match_score and listing.match_score >= 7:
                matches += 1

        run.completed_at = datetime.now(timezone.utc)
        run.new_listings_found = len(new_listings)
        run.matches_found = matches
        run.status = "completed"
        db.commit()

        return {
            "source": source,
            "status": "completed",
            "scraped": total,
            "passed_filter": len(filtered),
            "new_stored": len(new_listings),
            "matches": matches,
        }

    except Exception as e:
        run.status = "failed"
        run.error = str(e)
        run.completed_at = datetime.now(timezone.utc)
        db.commit()
        log.exception(f"[{source}] Pipeline failed")
        return {"source": source, "status": "failed", "error": str(e)}


def _run_all(db: Session) -> list[dict]:
    """Run each enabled platform sequentially. Each is isolated — one failing doesn't kill the rest."""
    results = []
    for name in SCRAPER_NAMES:
        log.info(f"=== Running {name} ===")
        result = _run_source(name, db)
        results.append(result)
        log.info(f"=== {name}: {result.get('status')} ===")
    return results


# ---------------------------------------------------------------------------
# Routes — Dashboard
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def dashboard(
    request: Request,
    tab: str = Query("matches", pattern="^(matches|all|reviewed|unreviewed)$"),
    db: Session = Depends(get_db),
):
    query = db.query(Listing)

    if tab == "matches":
        query = query.filter(Listing.match_score >= 7, Listing.is_room_share == False)  # noqa: E712
    elif tab == "reviewed":
        query = query.filter(Listing.feedback.isnot(None))
    elif tab == "unreviewed":
        query = query.filter(Listing.feedback.is_(None), Listing.match_score.isnot(None))

    listings = query.order_by(Listing.match_score.desc().nullslast(), Listing.created_at.desc()).limit(200).all()

    stats = {
        "total": db.query(Listing).count(),
        "matches": db.query(Listing).filter(Listing.match_score >= 7, Listing.is_room_share == False).count(),  # noqa: E712
        "unreviewed": db.query(Listing).filter(Listing.feedback.is_(None), Listing.match_score.isnot(None)).count(),
        "reviewed": db.query(Listing).filter(Listing.feedback.isnot(None)).count(),
    }

    last_run = db.query(SearchRun).order_by(SearchRun.started_at.desc()).first()

    # Per-source last run info for control panel
    source_status = {}
    for name in SCRAPER_NAMES:
        last = (
            db.query(SearchRun)
            .filter(SearchRun.sources_scraped.contains([name]))
            .order_by(SearchRun.started_at.desc())
            .first()
        )
        count = db.query(Listing).filter(Listing.source == name).count()
        source_status[name] = {"last_run": last, "count": count}

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "listings": listings,
        "tab": tab,
        "stats": stats,
        "last_run": last_run,
        "source_status": source_status,
        "scraper_names": SCRAPER_NAMES,
        "cron_secret": config.CRON_SECRET,
    })


# ---------------------------------------------------------------------------
# Routes — Listing detail
# ---------------------------------------------------------------------------

@app.get("/listing/{listing_id}", response_class=HTMLResponse)
def listing_detail(request: Request, listing_id: int, db: Session = Depends(get_db)):
    listing = db.query(Listing).get(listing_id)
    if not listing:
        raise HTTPException(404, "Listing not found")
    return templates.TemplateResponse("listing.html", {
        "request": request,
        "listing": listing,
    })


# ---------------------------------------------------------------------------
# Routes — Feedback
# ---------------------------------------------------------------------------

@app.post("/listing/{listing_id}/feedback")
def submit_feedback(
    listing_id: int,
    feedback: str = Form(...),
    note: str = Form(""),
    db: Session = Depends(get_db),
):
    listing = db.query(Listing).get(listing_id)
    if not listing:
        raise HTTPException(404, "Listing not found")

    listing.feedback = feedback
    listing.feedback_note = note if note else None
    db.commit()

    return RedirectResponse(url=f"/listing/{listing_id}", status_code=303)


# ---------------------------------------------------------------------------
# Routes — Draft message
# ---------------------------------------------------------------------------

@app.post("/listing/{listing_id}/draft")
def regenerate_draft(listing_id: int, db: Session = Depends(get_db)):
    listing = db.query(Listing).get(listing_id)
    if not listing:
        raise HTTPException(404, "Listing not found")

    listing.draft_message = draft_message(listing)
    db.commit()

    return RedirectResponse(url=f"/listing/{listing_id}", status_code=303)


# ---------------------------------------------------------------------------
# Routes — Run scrapers (per-platform + all)
# ---------------------------------------------------------------------------

@app.post("/run/{source}")
def trigger_source_run(
    source: str,
    background_tasks: BackgroundTasks,
    secret: str = Query(...),
):
    if secret != config.CRON_SECRET:
        raise HTTPException(403, "Invalid secret")
    if source not in SCRAPER_NAMES:
        raise HTTPException(404, f"Unknown source: {source}. Available: {SCRAPER_NAMES}")

    def _bg():
        from app.db import SessionLocal
        session = SessionLocal()
        try:
            _run_source(source, session)
        finally:
            session.close()

    background_tasks.add_task(_bg)
    return {"status": "started", "source": source}


@app.post("/run/all")
def trigger_all_run(
    background_tasks: BackgroundTasks,
    secret: str = Query(...),
):
    if secret != config.CRON_SECRET:
        raise HTTPException(403, "Invalid secret")

    def _bg():
        from app.db import SessionLocal
        session = SessionLocal()
        try:
            _run_all(session)
        finally:
            session.close()

    background_tasks.add_task(_bg)
    return {"status": "started", "sources": SCRAPER_NAMES}


# ---------------------------------------------------------------------------
# Routes — Re-score (wipe old scores, re-run AI with current prompt)
# ---------------------------------------------------------------------------

@app.post("/rescore")
def trigger_rescore(
    background_tasks: BackgroundTasks,
    secret: str = Query(...),
):
    if secret != config.CRON_SECRET:
        raise HTTPException(403, "Invalid secret")

    def _bg():
        from app.db import SessionLocal
        session = SessionLocal()
        try:
            listings = session.query(Listing).all()
            log.info(f"Re-scoring {len(listings)} listings with current prompt")
            for listing in listings:
                listing.match_score = None
                listing.match_reasons = None
                listing.match_concerns = None
                listing.summary = None
                listing.draft_message = None
                listing.availability_date = None
            session.commit()

            matches = 0
            for i, listing in enumerate(listings):
                score_and_update(listing, session)
                if listing.match_score and listing.match_score >= 7:
                    matches += 1
                if (i + 1) % 20 == 0:
                    log.info(f"  Re-scored {i + 1}/{len(listings)}")
            log.info(f"Re-score complete: {matches} matches out of {len(listings)}")
        finally:
            session.close()

    background_tasks.add_task(_bg)
    return {"status": "started", "message": "Re-scoring all listings in background"}


# ---------------------------------------------------------------------------
# Routes — Run history + health
# ---------------------------------------------------------------------------

@app.get("/runs", response_class=HTMLResponse)
def run_history(request: Request, db: Session = Depends(get_db)):
    runs = db.query(SearchRun).order_by(SearchRun.started_at.desc()).limit(50).all()
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "listings": [],
        "tab": "runs",
        "stats": {},
        "last_run": runs[0] if runs else None,
        "runs": runs,
        "source_status": {},
        "scraper_names": SCRAPER_NAMES,
        "cron_secret": config.CRON_SECRET,
    })


# ---------------------------------------------------------------------------
# Routes — Prompt editor
# ---------------------------------------------------------------------------

@app.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request, db: Session = Depends(get_db)):
    prompt = get_match_prompt(db)
    return templates.TemplateResponse("settings.html", {
        "request": request,
        "prompt": prompt,
        "saved": False,
    })


@app.post("/settings/prompt")
def update_prompt(
    request: Request,
    prompt: str = Form(...),
    db: Session = Depends(get_db),
):
    save_match_prompt(db, prompt)
    return templates.TemplateResponse("settings.html", {
        "request": request,
        "prompt": prompt,
        "saved": True,
    })


@app.get("/health")
def health():
    return {"status": "ok"}
