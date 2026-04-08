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
from app.matcher import draft_message, score_and_update
from app.models import Listing, SearchRun
from app.scrapers import ScraperResult, run_all_scrapers

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
    """Insert a new listing or update last_seen_at for existing ones.
    Returns the Listing only if it's new (needs scoring)."""
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


def _run_pipeline(db: Session) -> dict:
    """Full pipeline: scrape → dedup → score → draft."""
    run = SearchRun(status="running")
    db.add(run)
    db.commit()

    try:
        results = run_all_scrapers()
        sources_seen = list({r.source for r in results})

        new_listings: list[Listing] = []
        for r in results:
            listing = _upsert_listing(db, r)
            if listing:
                new_listings.append(listing)
        db.commit()

        log.info(f"New listings to score: {len(new_listings)}")

        matches = 0
        for listing in new_listings:
            score_and_update(listing, db)
            if listing.match_score and listing.match_score >= 7:
                matches += 1

        run.completed_at = datetime.now(timezone.utc)
        run.sources_scraped = sources_seen
        run.new_listings_found = len(new_listings)
        run.matches_found = matches
        run.status = "completed"
        db.commit()

        return {
            "status": "completed",
            "new_listings": len(new_listings),
            "matches": matches,
            "sources": sources_seen,
        }

    except Exception as e:
        run.status = "failed"
        run.error = str(e)
        run.completed_at = datetime.now(timezone.utc)
        db.commit()
        log.exception("Pipeline failed")
        raise


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

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "listings": listings,
        "tab": tab,
        "stats": stats,
        "last_run": last_run,
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
# Routes — Trigger pipeline run
# ---------------------------------------------------------------------------

@app.post("/run")
def trigger_run(
    background_tasks: BackgroundTasks,
    secret: str = Query(...),
    db: Session = Depends(get_db),
):
    if secret != config.CRON_SECRET:
        raise HTTPException(403, "Invalid secret")

    def _bg():
        from app.db import SessionLocal
        session = SessionLocal()
        try:
            _run_pipeline(session)
        finally:
            session.close()

    background_tasks.add_task(_bg)
    return {"status": "started", "message": "Pipeline running in background"}


@app.post("/run/sync")
def trigger_run_sync(secret: str = Query(...), db: Session = Depends(get_db)):
    """Synchronous run — useful for testing. Will timeout on large runs."""
    if secret != config.CRON_SECRET:
        raise HTTPException(403, "Invalid secret")
    result = _run_pipeline(db)
    return result


# ---------------------------------------------------------------------------
# Routes — Run history
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
    })


# ---------------------------------------------------------------------------
# Routes — Health check
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok"}
