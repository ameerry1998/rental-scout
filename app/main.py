from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import BackgroundTasks, Depends, FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app import config
from app.db import get_db, init_db
from app.matcher import draft_message, score_and_update, score_and_update_batch, get_match_prompt, save_match_prompt
from app.models import Listing, SearchRun, ActivityLog
from app.scrapers import ScraperResult, run_single_scraper, trigger_new_scrape, enrich_facebook_details, SCRAPER_NAMES

# Lock to prevent duplicate concurrent runs
_running_jobs: set[str] = set()
_jobs_lock = threading.Lock()


class DBLogHandler(logging.Handler):
    """Writes app log lines to the activity_logs DB table so they persist."""
    def emit(self, record):
        if record.name.startswith("app.") or record.name == "__main__":
            try:
                from app.db import SessionLocal
                session = SessionLocal()
                session.add(ActivityLog(
                    level=record.levelname,
                    message=record.getMessage(),
                ))
                session.commit()
                session.close()
            except Exception:
                pass


logging.basicConfig(level=logging.INFO)
logging.getLogger("app").addHandler(DBLogHandler())
log = logging.getLogger(__name__)

app = FastAPI(title="Rental Scout", version="1.0.0")
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


def _friendly_date(val: str) -> str:
    """Convert '2026-09-01' → 'September 1' or return as-is if unparseable."""
    try:
        d = datetime.strptime(val, "%Y-%m-%d")
        return d.strftime("%B %-d")
    except (ValueError, TypeError):
        return val


templates.env.filters["friendly_date"] = _friendly_date


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

        # Fetch full listing details for Facebook (only for filtered listings)
        if source == "facebook" and filtered:
            filtered = enrich_facebook_details(filtered)

        new_listings: list[Listing] = []
        for r in filtered:
            listing = _upsert_listing(db, r)
            if listing:
                new_listings.append(listing)
        db.commit()

        log.info(f"[{source}] Batch-scoring {len(new_listings)} new listings (5 per API call)")
        matches = score_and_update_batch(new_listings, db, batch_size=5)

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
        import traceback
        error_detail = traceback.format_exc()
        log.error(f"[{source}] Pipeline failed: {e}")
        log.error(f"[{source}] {error_detail}")
        run.status = "failed"
        run.error = str(e)
        run.completed_at = datetime.now(timezone.utc)
        try:
            db.commit()
        except Exception:
            db.rollback()
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
    tab: str = Query("matches", pattern="^(matches|scored|unscored|contacted|activity)$"),
    db: Session = Depends(get_db),
):
    query = db.query(Listing)

    if tab == "matches":
        query = query.filter(Listing.match_score >= 7, Listing.is_room_share == False)  # noqa: E712
    elif tab == "scored":
        query = query.filter(Listing.match_score.isnot(None))
    elif tab == "unscored":
        query = query.filter(Listing.match_score.is_(None))
    elif tab == "contacted":
        query = query.filter(Listing.feedback == "contacted")

    listings = query.order_by(Listing.match_score.desc().nullslast(), Listing.created_at.desc()).limit(200).all()

    unscored = db.query(Listing).filter(Listing.match_score.is_(None)).count()
    scored = db.query(Listing).filter(Listing.match_score.isnot(None)).count()
    stats = {
        "total": db.query(Listing).count(),
        "matches": db.query(Listing).filter(Listing.match_score >= 7, Listing.is_room_share == False).count(),  # noqa: E712
        "scored": scored,
        "unscored": unscored,
        "contacted": db.query(Listing).filter(Listing.feedback == "contacted").count(),
    }

    last_run = db.query(SearchRun).order_by(SearchRun.started_at.desc()).first()

    # Per-source last run info for control panel
    source_status = {}
    for name in SCRAPER_NAMES:
        # Use cast to text for Postgres JSON compatibility
        from sqlalchemy import cast, String
        last = (
            db.query(SearchRun)
            .filter(cast(SearchRun.sources_scraped, String).contains(name))
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

    job_key = f"run:{source}"
    with _jobs_lock:
        if job_key in _running_jobs:
            return {"status": "already_running", "source": source}
        _running_jobs.add(job_key)

    def _bg():
        from app.db import SessionLocal
        session = SessionLocal()
        try:
            _run_source(source, session)
        finally:
            session.close()
            with _jobs_lock:
                _running_jobs.discard(job_key)

    background_tasks.add_task(_bg)
    return {"status": "started", "source": source}


@app.post("/scrape/{source}")
def trigger_new_apify_scrape(
    source: str,
    secret: str = Query(...),
):
    """Trigger a fresh Apify run. Does NOT wait — results get imported on next Run click."""
    if secret != config.CRON_SECRET:
        raise HTTPException(403, "Invalid secret")
    if source not in SCRAPER_NAMES:
        raise HTTPException(404, f"Unknown source: {source}")
    trigger_new_scrape(source)
    return {"status": "triggered", "source": source, "message": "New scrape started. Click Run in a few minutes to import."}


@app.post("/run/all")
def trigger_all_run(
    background_tasks: BackgroundTasks,
    secret: str = Query(...),
):
    if secret != config.CRON_SECRET:
        raise HTTPException(403, "Invalid secret")

    with _jobs_lock:
        if "run:all" in _running_jobs:
            return {"status": "already_running"}
        _running_jobs.add("run:all")

    def _bg():
        from app.db import SessionLocal
        session = SessionLocal()
        try:
            _run_all(session)
        finally:
            session.close()
            with _jobs_lock:
                _running_jobs.discard("run:all")

    background_tasks.add_task(_bg)
    return {"status": "started", "sources": SCRAPER_NAMES}


# ---------------------------------------------------------------------------
# Routes — Re-import a source (wipe + re-fetch with current code)
# ---------------------------------------------------------------------------

@app.post("/reimport/{source}")
def trigger_reimport(
    source: str,
    background_tasks: BackgroundTasks,
    secret: str = Query(...),
):
    """Delete all listings from a source and re-import fresh. Use when the scraper code changed."""
    if secret != config.CRON_SECRET:
        raise HTTPException(403, "Invalid secret")
    if source not in SCRAPER_NAMES:
        raise HTTPException(404, f"Unknown source: {source}")

    job_key = f"reimport:{source}"
    with _jobs_lock:
        if job_key in _running_jobs:
            return {"status": "already_running"}
        _running_jobs.add(job_key)

    def _bg():
        from app.db import SessionLocal
        session = SessionLocal()
        try:
            deleted = session.query(Listing).filter(Listing.source == source).delete()
            session.commit()
            log.info(f"[{source}] Deleted {deleted} old listings, re-importing fresh")
            _run_source(source, session)
        finally:
            session.close()
            with _jobs_lock:
                _running_jobs.discard(job_key)

    background_tasks.add_task(_bg)
    return {"status": "started", "source": source, "message": f"Wiping {source} listings and re-importing"}


# ---------------------------------------------------------------------------
# Routes — Score unscored + Re-score all
# ---------------------------------------------------------------------------

@app.post("/score-remaining")
def trigger_score_remaining(
    background_tasks: BackgroundTasks,
    secret: str = Query(...),
):
    """Score only listings that have no score yet (picks up where it left off)."""
    if secret != config.CRON_SECRET:
        raise HTTPException(403, "Invalid secret")

    with _jobs_lock:
        if "score" in _running_jobs:
            return {"status": "already_running"}
        _running_jobs.add("score")

    def _bg():
        from app.db import SessionLocal
        session = SessionLocal()
        try:
            unscored = session.query(Listing).filter(Listing.match_score.is_(None)).all()
            log.info(f"Scoring {len(unscored)} unscored listings (batch mode)")
            if unscored:
                matches = score_and_update_batch(unscored, session, batch_size=5)
                log.info(f"Scoring complete: {matches} matches out of {len(unscored)}")
            else:
                log.info("No unscored listings found")
        finally:
            session.close()
            with _jobs_lock:
                _running_jobs.discard("score")

    background_tasks.add_task(_bg)
    return {"status": "started", "message": "Scoring unscored listings"}


@app.post("/rescore")
def trigger_rescore(
    background_tasks: BackgroundTasks,
    secret: str = Query(...),
):
    """Wipe ALL scores and re-score everything with current prompt."""
    if secret != config.CRON_SECRET:
        raise HTTPException(403, "Invalid secret")

    with _jobs_lock:
        if "rescore" in _running_jobs:
            return {"status": "already_running"}
        _running_jobs.add("rescore")

    def _bg():
        from app.db import SessionLocal
        session = SessionLocal()
        try:
            listings = session.query(Listing).all()
            log.info(f"Re-scoring {len(listings)} listings with current prompt (batch mode)")
            for listing in listings:
                listing.match_score = None
                listing.match_reasons = None
                listing.match_concerns = None
                listing.summary = None
                listing.draft_message = None
                listing.availability_date = None
            session.commit()

            matches = score_and_update_batch(listings, session, batch_size=5)
            log.info(f"Re-score complete: {matches} matches out of {len(listings)}")
        finally:
            session.close()
            with _jobs_lock:
                _running_jobs.discard("rescore")

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


@app.get("/api/logs")
def get_logs(since_id: int = Query(0), db: Session = Depends(get_db)):
    """Return activity log entries. `since_id` = last log ID seen, returns only newer entries."""
    query = db.query(ActivityLog)
    if since_id > 0:
        query = query.filter(ActivityLog.id > since_id)
    else:
        # First load: show last 100 entries
        query = query.order_by(ActivityLog.id.desc()).limit(100)
        entries = query.all()
        entries.reverse()
        return {
            "logs": [{"id": e.id, "ts": e.timestamp.strftime("%Y-%m-%d %H:%M:%S"), "msg": e.message, "level": e.level} for e in entries],
            "last_id": entries[-1].id if entries else 0,
        }

    entries = query.order_by(ActivityLog.id.asc()).limit(50).all()
    return {
        "logs": [{"id": e.id, "ts": e.timestamp.strftime("%Y-%m-%d %H:%M:%S"), "msg": e.message, "level": e.level} for e in entries],
        "last_id": entries[-1].id if entries else since_id,
    }


@app.get("/api/status")
def get_status():
    """Check what jobs are currently running."""
    with _jobs_lock:
        return {"running": list(_running_jobs)}


@app.get("/health")
def health():
    return {"status": "ok"}
