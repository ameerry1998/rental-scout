from datetime import datetime, timezone
from sqlalchemy import (
    Column, Integer, String, Float, Text, DateTime, JSON, Boolean,
)
from app.db import Base


def _now():
    return datetime.now(timezone.utc)


class Listing(Base):
    __tablename__ = "listings"

    id = Column(Integer, primary_key=True)
    source = Column(String(50), nullable=False)          # craigslist, zillow, etc.
    source_id = Column(String(255), nullable=False)       # unique ID from source
    url = Column(Text)
    title = Column(Text)
    price = Column(Integer)
    bedrooms = Column(Float)
    bathrooms = Column(Float)
    sqft = Column(Integer)
    address = Column(Text)
    neighborhood = Column(String(255))
    latitude = Column(Float)
    longitude = Column(Float)
    description = Column(Text)
    images = Column(JSON, default=list)
    raw_data = Column(JSON)

    # AI-extracted fields
    availability_date = Column(String(50))
    match_score = Column(Integer)
    match_reasons = Column(JSON, default=list)
    match_concerns = Column(JSON, default=list)
    summary = Column(Text)
    is_room_share = Column(Boolean, default=False)

    # User interaction
    feedback = Column(String(50))       # good_match, wrong_price, bad_location, already_gone, not_interested, spam
    feedback_note = Column(Text)
    draft_message = Column(Text)
    contact_info = Column(Text)

    # Timestamps
    first_seen_at = Column(DateTime, default=_now)
    last_seen_at = Column(DateTime, default=_now, onupdate=_now)
    created_at = Column(DateTime, default=_now)

    @property
    def score_color(self):
        if self.match_score is None:
            return "gray"
        if self.match_score >= 8:
            return "green"
        if self.match_score >= 5:
            return "yellow"
        return "red"


class SearchRun(Base):
    __tablename__ = "search_runs"

    id = Column(Integer, primary_key=True)
    started_at = Column(DateTime, default=_now)
    completed_at = Column(DateTime)
    sources_scraped = Column(JSON, default=list)
    new_listings_found = Column(Integer, default=0)
    matches_found = Column(Integer, default=0)
    status = Column(String(20), default="running")       # running, completed, failed
    error = Column(Text)
