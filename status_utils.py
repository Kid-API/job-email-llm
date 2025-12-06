"""
Helpers for cleaning and standardizing application status values.
"""

# Known statuses we want to standardize to.
CANONICAL = {
    "applied",
    "interview",
    "offer",
    "rejected",
    "unknown",
}

# Map a bunch of variations to the canonical buckets.
STATUS_MAP = {
    "application received": "applied",
    "applied": "applied",
    "applied for": "applied",
    "submitted": "applied",
    "application submitted": "applied",
    "interview invite": "interview",
    "scheduled interview": "interview",
    "interview scheduled": "interview",
    "interview": "interview",
    "rejection": "rejected",
    "declined": "rejected",
    "rejected": "rejected",
    "not selected": "rejected",
    "offer": "offer",
    "other": "unknown",
    "": "unknown",
}


def clean_status(raw_status):
    """
    Normalize a raw status string to one of the canonical values.

    - Lowercases and trims whitespace.
    - Falls back to "unknown" when we don't recognize it.
    """
    s = (raw_status or "").strip().lower()
    return STATUS_MAP.get(s, "unknown")
