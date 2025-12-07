status_map = {
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
    None: "unknown",
}


def clean_status(raw_status):
    """
    Normalize varied status strings to a small set of known values.
    Defaults to 'unknown' if the input is missing or not recognized.
    """
    normalized = (raw_status or "").strip().lower()
    return status_map.get(normalized, "unknown")
