# scripts/backfill_applications.py
import os
import sqlite3
import sys

# Make project root importable so we can load status_utils
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
from status_utils import clean_status

conn = sqlite3.connect("jobs.db")
conn.execute("PRAGMA foreign_keys = ON;")
cur = conn.cursor()

# Clear old application rows (optional, if you want a clean slate)
cur.execute("DELETE FROM applications;")

rows = cur.execute(
    "SELECT id, company, job_title, status, parsed_date, reason, error FROM emails"
).fetchall()

to_insert = []
for email_id, company, job_title, status, parsed_date, reason, error in rows:
    to_insert.append({
        "email_id": email_id,
        "company": company or "",
        "job_title": job_title or "",
        "status": clean_status(status),
        "parsed_date": parsed_date or "",
        "reason": reason or "",
        "error": error or "",
    })

cur.executemany(
    """INSERT INTO applications
       (email_id, company, job_title, status, parsed_date, reason, error)
       VALUES (:email_id, :company, :job_title, :status, :parsed_date, :reason, :error)
    """,
    to_insert,
)
conn.commit()
conn.close()
