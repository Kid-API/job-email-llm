import os
import sqlite3
import sys

# Make the project root importable so we can load status_utils.py
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
from status_utils import clean_status

conn = sqlite3.connect("jobs.db")
cur = conn.cursor()

cur.execute("SELECT id, status FROM emails")
updates = []
for email_id, raw in cur.fetchall():
    normalized = clean_status(raw)
    if normalized != raw:
        updates.append((normalized, email_id))

if updates:
    cur.executemany("UPDATE emails SET status = ? WHERE id = ?", updates)
    conn.commit()
    print(f"Updated {len(updates)} rows")
else:
    print("No status changes needed")

conn.close()
