from flask import Flask, render_template, request
import os
import sqlite3

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "jobs.db")
app = Flask(__name__)


def query(sql, params=()):
    """Run a read-only query against jobs.db and return rows as dict-like objects."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        ensure_tables(conn)
        return conn.execute(sql, params).fetchall()


def ensure_tables(conn):
    """Create the emails/applications tables if they don't exist yet."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS emails (
            id TEXT PRIMARY KEY,
            email_num INTEGER,
            subject TEXT,
            sender TEXT,
            date_email TEXT,
            date_email_iso TEXT,
            company TEXT,
            job_title TEXT,
            status TEXT,
            parsed_date TEXT,
            reason TEXT,
            error TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cols = {row[1] for row in conn.execute("PRAGMA table_info(emails)")}
    if "date_email_iso" not in cols:
        conn.execute("ALTER TABLE emails ADD COLUMN date_email_iso TEXT")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS applications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email_id TEXT NOT NULL,
            company TEXT,
            job_title TEXT,
            status TEXT,
            parsed_date TEXT,
            reason TEXT,
            error TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(email_id) REFERENCES emails(id) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_applications_email_id ON applications(email_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_applications_status ON applications(status)"
    )


@app.route("/")
def home():
    status = request.args.get("status", "").strip()
    where = "WHERE a.status = ?" if status else ""
    params = (status,) if status else ()
    rows = query(
        f"""SELECT a.company, a.job_title, a.status, e.date_email, e.date_email_iso
            FROM applications a
            JOIN emails e ON a.email_id = e.id
            {where}
            ORDER BY e.date_email_iso DESC""",
        params,
    )
    counts = query(
        "SELECT status, COUNT(*) AS count FROM applications GROUP BY status"
    )
    return render_template("home.html", rows=rows, counts=counts, status=status)


if __name__ == "__main__":
    app.run(debug=True)
