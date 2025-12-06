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
    sort = request.args.get("sort", "date")
    try:
        page = max(1, int(request.args.get("page", "1")))
    except ValueError:
        page = 1
    try:
        page_size = int(request.args.get("page_size", "50"))
    except ValueError:
        page_size = 50
    page_size = max(10, min(page_size, 200))

    status_expr = "COALESCE(NULLIF(a.status, ''), 'unknown')"
    where = f"WHERE {status_expr} = ?" if status else ""
    params = (status,) if status else ()

    sort_map = {
        "date": "e.date_email_iso DESC",
        "company": "a.company COLLATE NOCASE ASC, e.date_email_iso DESC",
        "status": f"{status_expr} ASC, e.date_email_iso DESC",
    }
    order_by = sort_map.get(sort, sort_map["date"])

    offset = (page - 1) * page_size

    total_rows = query(
        f"""SELECT COUNT(*) AS c
            FROM applications a
            JOIN emails e ON a.email_id = e.id
            {where}""",
        params,
    )[0]["c"]

    rows = query(
        f"""SELECT a.company, a.job_title, {status_expr} AS status, e.date_email, e.date_email_iso
            FROM applications a
            JOIN emails e ON a.email_id = e.id
            {where}
            ORDER BY {order_by}
            LIMIT ? OFFSET ?""",
        params + (page_size, offset),
    )
    counts = query(
        "SELECT COALESCE(NULLIF(status, ''), 'unknown') AS status, COUNT(*) AS count "
        "FROM applications GROUP BY 1"
    )
    has_prev = page > 1
    has_next = offset + page_size < total_rows
    return render_template(
        "home.html",
        rows=rows,
        counts=counts,
        status=status,
        sort=sort,
        page=page,
        page_size=page_size,
        has_prev=has_prev,
        has_next=has_next,
        total_rows=total_rows,
    )


if __name__ == "__main__":
    app.run(debug=True)
