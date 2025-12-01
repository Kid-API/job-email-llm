from flask import Flask, render_template, request
import sqlite3

DB_PATH = "jobs.db"
app = Flask(__name__)


def query(sql, params=()):
    """Run a read-only query against jobs.db and return rows as dict-like objects."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        ensure_tables(conn)
        return conn.execute(sql, params).fetchall()


def ensure_tables(conn):
    """Create the emails table if it doesn't exist yet."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS emails (
            id TEXT PRIMARY KEY,
            email_num INTEGER,
            subject TEXT,
            sender TEXT,
            date_email TEXT,
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


@app.route("/")
def home():
    status = request.args.get("status", "").strip()
    where = "WHERE status = ?" if status else ""
    params = (status,) if status else ()
    rows = query(
        f"""SELECT company, job_title, status, date_email
            FROM emails {where}
            ORDER BY date_email DESC""",
        params,
    )
    counts = query("SELECT status, COUNT(*) AS count FROM emails GROUP BY status")
    return render_template("home.html", rows=rows, counts=counts, status=status)


if __name__ == "__main__":
    app.run(debug=True)
