# Job Application Dashboard

A simple Flask + SQLite dashboard for tracking job-application emails. It fetches your Gmail messages, extracts companies/roles/statuses with an LLM, stores them in `jobs.db`, and shows a filterable/paginated UI.

## Requirements
- Python 3.11+ (repo currently uses 3.14 in a venv)
- `pip install -r requirements.txt` (if present) or install the libs you use: `flask`, `google-api-python-client`, `google-auth-oauthlib`, `google-auth-httplib2`, plus your LLM CLI (e.g., `ollama`)
- Gmail OAuth files: `credentials.json` (token saved to `token.pickle` after first auth)

## Setup
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt   # or install deps manually
```

## Ingest emails
Runs Gmail fetch + LLM parsing into SQLite (`jobs.db`).
```bash
python parse_gmail_jobs.py
```
Key options are in the script:
- `max_total` (default 4000) limits how many Gmail messages to fetch.
- Gmail query defaults to `after:2024/03/20` with subject filters; edit in `get_job_emails`.
- Duplicate skip is on (uses Gmail message ID).
- Blacklist terms live in `blacklist.txt` (one term/phrase per line, no quotes).

## Run the UI
Starts the Flask app at `http://127.0.0.1:5000/` (or `localhost:5000`).
```bash
python app.py
```
UI features:
- Filter by status, exclude statuses, hide unknowns
- Date range filters (YYYY-MM-DD)
- Sort by date/company/status
- Pagination with configurable page size
- Rows with blank company AND job title are hidden

## Normalize company names (optional)
Uses your local LLM (Ollama) to clean capitalization.
```bash
python scripts/normalize_company_names.py        # dry-run
python scripts/normalize_company_names.py --apply  # write changes
```

## Database notes
- Tables: `emails` (one row per Gmail message) and `applications` (one row per job mention, FK to emails).
- SQLite WAL files (`jobs.db-wal`, `jobs.db-shm`) should stay untracked; add them to `.gitignore`.
- If the DB is empty, re-run ingestion to repopulate.

## Common issues
- `ModuleNotFoundError: status_utils`: ensure `status_utils.py` is in the repo root (it is) and that `parse_gmail_jobs.py` is run from the project root.
- `no such table: applications`: run `app.py` or `parse_gmail_jobs.py` once to create tables, or restore a backup.
- 403 in browser: try `http://localhost:5000/`, disable VPN/proxy, or change port in `app.py`.
