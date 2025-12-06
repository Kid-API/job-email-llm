import os
import pickle
import base64
import csv
import sqlite3
import threading
import subprocess
import json
import time
import sys
from email.utils import parsedate_to_datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Paths and database setup
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)
DB_PATH = os.path.join(BASE_DIR, "jobs.db")
_db_lock = threading.Lock()

from status_utils import clean_status


def detect_platform(sender):
    """Roughly tag known platforms from the sender address."""
    s = (sender or "").lower()
    if "linkedin.com" in s:
        return "linkedin"
    if "indeed.com" in s:
        return "indeed"
    if "greenhouse.io" in s:
        return "greenhouse"
    if "lever.co" in s:
        return "lever"
    return "other"


def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    ensure_schema(conn)
    return conn


def load_existing_ids(conn):
    """Return a set of IDs already stored in the database."""
    ensure_schema(conn)
    rows = conn.execute("SELECT id FROM emails").fetchall()
    return {r[0] for r in rows}


def ensure_schema(conn):
    """Create tables and add missing columns if needed."""
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
    # Add date_email_iso if the table already existed
    cols = {row[1] for row in conn.execute("PRAGMA table_info(emails)")}
    if "date_email_iso" not in cols:
        conn.execute("ALTER TABLE emails ADD COLUMN date_email_iso TEXT")
    # Applications table stores one row per job mention, linked back to emails
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


def save_rows(conn, rows):
    """Save parsed rows into SQLite, updating existing IDs."""
    if not rows:
        return

    def _clean(value):
        if value is None:
            return ""
        if isinstance(value, list):
            return "; ".join(str(v) for v in value)
        if isinstance(value, dict):
            return json.dumps(value)
        return value

    cleaned = []
    applications_by_email = {}
    for row in rows:
        applications_by_email[row["id"]] = row.get("applications", [])
        cleaned_row = {k: _clean(v) for k, v in row.items() if k != "applications"}
        cleaned.append(cleaned_row)

    with _db_lock, conn:
        conn.executemany(
            """
            INSERT INTO emails
            (id, email_num, subject, sender, date_email, date_email_iso,
             company, job_title, status, parsed_date, reason, error)
            VALUES (:id, :email_num, :subject, :from, :date_email, :date_email_iso,
                    :company, :job_title, :status, :parsed_date, :reason, :error)
            ON CONFLICT(id) DO UPDATE SET
                subject=excluded.subject,
                sender=excluded.sender,
                date_email_iso=excluded.date_email_iso,
                company=excluded.company,
                job_title=excluded.job_title,
                status=excluded.status,
                parsed_date=excluded.parsed_date,
                reason=excluded.reason,
                error=excluded.error
            """,
            cleaned,
        )
        # Refresh applications for these emails to avoid duplicates
        email_ids = [(row["id"],) for row in cleaned]
        conn.executemany(
            "DELETE FROM applications WHERE email_id = ?",
            email_ids,
        )
        applications = []
        for row in rows:
            apps = applications_by_email.get(row["id"], [])
            if not apps:
                apps = [
                    {
                        "company": row.get("company", ""),
                        "job_title": row.get("job_title", ""),
                        "status": row.get("status", ""),
                        "parsed_date": row.get("parsed_date", ""),
                        "reason": row.get("reason", ""),
                        "error": row.get("error", ""),
                    }
                ]
            for app in apps:
                applications.append(
                    {
                        "email_id": _clean(row["id"]),
                        "company": _clean(app.get("company", "")),
                        "job_title": _clean(app.get("job_title", "")),
                        "status": _clean(app.get("status", "")),
                        "parsed_date": _clean(app.get("parsed_date", "")),
                        "reason": _clean(app.get("reason", "")),
                        "error": _clean(app.get("error", "")),
                    }
                )
        conn.executemany(
            """
            INSERT INTO applications
            (email_id, company, job_title, status, parsed_date, reason, error)
            VALUES (:email_id, :company, :job_title, :status, :parsed_date, :reason, :error)
            """,
            applications,
        )


# Gmail API setup
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def authenticate_gmail():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return build('gmail', 'v1', credentials=creds)

def load_blacklist(filename="blacklist.txt"):
    if not os.path.exists(filename):
        print(f"Warning: {filename} not found. No blacklist will be applied.")
        return []

    with open(filename, "r") as f:
        words = [line.strip().lower() for line in f if line.strip()]
    return words


def get_job_emails(service, query=None, max_total=300):
    if not query:
        query = "(subject:applied OR subject:application OR subject:interview OR subject:rejected) after:2024/03/20"
    emails = []
    next_page_token = None
    fetched = 0
    while fetched < max_total:
        kwargs = {
            "userId": "me",
            "q": query,
            "maxResults": min(500, max_total - fetched)
        }
        if next_page_token:
            kwargs["pageToken"] = next_page_token
        results = service.users().messages().list(**kwargs).execute()
        messages = results.get('messages', [])
        for msg in messages:
            msg_detail = service.users().messages().get(userId='me', id=msg['id'], format='full').execute()
            headers = msg_detail['payload']['headers']
            subject = next((h['value'] for h in headers if h['name'] == 'Subject'), '')
            sender = next((h['value'] for h in headers if h['name'] == 'From'), '')
            date = next((h['value'] for h in headers if h['name'] == 'Date'), '')
            # Extract body
            body = ""
            try:
                if 'parts' in msg_detail['payload']:
                    for part in msg_detail['payload']['parts']:
                        if part['mimeType'] == 'text/plain' and 'data' in part['body']:
                            body = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
                            break
                else:
                    body = base64.urlsafe_b64decode(msg_detail['payload']['body']['data']).decode('utf-8')
            except Exception as e:
                body = "(Unable to decode email body)"
            platform = detect_platform(sender)
            subject_trimmed = (subject or "")[:250]
            # Keep body short for privacy/cost; LinkedIn often puts the key info up top
            body_snippet = (body or "")[:1200]
            emails.append({
                'id': msg['id'],
                'subject': subject,
                'subject_trimmed': subject_trimmed,
                'from': sender,
                'date': date,
                'body': body,
                'body_snippet': body_snippet,
                'platform': platform,
            })
            fetched += 1
            if fetched >= max_total:
                break
        next_page_token = results.get("nextPageToken")
        if not next_page_token:
            break
    return emails

def extract_job_status_ollama(subject, body_snippet, platform):
    prompt = f"""
You are a filter and parser for job application emails.
First, decide if this email is about a job (application, interview, offer, rejection, recruiter outreach, etc.).
Exclude non-job topics (scholarships, rentals, therapy, promotions, roommate searches, etc.).
Prefer the subject for company/role if it looks clear; use the body snippet only if the subject is unclear.
Platform hint: {platform}.
Return JSON with:
{{
  "relevant": true/false,
  "reason": "short note",
  "jobs": [
    {{
      "company": "...",
      "job_title": "...",
      "status": "applied/interview/offer/rejected/unknown",
      "date": "optional"
    }}
  ],
  "error": ""
}}
If not job-related, set relevant=false and jobs=[].

Email subject (trimmed): {subject}
Email body snippet (trimmed): {body_snippet}

Only output the JSON object.
"""
    try:
        llm_start = time.time()
        result = subprocess.run(
            ['ollama', 'run', 'llama3', prompt],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            print(f"Ollama error {result.returncode}: {result.stderr[:500]}")
        response = result.stdout

        start = response.find('{')
        end = response.rfind('}') + 1
        json_text = response[start:end]

        try:
            data = json.loads(json_text)
        except Exception as e:
            print("Warning: Couldn't parse JSON from LLM response (payload suppressed).")
            data = {
                "relevant": False,
                "reason": "Parsing failed",
                "jobs": [],
                "error": str(e),
            }

        print(f"Ollama time: {time.time() - llm_start:.2f} seconds")
        return data
    except Exception as e:
        print("Warning: Could not parse JSON from LLM output:", str(e))
        return {
            "relevant": False,
            "reason": "Parsing failed",
            "jobs": [],
            "error": "Parsing failed",
        }

# Blacklist for obvious non-job junk
blacklist_keywords = load_blacklist()

def contains_blacklist_keywords(mail):
    text = (mail['subject'] + ' ' + mail['from']).lower()
    return any(word in text for word in blacklist_keywords)


# Cheap local filter to avoid sending obvious non-job emails to the LLM
job_like_keywords = [
    "job", "application", "applied", "interview", "offer",
    "position", "role", "career", "candidate", "hiring",
    "recruit", "recruiter", "opening"
]


def looks_job_related(mail):
    text = (mail.get("subject", "") + " " + mail.get("body", "")).lower()
    return any(word in text for word in job_like_keywords)


def to_iso_date(date_str):
    """Convert email date header to ISO string for sorting."""
    try:
        dt = parsedate_to_datetime(date_str)
        if dt is None:
            return ""
        # Normalize to naive UTC ISO for consistent sorting
        if dt.tzinfo:
            dt = dt.astimezone(tz=None)
        return dt.isoformat()
    except Exception:
        return ""


def process_email(mail, idx):
    try:
        llm_result = extract_job_status_ollama(
            mail.get('subject_trimmed', mail.get('subject', '')),
            mail.get('body_snippet', mail.get('body', '')),
            mail.get('platform', 'other'),
        )
        return (idx, mail, llm_result)
    except Exception as e:
        print(f"LLM error for email {idx}: {e}")
        return (idx, mail, {
            "jobs": [],
            "relevant": False,
            "reason": str(e), "error": str(e)
        })

def main():
    service = authenticate_gmail()
    conn = get_conn()

    emails = get_job_emails(service, max_total=50)  # Lowered for testing; increase as needed

    existing_ids = load_existing_ids(conn)

    emails_to_process = []
    skipped_dupe = 0
    skipped_blacklist = 0
    skipped_prefilter = 0
    for idx, mail in enumerate(emails, 1):
        if mail['id'] in existing_ids:
            skipped_dupe += 1
            continue
        if contains_blacklist_keywords(mail):
            skipped_blacklist += 1
            continue
        if not looks_job_related(mail):
            skipped_prefilter += 1
            continue
        emails_to_process.append((mail, idx))

    output_rows = []
    start_all = time.time()
    skipped_not_relevant = 0
    with ThreadPoolExecutor(max_workers=6) as executor:  # adjust concurrency
        future_to_idx = {executor.submit(process_email, mail, idx): idx for mail, idx in emails_to_process}
        for future in as_completed(future_to_idx):
            idx, mail, llm_result = future.result()
            if not llm_result.get("relevant", True):
                skipped_not_relevant += 1
                continue
            jobs = llm_result.get("jobs", [])
            if not isinstance(jobs, list):
                jobs = []
            # Backward-compat: if single fields came back, wrap them as one job
            if not jobs and any(llm_result.get(k) for k in ("company", "job_title", "status")):
                jobs = [{
                    "company": llm_result.get("company", ""),
                    "job_title": llm_result.get("job_title", ""),
                    "status": llm_result.get("status", ""),
                    "date": llm_result.get("date", ""),
                }]
            applications = []
            for job in jobs:
                applications.append(
                    {
                        "company": job.get("company", ""),
                        "job_title": job.get("job_title", ""),
                        "status": clean_status(job.get("status", "")),
                        "parsed_date": job.get("date", ""),
                        "reason": llm_result.get("reason", ""),
                        "error": llm_result.get("error", ""),
                    }
                )
            row = {
                "id": mail['id'],
                "email_num": idx,
                "subject": mail['subject'],
                "from": mail['from'],
                "date_email": mail['date'],
                "date_email_iso": to_iso_date(mail['date']),
                # Keep first job on the email row for compatibility; applications table is canonical
                "company": applications[0]["company"] if applications else "",
                "job_title": applications[0]["job_title"] if applications else "",
                "status": applications[0]["status"] if applications else "",
                "parsed_date": applications[0]["parsed_date"] if applications else "",
                "reason": llm_result.get("reason", ""),
                "error": llm_result.get("error", ""),
                "applications": applications,
            }
            output_rows.append(row)
            existing_ids.add(mail['id'])
            if idx % 10 == 0:
                print(f"Processed {idx} emails out of {len(emails_to_process)}")
    print(f"All LLM processing done in {time.time() - start_all:.2f} seconds.")

    if output_rows:
        save_rows(conn, output_rows)
        print(f"Saved {len(output_rows)} rows into {DB_PATH}")
    else:
        print("No new rows to save to the database.")

    print(
        f"Run summary: fetched {len(emails)}; "
        f"dupes skipped {skipped_dupe}; "
        f"blacklist skipped {skipped_blacklist}; "
        f"prefilter skipped {skipped_prefilter}; "
        f"LLM skipped {skipped_not_relevant}; "
        f"saved {len(output_rows)}"
    )
if __name__ == "__main__":
    while True:
        main()
        print("Waiting 5 minutes before next batch...")
        time.sleep(200)  # Sleep for 200 seconds (5 minutes)
