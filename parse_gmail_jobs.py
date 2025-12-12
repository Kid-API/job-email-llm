import os
import pickle
import base64
import sqlite3
import threading
import json
import time
import boto3
import sys
import random
import logging
from email.utils import parsedate_to_datetime
from botocore.config import Config
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from status_utils import clean_status


# Paths and database setup
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)
DB_PATH = os.path.join(BASE_DIR, "jobs.db")
_db_lock = threading.Lock()

# Structured logging setup
logging.basicConfig(
    filename="run.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def log_event(event, **kwargs):
    """Write a JSON-style event to the log file."""
    try:
        logging.info(json.dumps({"event": event, **kwargs}))
    except Exception:
        logging.info({"event": event, **kwargs})


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


def get_job_emails(service, query=None, max_total=200, start_page_token=None):
    if not query:
        query = "(subject:applied OR subject:application OR subject:interview OR subject:rejected) after:2024/03/20"
    emails = []
    next_page_token = start_page_token  # optional resume token from prior run
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
                        if part.get('mimeType') == 'text/plain' and 'data' in part['body']:
                            body = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
                            break
                else:
                    body = base64.urlsafe_b64decode(msg_detail['payload']['body']['data']).decode('utf-8')
            except Exception:
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
    return emails, next_page_token

_bedrock_client = None


def get_bedrock_client():
    """Create a Bedrock runtime client once, re-use it."""
    global _bedrock_client
    if _bedrock_client is None:
        region = os.getenv("AWS_REGION", "us-east-1")
        _bedrock_client = boto3.client(
            "bedrock-runtime",
            region_name=region,
            config=Config(retries={"max_attempts": 3, "mode": "standard"}),
        )
    return _bedrock_client


def extract_job_status_claude(subject, body_snippet, platform="other"):
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

    llm_start = time.time()
    client = get_bedrock_client()
    model_id = os.getenv(
        "BEDROCK_MODEL_ID",
        "anthropic.claude-3-haiku-20240307-v1:0",
    )
    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 400,
        "temperature": 0,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt}
                ],
            }
        ],
    }

    for attempt in range(4):  # simple retry for throttling
        try:
            # Slightly longer pause to reduce throttling
            time.sleep(2.0 + random.random() * 1.0)
            response = client.invoke_model(
                modelId=model_id,
                body=json.dumps(payload).encode("utf-8"),
                contentType="application/json",
                accept="application/json",
            )
            raw_body = response["body"].read()
            model_json = json.loads(raw_body)
            content = model_json.get("content", [])
            text = ""
            if content and isinstance(content, list):
                first = content[0]
                text = first.get("text", "") if isinstance(first, dict) else ""

            start = text.find("{")
            end = text.rfind("}") + 1
            json_text = text[start:end] if start != -1 and end != 0 else text
            try:
                data = json.loads(json_text)
            except Exception as e:
                print("Warning: Couldn't parse JSON from LLM response (payload suppressed).")
                data = {"company": "", "job_title": "", "status": "", "date": "", "relevant": False, "reason": "Parsing failed", "error": str(e)}
            elapsed = time.time() - llm_start
            print(f"[Bedrock] success model={model_id} time={elapsed:.2f}s")
            log_event("bedrock_success", model=model_id, elapsed_s=elapsed)
            return data
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            msg = e.response.get("Error", {}).get("Message", "")
            req_id = e.response.get("ResponseMetadata", {}).get("RequestId", "")
            print(f"[Bedrock] attempt {attempt+1} failed: code={code} msg={msg} req_id={req_id}")
            log_event("bedrock_error", attempt=attempt + 1, code=code, msg=msg, req_id=req_id)
            if code == "ThrottlingException" and attempt < 3:
                # exponential-ish backoff with jitter
                time.sleep(2.0 * (attempt + 1) + random.random())
                continue
            break
        except Exception as e:
            print(f"[Bedrock] attempt {attempt+1} failed: {e}")
            log_event("bedrock_error", attempt=attempt + 1, error=str(e))
            break

    return {
        "relevant": False,
        "reason": "Parsing failed",
        "jobs": [],
        "error": "Parsing failed",
    }

def contains_blacklist_keywords(mail, blacklist_keywords):
    # Only filter on subject and sender
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
        if dt.tzinfo:
            dt = dt.astimezone(tz=None)
        return dt.isoformat()
    except Exception:
        return ""

def process_email(mail, idx):
    try:
        llm_result = extract_job_status_claude(
            mail.get('subject_trimmed', mail.get('subject', '')),
            mail.get('body_snippet', mail.get('body', '')),
            mail.get('platform', 'other'),
        )
        return (idx, mail, llm_result)
    except Exception as e:
        print(f"LLM error for email {idx}: {e}")
        return (
            idx,
            mail,
            {
                "jobs": [],
                "relevant": False,
                "reason": str(e),
                "error": str(e),
            },
        )



def main():
    blacklist_keywords = load_blacklist()
    service = authenticate_gmail()
    conn = get_conn()

    # Read the most recent processed ID (if any) so we can stop when we reach it
    last_id = None
    if os.path.exists("last_processed_id.txt"):
        with open("last_processed_id.txt", "r") as f:
            last_id = f.read().strip() or None
        if last_id:
            print(f"Last processed email ID: {last_id}")
        else:
            print("last_processed_id.txt is empty; will process all fetched emails.")
    else:
        print("No last_processed_id.txt found; will process all fetched emails.")

    # Keep fetch size aligned with the intended batch size; resume from stored page token if present
    start_page_token = None
    token_path = "next_page_token.txt"
    if os.path.exists(token_path):
        with open(token_path, "r") as f:
            start_page_token = f.read().strip() or None
        if start_page_token:
            print(f"Resuming from stored page token.")

    emails, new_page_token = get_job_emails(service, max_total=75, start_page_token=start_page_token)

    existing_ids = load_existing_ids(conn)

    emails_to_process = []
    skipped_dupe = 0
    skipped_blacklist = 0
    skipped_prefilter = 0
    for idx, mail in enumerate(emails, 1):
        # Stop if we reach the last processed ID from the previous run
        if last_id and mail['id'] == last_id:
            print("Reached last processed email. Stopping this batch.")
            break
        # Temporarily disable duplicate skip to reprocess all emails
       # if mail['id'] in existing_ids:
       #     skipped_dupe += 1
       #    continue
        if contains_blacklist_keywords(mail, blacklist_keywords):
            skipped_blacklist += 1
            continue
        if not looks_job_related(mail):
            skipped_prefilter += 1
            continue
        emails_to_process.append((mail, idx))
    output_rows = []
    start_all = time.time()
    skipped_not_relevant = 0
    error_counts = {}
    with ThreadPoolExecutor(max_workers=1) as executor:  # adjust concurrency
        future_to_idx = {executor.submit(process_email, mail, idx): idx for mail, idx in emails_to_process}
        for future in as_completed(future_to_idx):
            idx, mail, llm_result = future.result()
            if not llm_result.get("relevant", True):
                skipped_not_relevant += 1
                key = llm_result.get("error") or llm_result.get("reason") or "not_relevant"
                error_counts[key] = error_counts.get(key, 0) + 1
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
            key = llm_result.get("error") or "ok"
            error_counts[key] = error_counts.get(key, 0) + 1
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
            # Pause slightly between emails to reduce throttling
            time.sleep(1.5 + random.random() * 0.5)
            if idx % 10 == 0:
                print(f"Processed {idx} emails out of {len(emails_to_process)}")
    print(f"All LLM processing done in {time.time() - start_all:.2f} seconds.")
  #  print("CWD:", os.getcwd())
  #  print("Rows to write:", len(output_rows))
  #  if len(output_rows) < 5:
  #      print("Sample output_rows:", output_rows[:5])

    if output_rows:
        save_rows(conn, output_rows)
        print(f"Saved {len(output_rows)} rows into {DB_PATH}")
        # Persist the oldest email ID in this batch so next run skips down to it
        oldest_id = emails_to_process[-1][0]['id'] if emails_to_process else None
        if oldest_id:
            with open("last_processed_id.txt", "w") as f:
                f.write(oldest_id)
            print(f"Saved oldest processed email ID: {oldest_id}")
    else:
        print("No new rows to save to the database.")

    # Persist the next page token (if any) so next run can resume deeper
    with open(token_path, "w") as f:
        f.write(new_page_token or "")
    if new_page_token:
        print("Saved next page token for next run.")
    else:
        print("No next page token; reached end of available pages for this query.")

    print("Error summary:", error_counts)
    log_event("run_summary", errors=error_counts)

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
        time.sleep(300)
