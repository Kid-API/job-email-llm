import os
import argparse
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
import re
from email.utils import parsedate_to_datetime
from botocore.config import Config
from botocore.exceptions import ClientError
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from status_utils import clean_reason, clean_status


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


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on", "y"}


BEDROCK_MIN_GAP_SECONDS = max(0.0, _env_float("BEDROCK_MIN_GAP_SECONDS", 10.0))
BEDROCK_MAX_ATTEMPTS = max(1, _env_int("BEDROCK_MAX_ATTEMPTS", 4))
BEDROCK_BACKOFF_BASE_SECONDS = max(1.0, _env_float("BEDROCK_BACKOFF_BASE_SECONDS", 6.0))
BEDROCK_MAX_OUTPUT_TOKENS = max(80, _env_int("BEDROCK_MAX_OUTPUT_TOKENS", 220))
BEDROCK_BODY_SNIPPET_CHARS = max(300, _env_int("BEDROCK_BODY_SNIPPET_CHARS", 700))
BEDROCK_ENABLE_SONNET = _env_bool("BEDROCK_ENABLE_SONNET", False)
BEDROCK_SONNET_TOKEN_THROTTLE_SWITCH = _env_bool(
    "BEDROCK_SONNET_TOKEN_THROTTLE_SWITCH", True
)
BEDROCK_MODEL_OVERRIDE = (os.getenv("BEDROCK_MODEL_OVERRIDE") or "").strip()
BEDROCK_AUTO_WAIT_ON_DAILY_QUOTA = _env_bool(
    "BEDROCK_AUTO_WAIT_ON_DAILY_QUOTA", True
)
BEDROCK_DAILY_QUOTA_WAIT_SECONDS = max(
    60, _env_int("BEDROCK_DAILY_QUOTA_WAIT_SECONDS", 1800)
)
BEDROCK_DAILY_QUOTA_MAX_WAIT_CYCLES = max(
    1, _env_int("BEDROCK_DAILY_QUOTA_MAX_WAIT_CYCLES", 48)
)
GMAIL_RESUME_OLDER_PAGES = _env_bool("GMAIL_RESUME_OLDER_PAGES", False)


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


def backfill_status_and_reason(conn):
    """
    Normalize existing application status/reason values in-place.

    This avoids a full Bedrock reparse when you only need cleaner labels.
    """
    rows = conn.execute(
        "SELECT id, status, reason FROM applications"
    ).fetchall()
    updates = []
    for row in rows:
        app_id = row[0]
        old_status = row[1] or ""
        old_reason = row[2] or ""
        new_status = clean_status(old_status)
        new_reason = clean_reason(old_reason, new_status)
        if old_status != new_status or old_reason != new_reason:
            updates.append((new_status, new_reason, app_id))
    if updates:
        with conn:
            conn.executemany(
                "UPDATE applications SET status = ?, reason = ? WHERE id = ?",
                updates,
            )
        print(f"Backfilled {len(updates)} application rows with normalized status/reason.")


def ensure_schema(conn):
    """Create tables and add missing columns if needed."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS emails (
            id TEXT PRIMARY KEY,
            email_num INTEGER,
            thread_id TEXT,
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
    if "thread_id" not in cols:
        conn.execute("ALTER TABLE emails ADD COLUMN thread_id TEXT")
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
            (id, email_num, thread_id, subject, sender, date_email, date_email_iso,
             company, job_title, status, parsed_date, reason, error)
            VALUES (:id, :email_num, :thread_id, :subject, :from, :date_email, :date_email_iso,
                    :company, :job_title, :status, :parsed_date, :reason, :error)
            ON CONFLICT(id) DO UPDATE SET
                thread_id=excluded.thread_id,
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
        query = "(subject:applied OR subject:application OR subject:interview OR subject:offer OR subject:follow OR subject:update OR subject:decision OR subject:role OR \"thank you\" OR \"move forward\") after:2024/03/20"
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
            body_snippet = (body or "")[:BEDROCK_BODY_SNIPPET_CHARS]
            emails.append({
                'id': msg['id'],
                'thread_id': msg_detail.get('threadId', ''),
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
_bedrock_rate_lock = threading.Lock()
_bedrock_next_allowed_at = 0.0
_bedrock_daily_quota_exhausted = False


def _wait_for_bedrock_slot():
    """Serialize Bedrock calls and ensure a minimum gap between invocations."""
    global _bedrock_next_allowed_at
    sleep_s = 0.0
    with _bedrock_rate_lock:
        now = time.monotonic()
        sleep_s = max(0.0, _bedrock_next_allowed_at - now)
        reserved_at = max(now, _bedrock_next_allowed_at)
        _bedrock_next_allowed_at = reserved_at + BEDROCK_MIN_GAP_SECONDS
    if sleep_s > 0:
        time.sleep(sleep_s)


def _extend_bedrock_backoff(attempt: int) -> float:
    """Push the next allowed Bedrock time forward with exponential backoff + jitter."""
    global _bedrock_next_allowed_at
    backoff = (BEDROCK_BACKOFF_BASE_SECONDS * (2 ** attempt)) + random.uniform(
        0, BEDROCK_BACKOFF_BASE_SECONDS
    )
    with _bedrock_rate_lock:
        _bedrock_next_allowed_at = max(_bedrock_next_allowed_at, time.monotonic() + backoff)
    return backoff


def _is_daily_quota_throttle(message: str) -> bool:
    """Detect account-level token caps that require waiting for quota reset."""
    msg = (message or "").lower()
    return "per day" in msg or "daily" in msg


def get_bedrock_client():
    """Create a Bedrock runtime client once, re-use it."""
    global _bedrock_client
    if _bedrock_client is None:
        region = os.getenv("AWS_REGION", "us-east-2")
        _bedrock_client = boto3.client(
            "bedrock-runtime",
            region_name=region,
            config=Config(retries={"max_attempts": 3, "mode": "standard"}),
        )
    return _bedrock_client


def extract_job_status_claude(subject, body_snippet, platform="other", sender="", force_model_id=None):
    global _bedrock_daily_quota_exhausted
    if _bedrock_daily_quota_exhausted:
        return {
            "relevant": False,
            "reason": "Bedrock daily token quota exhausted",
            "jobs": [],
            "error": "Daily token quota exhausted",
            "_stop_processing": True,
        }

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
    model_id = force_model_id or choose_model(subject, body_snippet, sender=sender, platform=platform)
    current_model_id = model_id
    print(f"[Bedrock] using model={current_model_id}")
    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": BEDROCK_MAX_OUTPUT_TOKENS,
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

    for attempt in range(BEDROCK_MAX_ATTEMPTS):
        try:
            _wait_for_bedrock_slot()
            response = client.invoke_model(
                modelId=current_model_id,
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
            print(f"[Bedrock] success model={current_model_id} time={elapsed:.2f}s")
            log_event("bedrock_success", model=current_model_id, elapsed_s=elapsed)
            if isinstance(data, dict):
                data["_model_id"] = current_model_id
            return data
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            msg = e.response.get("Error", {}).get("Message", "")
            req_id = e.response.get("ResponseMetadata", {}).get("RequestId", "")
            print(
                f"[Bedrock] attempt {attempt+1} failed: model={current_model_id} "
                f"code={code} msg={msg} req_id={req_id}"
            )
            log_event(
                "bedrock_error",
                attempt=attempt + 1,
                model=current_model_id,
                code=code,
                msg=msg,
                req_id=req_id,
            )
            is_throttle = code == "ThrottlingException"
            is_token_throttle = is_throttle and "token" in (msg or "").lower()
            is_daily_quota = is_token_throttle and _is_daily_quota_throttle(msg)
            if is_daily_quota:
                _bedrock_daily_quota_exhausted = True
                log_event(
                    "bedrock_daily_quota_exhausted",
                    model=current_model_id,
                    msg=msg,
                    req_id=req_id,
                )
                return {
                    "relevant": False,
                    "reason": "Bedrock daily token quota exhausted",
                    "jobs": [],
                    "error": "Daily token quota exhausted",
                    "_stop_processing": True,
                }
            if (
                is_token_throttle
                and current_model_id == SONNET_ID
                and BEDROCK_SONNET_TOKEN_THROTTLE_SWITCH
            ):
                current_model_id = HAIKU_ID
                print("[Bedrock] token throttle on Sonnet; switching to Haiku")
                log_event(
                    "bedrock_model_switch",
                    from_model=SONNET_ID,
                    to_model=HAIKU_ID,
                    reason="token_throttle",
                )
            if is_throttle and attempt < BEDROCK_MAX_ATTEMPTS - 1:
                backoff_s = _extend_bedrock_backoff(attempt)
                print(f"[Bedrock] throttled; backing off for {backoff_s:.1f}s")
                log_event(
                    "bedrock_backoff",
                    attempt=attempt + 1,
                    model=current_model_id,
                    backoff_s=round(backoff_s, 3),
                )
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

HAIKU_ID = "anthropic.claude-3-haiku-20240307-v1:0"
SONNET_ID = "anthropic.claude-3-sonnet-20240229-v1:0"

def choose_model(subject: str, body_snippet: str, sender: str = "", platform: str = "") -> str:
    """
    Prefer Haiku by default to avoid Bedrock token throttling.
    Sonnet is opt-in via BEDROCK_ENABLE_SONNET=1 for harder/noisy emails.
    """
    if BEDROCK_MODEL_OVERRIDE:
        return BEDROCK_MODEL_OVERRIDE
    if not BEDROCK_ENABLE_SONNET:
        return HAIKU_ID
    s = (sender or "").lower()
    if "linkedin.com" in s or "greenhouse.io" in s:
        return SONNET_ID
    if platform and platform.lower() in ("linkedin", "greenhouse"):
        return SONNET_ID
    subj = (subject or "").lower()
    body = (body_snippet or "").lower()
    text = subj + " " + body
    job_words = ("job", "application", "applied", "interview", "offer", "position", "role", "recruit", "hiring")
    has_job_word = any(w in text for w in job_words)
    is_very_long = len(body) > 1600
    has_many_lines = body.count("\n") > 20
    looks_weird = "unsubscribe" in text or "newsletter" in text
    low_signal = (not has_job_word) and len(subj) > 80
    # Sonnet only for high-complexity cases when explicitly enabled.
    return SONNET_ID if (is_very_long and (has_many_lines or low_signal or looks_weird)) else HAIKU_ID

def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip().lower()

def clean_job_title(raw_title: str, sender: str) -> str:
    """
    Strip out titles that are clearly the sender/your name or an email address.
    This avoids saving your own name (or the recruiter name) as the job title.
    """
    title = (raw_title or "").strip()
    if not title:
        return ""
    # Drop email-looking strings entirely.
    if "@" in title:
        return ""
    sender_text = sender or ""
    sender_name = sender_text.split("<")[0].replace('"', "").strip()
    sender_local = ""
    if "@" in sender_text:
        sender_local = sender_text.split("@")[0].split("<")[-1].strip()
    user_name = os.getenv("JOBAPPS_USER_NAME", "").strip()
    for candidate in (sender_name, sender_local, user_name):
        if candidate and _norm(candidate) == _norm(title):
            return ""
    return title

def clean_company(raw_company: str, sender: str) -> str:
    """Strip company values that are obviously just a person name or email."""
    company = (raw_company or "").strip()
    if not company:
        return ""
    if "@" in company:
        return ""
    sender_text = sender or ""
    sender_name = sender_text.split("<")[0].replace('"', "").strip()
    sender_local = ""
    if "@" in sender_text:
        sender_local = sender_text.split("@")[0].split("<")[-1].strip()
    user_name = os.getenv("JOBAPPS_USER_NAME", "").strip()
    for candidate in (sender_name, sender_local, user_name):
        if candidate and _norm(candidate) == _norm(company):
            return ""
    return company

def infer_title_from_subject(subject: str, company: str = "") -> str:
    """Try to backfill a title from the subject if the model returns blank."""
    subj = (subject or "").strip()
    if not subj:
        return ""
    # Remove company mention if present to reduce noise.
    if company:
        subj = re.sub(re.escape(company), "", subj, flags=re.IGNORECASE).strip(" |-:[]()")
    patterns = [
        r"interview for (.+)",
        r"application for (.+)",
        r"applied for (.+)",
        r"role:? (.+)",
        r"position:? (.+)",
    ]
    for pat in patterns:
        m = re.search(pat, subj, flags=re.IGNORECASE)
        if m:
            guess = m.group(1).strip(" |-:[]()\"'")
            if guess:
                return guess
    # Fallback: take the part before separators.
    for sep in ["|", "-", "–", ":"]:
        if sep in subj:
            chunk = subj.split(sep)[0].strip(" |-:[]()\"'")
            if chunk:
                return chunk
    return subj

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
            mail.get('from', ''),
        )
        # If the first pass (non-Sonnet) yielded only missing/unknown titles, retry with Sonnet
        def titles_missing(result):
            jobs = result.get("jobs") or []
            if not jobs:
                return True
            for j in jobs:
                title = (j.get("job_title") or "").strip().lower()
                if title and title != "unknown":
                    return False
            return True

        if (
            BEDROCK_ENABLE_SONNET
            and llm_result.get("_model_id") != SONNET_ID
            and titles_missing(llm_result)
        ):
            llm_result = extract_job_status_claude(
                mail.get('subject_trimmed', mail.get('subject', '')),
                mail.get('body_snippet', mail.get('body', '')),
                mail.get('platform', 'other'),
                mail.get('from', ''),
                force_model_id=SONNET_ID,
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
    global _bedrock_daily_quota_exhausted
    _bedrock_daily_quota_exhausted = False

    blacklist_keywords = load_blacklist()
    service = authenticate_gmail()
    conn = get_conn()
    backfill_status_and_reason(conn)
    print(
        "Bedrock config: "
        f"AWS_REGION={os.getenv('AWS_REGION', 'us-east-1')} "
        f"model_override={BEDROCK_MODEL_OVERRIDE or '(none)'} "
        f"enable_sonnet={BEDROCK_ENABLE_SONNET}"
    )

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

    # Keep fetch size aligned with the intended batch size.
    # By default we always start from page 1 so new emails are picked up every run.
    # Set GMAIL_RESUME_OLDER_PAGES=1 only when intentionally backfilling older pages.
    start_page_token = None
    token_path = "next_page_token.txt"
    if GMAIL_RESUME_OLDER_PAGES and os.path.exists(token_path):
        with open(token_path, "r") as f:
            start_page_token = f.read().strip() or None
        if start_page_token:
            print("Resuming from stored page token (older-page backfill mode).")
    elif os.path.exists(token_path):
        print("Ignoring stored page token to prioritize newest emails.")

    emails, new_page_token = get_job_emails(service, max_total=20, start_page_token=start_page_token)

    existing_ids = load_existing_ids(conn)

    emails_to_process = []
    checkpoint_id = None
    skipped_dupe = 0
    skipped_thread_replaced = 0
    skipped_blacklist = 0
    skipped_prefilter = 0
    threads_latest: dict[str, tuple[str, dict]] = {}
    no_thread_emails: list[tuple[int, dict]] = []
    for idx, mail in enumerate(emails, 1):
        # Stop if we reach the last processed ID from the previous run
        if last_id and mail['id'] == last_id:
            print("Reached last processed email. Stopping this batch.")
            break
        # Track the oldest newly-seen message so progress is saved even on parse failures.
        checkpoint_id = mail["id"]
        # Skip already-processed emails
        if mail['id'] in existing_ids:
            skipped_dupe += 1
            continue
        if contains_blacklist_keywords(mail, blacklist_keywords):
            skipped_blacklist += 1
            continue
        if not looks_job_related(mail):
            skipped_prefilter += 1
            continue
        thread_id = mail.get("thread_id", "")
        if thread_id:
            iso = to_iso_date(mail.get("date", ""))
            existing = threads_latest.get(thread_id)
            if existing:
                existing_iso = existing[0]
                if iso > existing_iso:
                    threads_latest[thread_id] = (iso, mail)
                    skipped_thread_replaced += 1
            else:
                threads_latest[thread_id] = (iso, mail)
        else:
            no_thread_emails.append((idx, mail))
    # Flatten chosen thread reps plus no-thread emails
    emails_to_process = [(m, idx) for idx, m in no_thread_emails]
    emails_to_process.extend([(pair[1], idx) for idx, pair in enumerate(threads_latest.values(), start=len(emails_to_process)+1)])
    output_rows = []
    start_all = time.time()
    skipped_not_relevant = 0
    error_counts = {}
    stopped_for_daily_quota = False
    for mail, idx in emails_to_process:
        idx, mail, llm_result = process_email(mail, idx)
        if llm_result.get("_stop_processing"):
            stopped_for_daily_quota = True
            print("Bedrock daily token quota hit; stopping this run without advancing progress markers.")
            key = llm_result.get("error") or "Daily token quota exhausted"
            error_counts[key] = error_counts.get(key, 0) + 1
            break
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
            cleaned_company = clean_company(job.get("company", ""), mail.get("from", ""))
            cleaned_title = clean_job_title(job.get("job_title", ""), mail.get("from", ""))
            if not cleaned_company or cleaned_company.lower() == "unknown":
                continue
            if not cleaned_title:
                inferred = infer_title_from_subject(mail.get("subject", ""), cleaned_company)
                cleaned_title = clean_job_title(inferred, mail.get("from", ""))
            review_note = ""
            if not cleaned_title:
                cleaned_title = "(unknown title - review)"
                review_note = "missing_title"
            parsed_date = job.get("date", "") or to_iso_date(mail.get("date", ""))
            reason_raw = llm_result.get("reason", "") or review_note
            if review_note and llm_result.get("reason"):
                reason_raw = f"{llm_result.get('reason')} | {review_note}"
            status_normalized = clean_status(job.get("status", ""))
            reason_normalized = clean_reason(reason_raw, status_normalized)
            applications.append(
                {
                    "company": cleaned_company,
                    "job_title": cleaned_title,
                    "status": status_normalized,
                    "parsed_date": parsed_date,
                    "reason": reason_normalized,
                    "error": llm_result.get("error", ""),
                }
            )
        key = llm_result.get("error") or "ok"
        error_counts[key] = error_counts.get(key, 0) + 1
        if not applications:
            continue
        row = {
            "id": mail['id'],
            "email_num": idx,
            "thread_id": mail.get("thread_id", ""),
            "subject": mail['subject'],
            "from": mail['from'],
            "date_email": mail['date'],
            "date_email_iso": to_iso_date(mail['date']),
            # Keep first job on the email row for compatibility; applications table is canonical
            "company": applications[0]["company"] if applications else "",
            "job_title": applications[0]["job_title"] if applications else "",
            "status": applications[0]["status"] if applications else "",
            "parsed_date": applications[0]["parsed_date"] if applications else "",
            "reason": applications[0]["reason"] if applications else "",
            "error": llm_result.get("error", ""),
            "applications": applications,
        }
        output_rows.append(row)
        existing_ids.add(mail['id'])
        # Pause slightly longer between emails to reduce throttling.
        time.sleep(6.0 + random.random() * 2.0)  # 6-8s pacing to ease throttling
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
    else:
        print("No new rows to save to the database.")

    if stopped_for_daily_quota:
        print("Kept last_processed_id.txt unchanged because quota was exhausted mid-run.")
    elif checkpoint_id:
        with open("last_processed_id.txt", "w") as f:
            f.write(checkpoint_id)
        print(f"Saved last processed email ID: {checkpoint_id}")
    elif last_id:
        print("No newer emails found; keeping existing last_processed_id.txt value.")

    # Persist the next page token (if any) so next run can resume deeper
    if stopped_for_daily_quota:
        token_to_save = start_page_token
    elif GMAIL_RESUME_OLDER_PAGES:
        token_to_save = new_page_token
    else:
        token_to_save = ""
    with open(token_path, "w") as f:
        f.write(token_to_save or "")
    if stopped_for_daily_quota and start_page_token:
        print("Kept existing page token so the same batch can resume after quota resets.")
    elif token_to_save:
        print("Saved next page token for next run.")
    elif not GMAIL_RESUME_OLDER_PAGES:
        print("Cleared page token (incremental newest-first mode).")
    else:
        print("No next page token; reached end of available pages for this query.")

    print("Error summary:", error_counts)
    log_event("run_summary", errors=error_counts)

    print(
        f"Run summary: fetched {len(emails)}; "
        f"dupes skipped {skipped_dupe}; "
        f"threads replaced {skipped_thread_replaced}; "
        f"blacklist skipped {skipped_blacklist}; "
        f"prefilter skipped {skipped_prefilter}; "
        f"LLM skipped {skipped_not_relevant}; "
        f"saved {len(output_rows)}"
    )
    return {
        "stopped_for_daily_quota": stopped_for_daily_quota,
        "saved_rows": len(output_rows),
    }


def run_with_auto_wait():
    """Optionally sleep-and-retry when daily token quota is exhausted."""
    wait_cycle = 0
    while True:
        result = main()
        if not result.get("stopped_for_daily_quota", False):
            break
        if not BEDROCK_AUTO_WAIT_ON_DAILY_QUOTA:
            print("Auto-wait disabled; exiting after daily quota exhaustion.")
            break
        wait_cycle += 1
        if wait_cycle > BEDROCK_DAILY_QUOTA_MAX_WAIT_CYCLES:
            print(
                "Reached max auto-wait cycles; exiting without more retries."
            )
            log_event(
                "bedrock_daily_quota_wait_limit_reached",
                max_cycles=BEDROCK_DAILY_QUOTA_MAX_WAIT_CYCLES,
            )
            break
        jitter = random.uniform(0, min(120, BEDROCK_DAILY_QUOTA_WAIT_SECONDS * 0.1))
        sleep_s = BEDROCK_DAILY_QUOTA_WAIT_SECONDS + jitter
        print(
            f"Daily quota exhausted. Auto-wait sleeping for {sleep_s:.0f}s "
            f"(cycle {wait_cycle}/{BEDROCK_DAILY_QUOTA_MAX_WAIT_CYCLES})."
        )
        log_event(
            "bedrock_daily_quota_wait",
            cycle=wait_cycle,
            sleep_s=round(sleep_s, 3),
            max_cycles=BEDROCK_DAILY_QUOTA_MAX_WAIT_CYCLES,
        )
        time.sleep(sleep_s)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse Gmail job emails into SQLite.")
    parser.add_argument(
        "--backfill-only",
        action="store_true",
        help="Normalize existing status/reason fields in SQLite without calling Gmail/Bedrock.",
    )
    args = parser.parse_args()

    if args.backfill_only:
        conn = get_conn()
        backfill_status_and_reason(conn)
        print("Backfill complete.")
    else:
        run_with_auto_wait()
