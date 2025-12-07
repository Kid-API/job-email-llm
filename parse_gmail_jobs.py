import os
import pickle
import base64
import csv
import sqlite3
import threading
import json
import time
import boto3
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor, as_completed
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

DB_PATH = "jobs.db"
_db_lock = threading.Lock()

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("""
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
    """)
    return conn

def save_rows(conn, rows):
    with _db_lock, conn:
        conn.executemany("""
            INSERT INTO emails
            (id, email_num, subject, sender, date_email,
             company, job_title, status, parsed_date, reason, error)
            VALUES (:id, :email_num, :subject, :from, :date_email,
                    :company, :job_title, :status, :parsed_date, :reason, :error)
            ON CONFLICT(id) DO UPDATE SET
                subject=excluded.subject,
                sender=excluded.sender,
                company=excluded.company,
                job_title=excluded.job_title,
                status=excluded.status,
                parsed_date=excluded.parsed_date,
                reason=excluded.reason,
                error=excluded.error
        """, rows)

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

def get_job_emails(service, query=None, max_total=400):
    if not query:
        query = "(subject:applied OR subject:application OR subject:interview OR subject:rejected) after:2024/01/01"
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
                        if part.get('mimeType') == 'text/plain' and 'data' in part['body']:
                            body = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
                            break
                else:
                    body = base64.urlsafe_b64decode(msg_detail['payload']['body']['data']).decode('utf-8')
            except Exception:
                body = "(Unable to decode email body)"
            emails.append({
                'id': msg['id'],
                'subject': subject,
                'from': sender,
                'date': date,
                'body': body
            })
            fetched += 1
            if fetched >= max_total:
                break
        next_page_token = results.get("nextPageToken")
        if not next_page_token:
            break
    return emails

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


def extract_job_status_claude(subject, body):
    prompt = f"""
You are a filter and parser for job application emails.
First, determine if this email is actually related to a *work/job application, interview, or job search communication*. 
EXCLUDE emails related to: scholarships, comedy, rentals (including rental applications, apartment hunting, housing, lease, sublet), therapy, promotions, roommate searches, or anything not directly about paid employment.
Output a JSON object with:
- relevant: true or false
- reason: short explanation (e.g., 'job interview', 'rental application', 'not job-related')
- company: (if relevant)
- job_title: (if relevant)
- status: (applied/interview/offer/rejection/other, if relevant)
- date: (if relevant)
If the email is not job-related, set relevant to false and leave the rest blank.

Email subject: {subject}
Email body: {body}

Only output the JSON object.
"""
    try:
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
        print(f"Bedrock time: {time.time() - llm_start:.2f} seconds")
        return data
    except Exception as e:
        print("Warning: Could not parse JSON from LLM output:", str(e))
        return {"company": "", "job_title": "", "status": "", "date": "", "relevant": False, "reason": "Parsing failed", "error": "Parsing failed"}

def contains_blacklist_keywords(mail, blacklist_keywords):
    # Only filter on subject and sender
    text = (mail['subject'] + ' ' + mail['from']).lower()
    return any(word in text for word in blacklist_keywords)

def process_email(mail, idx, blacklist_keywords):
    # Returns a row dict or None if skipped
    if contains_blacklist_keywords(mail, blacklist_keywords):
        print(f"Skipping email with blacklisted keyword: {mail['subject']}")
        return None
    llm_result = extract_job_status_claude(mail['subject'], mail['body'])
    if not llm_result.get("relevant", True):
        print(f"Skipping non-job email (reason: {llm_result.get('reason', '')})")
        return None
    row = {
        "id": mail['id'],
        "email_num": idx,
        "subject": mail['subject'],
        "from": mail['from'],
        "date_email": mail['date'],
        "company": llm_result.get("company", ""),
        "job_title": llm_result.get("job_title", ""),
        "status": llm_result.get("status", ""),
        "parsed_date": llm_result.get("date", ""),
        "reason": llm_result.get("reason", ""),
        "error": llm_result.get("error", "")
    }
    return row

def main():
    blacklist_keywords = load_blacklist()
    service = authenticate_gmail()
    emails = get_job_emails(service, max_total=400)
    conn = get_conn()
    csv_file = "parsed_job_apps.csv"
    existing_ids = set()
    if os.path.exists(csv_file):
        with open(csv_file, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if 'id' in row:
                    existing_ids.add(row['id'])
    emails_to_process = []
    for idx, mail in enumerate(emails, 1):
        if mail['id'] in existing_ids:
            print(f"Skipping duplicate email with ID {mail['id']}")
            continue
        emails_to_process.append((mail, idx))
    output_rows = []
    start_all = time.time()
    with ThreadPoolExecutor(max_workers=8) as executor:  # 8 parallel LLM calls
        future_to_idx = {executor.submit(process_email, mail, idx, blacklist_keywords): idx for mail, idx in emails_to_process}
        for future in as_completed(future_to_idx):
            row = future.result()
            if row is not None:
                output_rows.append(row)
                existing_ids.add(row['id'])
            idx = future_to_idx[future]
            print(f"Processed email {idx} of {len(emails_to_process)}")
    print(f"All LLM processing done in {time.time() - start_all:.2f} seconds.")
    print("CWD:", os.getcwd())
    print("CSV full path:", os.path.abspath(csv_file))
    print("Rows to write:", len(output_rows))
    if len(output_rows) < 5:
        print("Sample output_rows:", output_rows[:5])

    if output_rows:
        save_rows(conn, output_rows)
        print(f"Saved {len(output_rows)} rows into {DB_PATH}")
    else:
        print("No new rows to save.")

if __name__ == "__main__":
    while True:
        main()
        print("Waiting 2 minutes before next batch...")
        time.sleep(120)
