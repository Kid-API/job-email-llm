import os
import pickle
import base64
import csv
import subprocess
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

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

def get_job_emails(service, query=None, max_total=100):
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
                        if part['mimeType'] == 'text/plain' and 'data' in part['body']:
                            body = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
                            break
                else:
                    body = base64.urlsafe_b64decode(msg_detail['payload']['body']['data']).decode('utf-8')
            except Exception as e:
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

def extract_job_status_ollama(subject, body):
    prompt = f"""
You are a filter and parser for job application emails.
First, determine if this email is actually related to a *work/job application, interview, or job search communication*. 
EXCLUDE emails related to: scholarships, comedy, rentals (including rental applications, apartment hunting, housing, lease, sublet), sales, therapy, promotions, roommate searches, or anything not directly about paid employment.
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
        result = subprocess.run(
            ['ollama', 'run', 'llama3', prompt],
            capture_output=True, text=True
        )
        response = result.stdout

        # --- This is the ONLY JSON parsing code you need ---
        start = response.find('{')
        end = response.rfind('}') + 1
        json_text = response[start:end]

        try:
            data = json.loads(json_text)
        except Exception as e:
            print("Warning: Couldn't parse JSON! This is what Ollama sent back:")
            print(response)
            data = {"company": "", "job_title": "", "status": "", "date": "", "relevant": False, "reason": "Parsing failed", "error": str(e)}

        print(f"Ollama time: {time.time() - llm_start:.2f} seconds")
        return data
    except Exception as e:
        print("Warning: Could not parse JSON from LLM output:", str(e))
        return {"company": "", "job_title": "", "status": "", "date": "", "relevant": False, "reason": "Parsing failed", "error": "Parsing failed"}


start = response.find('{')
end = response.rfind('}') + 1
json_text = response[start:end]

try:
    data = json.loads(json_text)
except Exception as e:
    print("Warning: Couldn't parse JSON! This is what Ollama sent back:")
    print(response)
    data = {"company": "", "job_title": "", "status": "", "date": "", "relevant": False, "reason": "Parsing failed", "error": str(e)}

        print(f"Ollama time: {time.time() - llm_start:.2f} seconds")
        json_start = result.stdout.find('{')
        json_end = result.stdout.rfind('}') + 1
        data = json.loads(result.stdout[json_start:json_end])
        return data
    except Exception as e:
        print("Warning: Could not parse JSON from LLM output:", result.stdout)
        return {"company": "", "job_title": "", "status": "", "date": "", "relevant": False, "reason": "Parsing failed", "error": "Parsing failed"}

blacklist_keywords = ['match', 'matches', 'alerts', 'viewed', 'updates', 'canceled', 'reminder']

def contains_blacklist_keywords(mail):
    text = (mail['subject'] + ' ' + mail['body']).lower()
    return any(word in text for word in blacklist_keywords)

def process_email(mail, idx):
    try:
        llm_result = extract_job_status_ollama(mail['subject'], mail['body'])
        return (idx, mail, llm_result)
    except Exception as e:
        print(f"LLM error for email {idx}: {e}")
        return (idx, mail, {"company": "", "job_title": "", "status": "", "date": "", "relevant": False, "reason": str(e), "error": str(e)})

def main():
    service = authenticate_gmail()
    emails = get_job_emails(service, max_total=100)  # Bump up as needed

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
        if contains_blacklist_keywords(mail):
            print(f"Skipping email with blacklisted word: {mail['subject']}")
            continue
        emails_to_process.append((mail, idx))

    output_rows = []
    start_all = time.time()
    with ThreadPoolExecutor(max_workers=4) as executor:  # 24GB RAM = safe for 4!
        future_to_idx = {executor.submit(process_email, mail, idx): idx for mail, idx in emails_to_process}
        for future in as_completed(future_to_idx):
            idx, mail, llm_result = future.result()
            if not llm_result.get("relevant", True):
                print(f"Skipping non-job email (reason: {llm_result.get('reason', '')})")
                continue
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
            output_rows.append(row)
            existing_ids.add(mail['id'])
            if idx % 10 == 0:
                print(f"Processed {idx} emails out of {len(emails_to_process)}")
    print(f"All LLM processing done in {time.time() - start_all:.2f} seconds.")

    # Write all results to CSV
    with open(csv_file, "w", newline='') as csvfile:
        fieldnames = ["id", "email_num", "subject", "from", "date_email", "company", "job_title", "status", "parsed_date", "reason", "error"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)
    print("All emails processed! Results saved to parsed_job_apps.csv")

if __name__ == "__main__":
    main()
