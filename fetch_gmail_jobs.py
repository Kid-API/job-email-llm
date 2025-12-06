import os.path
import pickle
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import base64
import csv
import subprocess
import json
from status_utils import clean_status

# OAuth and Gmail API setup
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

def get_job_emails(service, query=None, max_results=20):
    # Refine this query as needed!
    if not query:
        query = "(subject:applied OR subject:application OR subject:interview OR subject:offer OR subject:rejected) after:2024/01/01"
    results = service.users().messages().list(userId='me', q=query, maxResults=max_results).execute()
    messages = results.get('messages', [])
    emails = []

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
            'subject': subject,
            'from': sender,
            'date': date,
            'body': body
        })
    return emails

def extract_job_status_ollama(email_text):
    prompt = f"""Parse this job application-related email and return a JSON with these fields: company, job_title, status (applied/interview/offer/rejection/other), date (if found).
    Email:
    {email_text}
    Only return the JSON object, no explanation."""
    result = subprocess.run(
        ['ollama', 'run', 'llama3', prompt],
        capture_output=True, text=True
    )
    # Try to load JSON safely even if there's extra text
    try:
        json_start = result.stdout.find('{')
        json_end = result.stdout.rfind('}') + 1
        data = json.loads(result.stdout[json_start:json_end])
        return data
    except Exception as e:
        print("Warning: Could not parse JSON from LLM output (payload suppressed):", str(e))
        return {"company": "", "job_title": "", "status": "", "date": "", "error": "Parsing failed"}

def main():
    service = authenticate_gmail()
    emails = get_job_emails(service, max_results=20)  # Change max_results as needed

    output_rows = []
    for idx, mail in enumerate(emails, 1):
        print(f"Processing email {idx} of {len(emails)}...")
        llm_result = extract_job_status_ollama(mail['body'])
        row = {
            "email_num": idx,
            "subject": mail['subject'],
            "from": mail['from'],
            "date_email": mail['date'],
            "company": llm_result.get("company", ""),
            "job_title": llm_result.get("job_title", ""),
            "status": clean_status(llm_result.get("status", "")),
            "parsed_date": llm_result.get("date", ""),
            "error": llm_result.get("error", "")
        }
        output_rows.append(row)

    # Write all results to CSV
    with open("parsed_job_apps.csv", "w", newline='') as csvfile:
        fieldnames = ["email_num", "subject", "from", "date_email", "company", "job_title", "status", "parsed_date", "error"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)
    print("Done! Results saved to parsed_job_apps.csv")

if __name__ == "__main__":
    main()
