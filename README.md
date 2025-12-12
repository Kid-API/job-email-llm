# Job Application Dashboard

A Flask app that parses job-related Gmail threads, uses Bedrock (Anthropic) to extract company/title/status, and shows a dashboard backed by SQLite.

## Prereqs
- Python 3.11+
- AWS CLI configured (for ECR/Bedrock)
- Docker (for image builds)
- Gmail OAuth files: `credentials.json` and `token.pickle` (keep out of git)
- Bedrock access (Anthropic Haiku/Sonnet)

## Local setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
python app.py   # http://127.0.0.1:5000
```
First run of `parse_gmail_jobs.py` will open a browser to create `token.pickle`.

## Docker
Build and run locally:
```bash
docker build -t job-apps:latest .
docker run --rm -p 5000:5000 \
  -v "$(pwd)/credentials.json:/app/credentials.json:ro" \
  -v "$(pwd)/token.pickle:/app/token.pickle" \
  -v "$(pwd)/jobs.db:/app/jobs.db" \
  job-apps:latest
```
Then open http://localhost:5000.

## Push to ECR (example)
```bash
REGION=us-east-2
ACCOUNT=934538657202
REPO=job-apps

docker build --platform linux/amd64 -t ${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com/${REPO}:latest .
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin ${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com
docker push ${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com/${REPO}:latest
```

## Run on EC2
On the instance:
```bash
aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 934538657202.dkr.ecr.us-east-2.amazonaws.com
docker pull 934538657202.dkr.ecr.us-east-2.amazonaws.com/job-apps:latest

docker stop job-apps 2>/dev/null && docker rm job-apps 2>/dev/null
docker run -d --name job-apps -p 80:5000 \
  -v "$HOME/credentials.json:/app/credentials.json:ro" \
  -v "$HOME/token.pickle:/app/token.pickle" \
  -v "$HOME/jobs.db:/app/jobs.db" \
  934538657202.dkr.ecr.us-east-2.amazonaws.com/job-apps:latest
```
Ensure the security group allows port 80 (or 5000 if you map that) from your IP.

## Gmail parsing
Run once inside the container:
```bash
docker exec job-apps python /app/parse_gmail_jobs.py
```
Scheduled (cron on EC2):
```
0 1 * * * /usr/bin/docker exec job-apps python /app/parse_gmail_jobs.py >> $HOME/job_app_cron.log 2>&1
```

## Data & secrets
- DB lives at `/home/ubuntu/jobs.db` on EC2 (mounted to `/app/jobs.db`).
- Do NOT commit `credentials.json`, `token.pickle`, `jobs.db`, `run.log`.

## Troubleshooting
- Check logs: `docker logs -f job-apps`
- Verify mounts: `docker exec job-apps ls -l /app/credentials.json /app/token.pickle /app/jobs.db`
- If “refused to connect”: confirm port mapping (`docker ps`), app binds to `0.0.0.0`, and SG allows the port.
