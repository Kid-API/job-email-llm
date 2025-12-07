FROM python:3.11-slim

WORKDIR /app

# Make Python output unbuffered and avoid .pyc files
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

# Flask runs on 5000 by default
EXPOSE 5000

CMD ["python", "app.py"]
