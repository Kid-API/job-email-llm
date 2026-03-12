FROM python:3.11-slim

WORKDIR /app

# Make Python output unbuffered and avoid .pyc files
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

EXPOSE 5000
ENV FLASK_APP=app.py

HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:' + __import__('os').environ.get('PORT', '5000') + '/healthz')"

# Bind to 0.0.0.0 and honor PORT for cloud runtimes (Cloud Run/Render/Fly/etc.)
CMD ["sh", "-c", "flask run --host=0.0.0.0 --port=${PORT:-5000}"]
