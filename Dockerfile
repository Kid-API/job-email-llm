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

# Bind to 0.0.0.0 so the container port is reachable from the host/EC2
CMD ["flask", "run", "--host=0.0.0.0", "--port=5000"]
