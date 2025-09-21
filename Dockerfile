FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y build-essential ca-certificates && rm -rf /var/lib/apt/lists/*
COPY proxy/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY proxy /app
ENV PORT 8080
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:8080", "app:app", "--timeout", "120"]
