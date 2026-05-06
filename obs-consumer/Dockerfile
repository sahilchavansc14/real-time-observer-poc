FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY obs_events_exporter.py .

EXPOSE 9108

CMD ["python", "-u", "obs_events_exporter.py"]
