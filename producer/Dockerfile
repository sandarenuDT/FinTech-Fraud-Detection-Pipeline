FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Wait for Kafka to be ready then start producing
CMD ["python", "producer.py", "--rate", "1", "--duration", "0"]
