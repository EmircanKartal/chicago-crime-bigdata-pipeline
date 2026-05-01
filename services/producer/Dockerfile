FROM python:3.11-slim
WORKDIR /app

COPY services/producer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY services/producer/app/ /app/app/
COPY scripts/ /app/scripts/

CMD ["python", "app/producer.py"]