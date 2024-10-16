FROM python:3.13-slim

RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app/
COPY --chown=1069:1069 src/ .

CMD ["python", "main.py"]
