FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY selfiq ./selfiq
COPY dev ./dev
CMD ["uvicorn", "dev.backend:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--ws-max-size", "8192", "--no-access-log"]
