FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./
RUN uv sync --group platform --no-dev

COPY src/ src/

ENV PYTHONPATH=/app/src

CMD ["uvicorn", "llm_station.main:app", "--host", "0.0.0.0", "--port", "8000"]
