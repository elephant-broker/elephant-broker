FROM python:3.11-slim AS base

WORKDIR /app

COPY hitl_middleware/ hitl_middleware/
COPY pyproject.toml .
RUN pip install --no-cache-dir .

EXPOSE 8421

CMD ["python", "-m", "hitl_middleware"]
