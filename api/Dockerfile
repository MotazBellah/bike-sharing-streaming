FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:0.6.14 /uv /usr/local/bin/uv

WORKDIR /app

# Dependencies resolve from uv.lock in their own layer, so editing application
# code does not invalidate the install.
COPY pyproject.toml uv.lock /app/
RUN uv sync --frozen --only-group api --no-install-project

ENV PATH="/app/.venv/bin:$PATH"

COPY api /app/api

# The API only ever reads from Redis; nothing here needs root.
RUN useradd --create-home --uid 1001 appuser && chown -R appuser /app
USER appuser

EXPOSE 8000
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
