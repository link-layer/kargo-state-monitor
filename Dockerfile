FROM python:3.13-slim

RUN useradd -m -u 1000 monitor

WORKDIR /app
RUN chown monitor:monitor /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY --chown=monitor:monitor pyproject.toml .
COPY --chown=monitor:monitor main.py .

USER monitor

RUN uv sync --no-dev --no-install-project

ENV PYTHONUNBUFFERED=1

CMD ["uv", "run", "main.py"]
