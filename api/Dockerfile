# First stage: Build the transfer library
FROM python:3.12-slim AS transfer-builder

WORKDIR /transfer-build

COPY data-transfer/pontoon/pyproject.toml .
COPY data-transfer/pontoon/pontoon/ /transfer-build/pontoon/

RUN pip install build && python -m build --wheel

# Second stage: API image
FROM python:3.12-slim

ENV ENV=production

WORKDIR /api

# Needed for psycopg2
RUN apt-get update \
    && apt-get -y install libpq-dev gcc \
    && pip install psycopg2

# Copy and install the transfer library wheel
COPY --from=transfer-builder /transfer-build/dist/*.whl /api/dist/
RUN pip install --only-binary=:all: --no-cache-dir /api/dist/pontoon-*.whl

# Build the API app
COPY ./api/pyproject.toml /api/pyproject.toml
COPY ./api/app /api/app
COPY ./api/db /api/db
RUN pip install build && python -m build --wheel

RUN pip install --no-cache-dir /api/dist/app-*.whl
RUN pip install SQLAlchemy==2.0.41

EXPOSE 8000

CMD ["fastapi", "run", "app/main.py", "--port", "8000"]