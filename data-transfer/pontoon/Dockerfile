# Use Python 3.12 slim as base image
FROM python:3.12-slim

# Set work directory
WORKDIR /transfer-library

# Needed for psycopg2
RUN apt-get update \
    && apt-get -y install libpq-dev gcc \
    && pip install psycopg2

# Copy pyproject.toml and README.md first for better caching
COPY pyproject.toml ./pyproject.toml

# Install the pontoon package and its dependencies
RUN pip install --no-cache-dir .

# Copy the pontoon source code
COPY pontoon/ ./pontoon/
