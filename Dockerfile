ARG API_IMAGE=latest
ARG FRONTEND_IMAGE=latest

FROM ${API_IMAGE} AS pontoon-api
FROM ${FRONTEND_IMAGE} AS pontoon-frontend

FROM python:3.12-slim

EXPOSE 3000
EXPOSE 8000
EXPOSE 5432
EXPOSE 6379

ENV ENV=dev
ENV CELERY_BROKER_URL=redis://localhost:6379/0
ENV CELERY_RESULT_BACKEND=redis://localhost:6379/1
ENV PONTOON_API_ENDPOINT=http://localhost:8000
ENV ALLOW_ORIGIN=http://localhost:3000


WORKDIR /pontoon

# Install unified image dependencies
RUN apt-get update && \
    apt-get install -y \
    supervisor \
    redis-server \
    postgresql-17 \
    postgresql-client-17 \
    gcc \
    libpq-dev \
    curl \ 
    ca-certificates \
    gnupg && \
    curl -fsSL https://deb.nodesource.com/setup_18.x | bash - && \
    apt-get install -y nodejs && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


# Copy api and transfer library builds
COPY --from=pontoon-api /api/dist/*.whl ./dist/
RUN mkdir ./api


# Setup virtual envs for different services
RUN python -m venv api-venv
RUN python -m venv worker-venv
RUN python -m venv beat-venv


# Install API
RUN api-venv/bin/pip install dist/app-*.whl && \
    api-venv/bin/pip install dist/pontoon-*.whl && \
    api-venv/bin/pip install SQLAlchemy==2.0.41


# Install Celery worker(s)
RUN worker-venv/bin/pip install dist/pontoon-*.whl


# Install Red Beat 
RUN beat-venv/bin/pip install dist/pontoon-*.whl


# Install Frontend
ENV HOSTNAME="0.0.0.0"
ENV PORT=3000
ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1

RUN addgroup --system --gid 1001 nodejs
RUN adduser --system --uid 1001 nextjs
COPY --from=pontoon-frontend /app/public/ ./frontend/public
COPY --from=pontoon-frontend --chown=nextjs:nodejs /app ./frontend
COPY --from=pontoon-frontend --chown=nextjs:nodejs /app/.next/static ./frontend/.next/static


# Initialize postgres
ENV POSTGRES_HOST=localhost
ENV POSTGRES_PORT=5432
ENV POSTGRES_USER=dev
ENV POSTGRES_PASSWORD=dev
ENV POSTGRES_DATABASE=pontoon

ENV PATH="/usr/lib/postgresql/17/bin:$PATH"
RUN mkdir -p /var/lib/postgresql/data && \
    chown -R postgres:postgres /var/lib/postgresql
USER postgres
COPY --from=pontoon-api /api/db/migrations/V0001__initial_pontoon_schema.sql ./migrations/
RUN initdb -D /var/lib/postgresql/data
RUN echo "host all all all md5" >> /var/lib/postgresql/data/pg_hba.conf && \
    echo "listen_addresses='*'" >> /var/lib/postgresql/data/postgresql.conf
RUN pg_ctl -D /var/lib/postgresql/data -o "-c listen_addresses=''" -w start && \
    psql --username=postgres -c "CREATE USER $POSTGRES_USER WITH PASSWORD '$POSTGRES_PASSWORD';" && \
    psql --username=postgres -c "CREATE DATABASE $POSTGRES_DATABASE OWNER $POSTGRES_USER;" && \
    PGPASSWORD=$POSTGRES_PASSWORD psql --username=$POSTGRES_USER -d $POSTGRES_DATABASE -f ./migrations/V0001__initial_pontoon_schema.sql && \
    pg_ctl -D /var/lib/postgresql/data -m fast stop

USER root

# Initialize redis
COPY redis.conf /etc/redis/redis.conf
RUN mkdir /var/lib/redis/data

# Configure Supervisor
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
CMD ["/usr/bin/supervisord", "-n", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
