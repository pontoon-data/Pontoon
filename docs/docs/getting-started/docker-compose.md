# Docker Compose Deployment

Deploy Pontoon using Docker Compose for production environments with full control over your infrastructure.

## Prerequisites

- **Docker** & **Docker Compose V2** ([Install](https://docs.docker.com/compose/install/))

## Step 1: Clone the Repository

```bash
git clone https://github.com/pontoon-data/Pontoon.git
```

## Step 2: Environment Configuration (Optional)

Update `.env` with any changes.

If you are using a Postgres and/or Redis database that is external to Pontoon, update the relevant values in `.env`.

> 💡 **Note:** For production workloads, we recommend using external Postgres and Redis databases.

## Step 3: Build and Run Pontoon

```bash
docker compose up --build
```

## Step 4: You're Ready! 🚀

Navigate to `http://localhost:3000` to start exploring Pontoon or checkout the API docs at `http://localhost:8000/docs`.

To learn about adding a source, check out the [Sources & Destinations documentation](../sources-destinations/overview.md).
