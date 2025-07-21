# Quick Start

To get Pontoon up and running in minutes, we packaged Pontoon into a single Docker container.

> 💡 **Note:** For production workloads, we recommend deploying with [Docker Compose](docker-compose.md) or [AWS Fargate](aws-fargate.md).

## Step 1: Requirements

- Install [Docker](https://www.docker.com/)

## Step 2: Run Pontoon

```bash
docker run \
    --name pontoon \
    -p 3000:3000 \
    -p 8000:8000 \
    --rm \
    --pull=always \
    -v pontoon-internal-postgres:/var/lib/postgresql/data \
    -v pontoon-internal-redis:/data \
    ghcr.io/pontoon-data/pontoon/pontoon-unified:latest
```

## Step 3: You're Ready! 🚀

Navigate to `http://localhost:3000` to start exploring Pontoon or checkout the API docs at `http://localhost:8000/docs`.

To learn about adding a source, check out the [Sources & Destinations documentation](../sources-destinations/overview.md).
