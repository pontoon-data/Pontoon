# Pontoon Data Transfer

## Setup:

### Rename the env files

Rename `api/app/sample.dev.env` to `api/app/dev.env`. Rename `web-app/package/sample.env.local` to `web-app/package/.env.development`.

## Running Pontoon with Docker Compose

```sh
docker compose up --build
```

To view the Web UI: `localhost:3000`. To view the OpenAPI docs / test the API: `localhost:8000/docs`

## Running Pontoon with Docker Image (unified)

Pontoon is packaged as a single docker image for ease of getting started. In production, we recommend deploying using docker compose.

```sh
# Builds base images
docker compose build

# Builds unified image
docker build -t pontoon:0.0.1 .

# Runs unified image
docker run -it --name pontoon --rm -p 3000:3000 -v pontoon-internal-postgres:/var/lib/postgresql/data -v redis-data:/data pontoon:0.0.1
```
