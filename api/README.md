# API using FastAPI

The Pontoon API is built with [FastAPI](https://fastapi.tiangolo.com/).

## Running the API

The easiest way to run the API is with docker compose from the root of the Pontoon directory.

To build only the API container

```sh
docker compose build api
```

To run only the API container

```sh
docker compose up api
```

## API Dependencies

The API is dependent on the data-transfer library in this repo. The docker container builds and packages the data-transfer library in the container automatically.

### SQLAlchemy Dependency Problem

The [sqlalchemy-redshift](https://github.com/sqlalchemy-redshift/sqlalchemy-redshift) library is still on SQLAlchemy 1.4, which causes dependency conflicts with sqlmodel (which is used by FastAPI). SQLAlchemy 2+ is installed and still works as expected.

## Database migrations

- Migrations are managed using [PGmigrate](https://github.com/yandex/pgmigrate)
- `make db-migrate-dev` will apply schema migrations up to current version in local dev db
- `make db-migrate` will apply migrations to the db specified in `db/migrations.yml` - intended for CI/prod use, credentials would be populated from secrets
- To add a new version migration, add a file `db/migrations/V<version>__<description>.sql` with the SQL you want to apply
  - Look at `python -m pgmigrate --help` and the commands in `Makefile` for how to inspect, dry-run and apply different versions
