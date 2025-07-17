# Running Tests

You need running instances of the following data stores to run the full test suite:
- PostgreSQL
- Redshift
- BigQuery
- Snowflake

You will also need a Google Cloud Storage (GCS) bucket and an S3 bucket.

## Setup

Create a `.env` in this diretory and set the variables for your environment and resources:

```bash
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=

POSTGRES_HOST=
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DATABASE=

REDSHIFT_HOST=
REDSHIFT_USER=
REDSHIFT_PASSWORD=
REDSHIFT_DATABASE=
REDSHIFT_IAM_ROLE=
S3_BUCKET=

BQ_PROJECT_ID=
GCS_BUCKET=
GCP_SERVICE_ACCOUNT_FILE=

SNOWFLAKE_USER=
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=
SNOWFLAKE_SCHEMA=
SNOWFLAKE_ACCESS_TOKEN=

GLUE_IAM_ROLE=
```

### PostgreSQL and Redshift
Create schemas named `source` and `target`. Load the `tests/data/leads_xs_*.csv` test data into a table named `source.leads_xs`

### BigQuery and Snowflake
Create schemas named `pontoon` and `target`. Load the same test data into `pontoon.leads_xs`

## Running

`make tests` or use `pytest` directly to run specific sets of tests.
