# Running Tests

You need running instances of the following data stores to run the full test suite:
- PostgreSQL
- Redshift
- BigQuery
- Snowflake

You will also need a Google Cloud Storage (GCS) bucket and an S3 bucket.

## Setup

Copy the `sample.env` file to `.env` and set the variables for your environment.

### PostgreSQL and Redshift
Create schemas named `source` and `target`. Load the `tests/data/leads_xs_*.csv` test data into a table named `source.leads_xs`

### BigQuery and Snowflake
Create schemas named `pontoon` and `target`. Load the same test data into `pontoon.leads_xs`

## Running

`make tests` or use `pytest` directly to run specific sets of tests.
