# Pontoon

This repo is R&amp;D for our data transfer machinery.

### Overview
* Python library that provides data movement between source and destination data stores
* It provides two main abstractions, `Source` and `Destination` along with plumbing to move data between them
* The `pontoon.orchestration` modules provide abstractions for working with `Pipeline`s composed of different source and destination stores 

#### Sources
* Anything that SQLAlchemy can connect to:
    * Postgresql, MySQL, MSSQL, Oracle, SQLite
    * Redshift, BigQuery, Snowflake, Athena  

#### Destinations
* S3 (Parquet)
* GCS (Parquet)
* Snowflake Managed Storage (Parquet)
* Glue (via S3)
* Redshift (via SQL _or_ S3 copy)
* Big Query (via SQL _or_ GCS copy)
* Snowflake (via SQL _or_ Snowflake Storage copy)
* And anything that SQLAlchemy supports 
  * Caveat: the generic `destination-sql` only supports full load replication because there isn't an efficient, data store agnostic way to implement UPSERT
  * We can likely do something with temporary tables / joins / deletes / inserts but have not implemented that yet 



### Sources and Destinations
```python
from pontoon import get_source, get_destination

s3_config = {
    's3_bucket_name': '...',
    's3_bucket_path': '/my/data',
    's3_region': '...'
}

src = get_source(
    'sql-source', 
    config={
        'source': {'dsn': 'postgres://user:pass@host/db'},
        'streams': [{
            'schema': 'public',
            'table': 'events_sm',
            'query': 'SELECT * FROM events_sm where user_id=128765'
        }]
    }
)

s3_dest = get_destination(
    's3-destination',
    config={
        's3': s3_config,
        'batch_size': 1000000,
        'parquet': {
            'compression': 'SNAPPY'
        }
    }
)

glue_dest = get_destination(
    'glue-destination', 
    config={
        's3': s3_config,
        'glue_database': 'pontoon',
        'glue_iam_role': '...'
    }
)

redshift_dest = get_destination(
    'redshift-destination',
    config={
        's3': s3_config,
        'iam_role': '...',
        'destination': {
            'dsn': 'postgres://redshift:pass@endpoint/db'
        }
    }
)

ds = src.read()

# magic
s3_dest.write(ds)
glue_dest.write(ds)
redshift_dest.write(ds)

```

### Connection checks
```python
from pontoon import get_source 

db_source = get_source(
    'source-sql',
    config={
        'source': {'dsn': '<database connection config>' }
    }
)

success, msg = db_source.check_connection()
print(success)

```

### Managing pipelines
```python
from pontoon.orchestration import Transfer

# create a new transfer
t = Transfer.create(str(uuid.uuid4()))
transfer_id = t.uuid() # save this to the app db somewhere

# or work with an existing one...
t = Transfer(existing_uuid)

# t.exists() - does the infra for this pipe exist? 
# t.delete() - delete the infra for this pipe
# t.status() - current status (RUNNING/SCHEDULED)
# t.disable() / t.enable()
# manual_t = t.run() - create a manual run of this transfer
# manual_t.apply()   - schedule and run it now

t.schedule(Transfer.DAILY)
t.set_destination(destination_uuid)

t.apply()

```



### Features
* Pure Python 3.12 - can run scripts in Glue Python jobs, Python lambdas, etc.
* Uses [PyArrow](https://arrow.apache.org/docs/python/index.html) (Apache Arrow) for in-memory data management and reading/writing file formats
* Connectors based on SQLAlchemy support a lot of data stores out of the box 
* Full load and incremental sync modes

### Limitations
* Code isn't hardened, limited unit tests so far
* Designed for syncs up to ~100m records, i.e. ~10-15GB transfers
* Auth mechanisms are limited to what SQLAlchemy supports via connection strings
  * Eg, mostly username/password or service account credentials
  * No role based, account based, or rotation options yet
* No schema evolution yet  - coming soon
* No integrity checks yet - coming soon


### Concepts
* `Source`: represents a source data store 
* `Destination`: represents a destination data store
* `Namespace`: represents a top-level naming heirarchy, e.g. a database
* `Stream`: represents a data object with a schema, e.g. a table in most databases
* `Record`: represents a row of data 
* `Cache`: represents an intermediate storage container for records as they're read and written
* `Dataset`: represents a `Namespace` with one or more `Streams` and a `Cache` containing `Records` for the streams
* `Mode`: represents a data replication mode (full, incremental) and relevant parameters (start, end, period) 


### Replication modes
* `Sources` and `Destinations` accept a `Mode` configuration block
* The mode specifies the type of replication, ie. `FULL_REFRESH` or `INCREMENTAL`
* How the mode is implemented depends on the data store, but generally:
  * Target table is created if it doesn't exist (schema must match incoming records)
  * Full refresh:
    * All records in the target table are deleted and then all records from the source are loaded
  * Incremental:
    * Each stream must have a `primary_field` and `cursor_field` configured
    * `primary_field` is a unique primary key column
    * `cursor_field` is a column that determines when records were updated/modified
    * New or modified records from the source are merged (or upsert'ed) into the target table


### Setup
* Install Python 3.12 on your machine

Create a virtual environment:
```
python -m venv dev
source dev/bin/activate
```

Install a binary release:
```
$ (dev) pip install pontoon-1.0.0a0.tar.gz
$ (dev) python
$ (dev) >>> from pontoon.orchestration import Transfer 
```

Or to build locally, install the dev environment dependencies:
```
$ (dev) cd pontoon
$ (dev) pip install -r requirements.txt
```

Install the `pontoon` package in development mode:
```
$ (dev) cd pontoon
$ (dev) make install-dev 
```

Run the package tests:
```
$ (dev) cd pontoon
$ (dev) make tests
```

Production build (optional):
```
$ (dev) cd pontoon
$ (dev) make build  # creates dist/pontoon-<version>.tar.gz
```

Running example/test scripts:
* Install Postgresql locally - recommend *not* using Docker to avoid virtualization overheads, which impact I/O performance significantly for larger tests
* It doesn't really matter which RDBMS you choose as long as it's supported by SQLAlchemy
* This DB will be your source database for running tests and examples

Once you have a database running locally, generate and load the `leads` dataset:
```
$ (dev) cd datasets
$ (dev) python generate_leads_data.py leads.csv 100000000 10 2024-01-01 2024-01-10
```
This will generate 100m "leads" spread across 10 days and 10 different customers, about 1m 
leads per customer per day.

Generating the 100m record dataset will take ~30min and ~15GB of disk space. You can
generate a smaller version of the dataset if you'd like.

See the `datasets/leads.sql` for a `CREATE TABLE` statement you can use, and then `datasets/load_leads.sql` for `COPY` command you can modify to load your `leads.csv` into your local Postgres DB (that can take 30min+ for the full dataset) Run these from `psql` or Postico or something similar. 

You _can_ use a cloud warehouse as a source (instead of local Postgres) but it gets expensive leaving those running with data loaded, so I typically use a local source and spin up a destination as needed.

Create a `.env` file in the `examples/` directory using `examples/sample.env` as a starting place. The example scripts will load your `.env` into environment variables.

To run examples you may need to create the necessary cloud resources and data warehouses:
* Redshift
  * You'll want an S3 bucket
  * You'll want a serverless cluster, open to public, SG allowing 0.0.0.0 ingress, and a default IAM role associated that enables copy from the s3 bucket
  * Also probably want a super user (admin) account user + password
  * Create a schema named `pontoon` for examples
* BigQuery
  * You'll need a GCS bucket
  * Create a datatset named `pontoon` for examples, in the same region as the GCS bucket
  * Create a service account for the project and download the JSON key file
* Snowflake
  * You'll need an XS data warehouse
  * Create a `pontoon` database
  * Create a schema named `pontoon` in your database for examples 
  * Account admin user + password to connect

* The `examples/basic.py` script has blocks for most of the supported source and destinations -- you can uncomment blocks as desired and run the script to execute transfers 
