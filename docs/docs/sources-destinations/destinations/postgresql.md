# PostgreSQL Destination

Configure [PostgreSQL](https://www.postgresql.org/) as a destination for your data transfers in Pontoon.

## Prerequisites

Before configuring PostgreSQL as a destination, ensure you have:

- **PostgreSQL Server**: Running PostgreSQL instance (local or cloud)
- **Database**: Target database for storing data
- **Schema**: Target schema within the database
- **User Credentials**: Database username and password
- **Network Access**: Network connectivity to PostgreSQL

## Configuration

### Connection Details

| Parameter  | Description             | Required | Example                |
| ---------- | ----------------------- | -------- | ---------------------- |
| `host`     | PostgreSQL host address | Yes      | `my-postgres-host.com` |
| `port`     | PostgreSQL port         | Yes      | `5432`                 |
| `database` | Target database name    | Yes      | `analytics`            |
| `schema`   | Target schema name      | Yes      | `raw_data`             |
| `user`     | Database username       | Yes      | `pontoon_user`         |
| `password` | Database password       | Yes      | `your_password`        |

## Setup Instructions

### Step 1: Create PostgreSQL Resources

#### Create Database and User

```sql
-- Create database
CREATE DATABASE analytics;

-- Create user
CREATE USER pontoon_user WITH PASSWORD 'your_secure_password';

-- Create schema
CREATE SCHEMA raw_data;

-- Grant permissions
GRANT CONNECT ON DATABASE analytics TO pontoon_user;
GRANT USAGE ON SCHEMA raw_data TO pontoon_user;
GRANT CREATE ON SCHEMA raw_data TO pontoon_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw_data TO pontoon_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA raw_data TO pontoon_user;

-- Grant future permissions
ALTER DEFAULT PRIVILEGES IN SCHEMA raw_data GRANT ALL ON TABLES TO pontoon_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw_data GRANT ALL ON SEQUENCES TO pontoon_user;
```

### Step 2: Configure Pontoon

1. Navigate to **Destinations** → **New Destination**
2. Select **PostgreSQL** as the destination type
3. Enter connection details:
4. Click **Test Connection** to verify
5. Click **Save** to create the destination
