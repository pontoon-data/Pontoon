# Postgres Source

Configure [Postgres](https://www.postgresql.org/) as a source for your data transfers in Pontoon.

## Prerequisites

Before configuring Postgres as a destination, ensure you have:

- **Postgres Server**: Running Postgres instance (local or cloud)
- **Database**: Target database for storing data
- **User Credentials**: Database username and password
- **Network Access**: Network connectivity to Postgres

## Configuration

### Connection Details

| Parameter  | Description           | Required | Example                |
| ---------- | --------------------- | -------- | ---------------------- |
| `hostname` | Postgres host address | Yes      | `my-postgres-host.com` |
| `port`     | Postgres port         | Yes      | `5432`                 |
| `database` | Target database name  | Yes      | `analytics`            |
| `user`     | Database username     | Yes      | `pontoon_user`         |
| `password` | Database password     | Yes      | `your_password`        |

## Setup Instructions

### Step 1: Create PostgreSQL Resources

#### Create Database and User

```sql
-- Create database
CREATE DATABASE analytics;

-- Create user
CREATE USER pontoon_user WITH PASSWORD 'your_secure_password';

-- Create schema
CREATE SCHEMA pontoon_data;

-- Grant permissions
GRANT CONNECT ON DATABASE analytics TO pontoon_user;
GRANT USAGE ON SCHEMA pontoon_data TO pontoon_user;
GRANT SELECT ON ALL TABLES IN SCHEMA pontoon_data TO pontoon_user;

-- Optionally, grant future permissions
ALTER DEFAULT PRIVILEGES IN SCHEMA pontoon_data GRANT SELECT ON TABLES TO pontoon_user;
```

### Step 2: Configure Pontoon

1. Navigate to **Sources** → **New Source**
2. Select **Postgres** as the source type
3. Enter connection details:
4. Click **Test Connection** to verify
5. Click **Save** to create the source
