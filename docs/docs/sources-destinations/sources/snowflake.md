# Snowflake Source

Configure [Snowflake](https://www.snowflake.com/) as a source for your data transfers in Pontoon.

## Prerequisites

Before configuring Snowflake as a destination, ensure you have:

- **Snowflake Account**: Active Snowflake account with appropriate permissions
- **Warehouse**: Snowflake warehouse for compute resources
- **Database**: Database to store Pontoon data
- **Schema**: Target schema within the database
- **User Credentials**: Username and password
- **Network Access**: Network connectivity to Snowflake

## Configuration

### Connection Details

| Parameter   | Description                  | Required | Example             |
| ----------- | ---------------------------- | -------- | ------------------- |
| `account`   | Snowflake account identifier | Yes      | `xy12345.us-east-1` |
| `database`   | Snowflake database name | Yes      | `pontoon` |
| `warehouse` | Snowflake warehouse name     | Yes      | `PONTOON_WH`        |
| `user`      | Snowflake username           | Yes      | `PONTOON_USER`      |
| `access_token`  | Snowflake access token           | Yes      | `af877f76...`     |

## Setup Instructions

### Step 1: Create Snowflake Resources

#### Create Database and Schema

```sql
-- Optionally, create database for customer data
CREATE DATABASE PONTOON;

-- Use the database
USE DATABASE PONTOON;

-- Create schema for raw data
CREATE SCHEMA PONTOON_DATA;
```

#### Create User and Role

```sql
-- Create role for Pontoon
CREATE ROLE PONTOON_ROLE;

-- Create user for Pontoon
CREATE USER PONTOON_USER
  PASSWORD = 'your_secure_password'
  DEFAULT_ROLE = PONTOON_ROLE
  DEFAULT_WAREHOUSE = <YOUR_WAREHOUSE>;

-- Grant database and schema permissions
GRANT USAGE ON DATABASE PONTOON TO ROLE PONTOON_ROLE;
GRANT USAGE ON SCHEMA PONTOON.PONTOON_DATA TO ROLE PONTOON_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA PONTOON.PONTOON_DATA TO ROLE PONTOON_ROLE;

-- Assign role to user
GRANT ROLE PONTOON_ROLE TO USER PONTOON_USER;
```

### Step 2: Configure access token
Create a Snowflake [Access Token](https://docs.snowflake.com/en/user-guide/programmatic-access-tokens) (**Admin > Users & Roles > Programmatic access tokens**) for `PONTOON_USER` and ensure you have a Snowflake [Network Policy](https://docs.snowflake.com/en/user-guide/network-policies) in place that allows `PONTOON_USER` to access your Snowflake instance using the access token.

### Step 2: Configure Pontoon

1. Navigate to **Sources** → **New Source**
2. Select **Snowflake** as the source type
3. Enter connection details:
4. Click **Test Connection** to verify
5. Click **Save** to create the source
