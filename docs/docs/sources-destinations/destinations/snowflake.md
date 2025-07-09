# Snowflake Destination

Configure [Snowflake](https://www.snowflake.com/) as a destination for your data transfers in Pontoon.

## Prerequisites

Before configuring Snowflake as a destination, ensure you have:

- **Recipient**: Recipient defined with the correct Tenant ID
- **Snowflake Account**: Active Snowflake destination account with appropriate permissions
- **Warehouse**: Snowflake warehouse for compute resources
- **Database**: Target database within the cluster
- **Schema**: Target schema within the database
- **User Credentials**: Username and password
- **Network Access**: Network connectivity to Snowflake (IP whitelist or private link)

## Configuration

### Connection Details

| Parameter   | Description                  | Required | Example             |
| ----------- | ---------------------------- | -------- | ------------------- |
| `account`   | Snowflake account identifier | Yes      | `xy12345.us-east-1` |
| `warehouse` | Snowflake warehouse name     | Yes      | `PONTOON_WH`        |
| `schema`    | Target schema name           | Yes      | `EXPORT`            |
| `user`      | Snowflake username           | Yes      | `PONTOON_USER`      |
| `password`  | Snowflake password           | Yes      | `your_password`     |

## Setup Instructions

### Step 1: Create Snowflake Resources

#### Create Warehouse

```sql
-- Create a warehouse for Pontoon
CREATE WAREHOUSE PONTOON_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 1;
```

#### Create Database and Schema

```sql
-- Create database for customer data
CREATE DATABASE CUSTOMER_DATA;

-- Use the database
USE DATABASE CUSTOMER_DATA;

-- Create schema for raw data
CREATE SCHEMA EXPORT;
```

#### Create User and Role

```sql
-- Create role for Pontoon
CREATE ROLE PONTOON_ROLE;

-- Create user for Pontoon
CREATE USER PONTOON_USER
  PASSWORD = 'your_secure_password'
  DEFAULT_ROLE = PONTOON_ROLE
  DEFAULT_WAREHOUSE = PONTOON_WH;

-- Grant warehouse usage
GRANT USAGE ON WAREHOUSE PONTOON_WH TO ROLE PONTOON_ROLE;

-- Grant database and schema permissions
GRANT USAGE ON DATABASE CUSTOMER_DATA TO ROLE PONTOON_ROLE;
GRANT USAGE ON SCHEMA CUSTOMER_DATA.EXPORT TO ROLE PONTOON_ROLE;

-- Grant table creation permissions
GRANT CREATE TABLE ON SCHEMA CUSTOMER_DATA.EXPORT TO ROLE PONTOON_ROLE;

-- Grant data loading permissions
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA CUSTOMER_DATA.EXPORT TO ROLE PONTOON_ROLE;
GRANT INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA CUSTOMER_DATA.EXPORT TO ROLE PONTOON_ROLE;

-- Assign role to user
GRANT ROLE PONTOON_ROLE TO USER PONTOON_USER;
```

### Step 2: Configure Pontoon

1. Navigate to **Destinations** → **New Destination**
2. Select **Snowflake** as the destination type
3. Enter connection details:
4. Click **Test Connection** to verify
5. Click **Save** to create the destination
