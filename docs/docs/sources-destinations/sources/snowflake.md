# Snowflake Source

Configure [Snowflake](https://www.snowflake.com/) as a source for your data transfers in Pontoon.

## Prerequisites

Before configuring Snowflake as a destination, ensure you have:

- **Snowflake Account**: Active Snowflake account with appropriate permissions
- **Warehouse**: Snowflake warehouse for compute resources
- **Schema**: Target schema within the database
- **User Credentials**: Username and password
- **Network Access**: Network connectivity to Snowflake

## Configuration

### Connection Details

| Parameter   | Description                  | Required | Example             |
| ----------- | ---------------------------- | -------- | ------------------- |
| `account`   | Snowflake account identifier | Yes      | `xy12345.us-east-1` |
| `warehouse` | Snowflake warehouse name     | Yes      | `PONTOON_WH`        |
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
CREATE SCHEMA RAW_DATA;
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
GRANT USAGE ON SCHEMA CUSTOMER_DATA.RAW_DATA TO ROLE PONTOON_ROLE;

-- Grant table creation permissions
GRANT CREATE TABLE ON SCHEMA CUSTOMER_DATA.RAW_DATA TO ROLE PONTOON_ROLE;

-- Grant data loading permissions
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA CUSTOMER_DATA.RAW_DATA TO ROLE PONTOON_ROLE;
GRANT INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA CUSTOMER_DATA.RAW_DATA TO ROLE PONTOON_ROLE;

-- Assign role to user
GRANT ROLE PONTOON_ROLE TO USER PONTOON_USER;
```

### Step 2: Configure Pontoon

1. Navigate to **Sources** → **New Source**
2. Select **Snowflake** as the source type
3. Enter connection details:
4. Click **Test Connection** to verify
5. Click **Save** to create the source
