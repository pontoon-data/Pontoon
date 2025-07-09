# Amazon Redshift Source

Configure [Amazon Redshift](https://aws.amazon.com/redshift/) as a source for your data transfers in Pontoon.

## Prerequisites

Before configuring Redshift as a destination, ensure you have:

- **AWS Account**: Active AWS account with Redshift access
- **Redshift Cluster**: Running Redshift cluster
- **Database**: Target database within the cluster
- **User Credentials**: Database username and password
- **Network Access**: VPC connectivity or public access

## Configuration

### Connection Details

| Parameter  | Description               | Required | Example                                              |
| ---------- | ------------------------- | -------- | ---------------------------------------------------- |
| `hostname` | Redshift cluster endpoint | Yes      | `my-cluster.abc123.us-east-1.redshift.amazonaws.com` |
| `port`     | Redshift port             | Yes      | `5439`                                               |
| `database` | Target database name      | Yes      | `analytics`                                          |
| `user`     | Database username         | Yes      | `pontoon_user`                                       |
| `password` | Database password         | Yes      | `your_secure_password`                               |

## Setup Instructions

### Step 1: Create Database User

```sql
-- Create user for Pontoon
CREATE USER pontoon_user PASSWORD 'your_secure_password';

-- Create schema
CREATE SCHEMA raw_data;

-- Grant permissions
GRANT USAGE ON SCHEMA raw_data TO pontoon_user;
GRANT CREATE ON SCHEMA raw_data TO pontoon_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw_data TO pontoon_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw_data TO pontoon_user;
```

### Step 2: Configure Pontoon

1. Navigate to **Sources** → **New Source**
2. Select **Redshift** as the source type
3. Enter connection details:
4. Click **Test Connection** to verify
5. Click **Save** to create the source
