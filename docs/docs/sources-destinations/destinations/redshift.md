# Amazon Redshift Destination

Configure [Amazon Redshift](https://aws.amazon.com/redshift/) as a destination for your data transfers in Pontoon. Data is loaded via S3 for improved performance.

## Prerequisites

Before configuring Redshift as a destination, ensure you have:

- **Recipient**: Recipient defined with the correct Tenant ID
- **AWS Account**: Active AWS account with Redshift access
- **Redshift Cluster**: Running Redshift cluster
- **Database**: Target database within the cluster
- **Schema**: Target schema within the database
- **User Credentials**: Database username and password
- **Network Access**: VPC connectivity or public access
- **S3 Bucket**: S3 bucket for staging data during transfer
- **IAM Role**: IAM role associated with Redshift cluster with permissions to load data from the S3 bucket

## How it works

The Redshift destination connector will perform the required DDL operations to replicate tables, transfer data by writing to an S3 location associated with the destination Redshift cluster, and run a `COPY` command to load the data.

## Configuration

### Connection Details

| Parameter               | Description                         | Required | Example                                              |
| ----------------------- | ----------------------------------- | -------- | ---------------------------------------------------- |
| `hostname`              | Redshift cluster endpoint           | Yes      | `my-cluster.abc123.us-east-1.redshift.amazonaws.com` |
| `port`                  | Redshift port                       | Yes      | `5439`                                               |
| `database`              | Target database name                | Yes      | `analytics`                                          |
| `user`                  | Database username                   | Yes      | `pontoon_user`                                       |
| `password`              | Database password                   | Yes      | `your_secure_password`                               |
| `target_schema`         | Target schema name                  | Yes      | `export`                                             |
| `s3_region`             | S3 region                           | Yes      | `us-east-1`                                          |
| `s3_bucket`             | S3 bucket for staging data          | Yes      | `s3://mybucket`                                      |
| `s3_prefix`             | S3 bucket prefix for data files     | Yes      | `/exports`                                           |
| `iam_role`              | IAM role ARN for Redshift-S3 access | Yes      | `arn:aws:iam::123456789012:role/RedshiftS3Role`      |
| `aws_access_key_id`     | AWS access key ID                   | Yes      | `AKIAIOSFODNN7EXAMPLE`                               |
| `aws_secret_access_key` | AWS secret access key               | Yes      | `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`           |

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
```

### Step 2: Configure AWS Resources

**Create S3 Bucket**: Create an S3 bucket for staging data during transfers

   ```bash
   aws s3 mb s3://your-pontoon-bucket --region us-east-1
   ```

**Create IAM Role**: Create an IAM role with the following permissions:

   ```json
   {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject"
      ],
      "Resource": "arn:aws:s3:::your-pontoon-bucket/*"
    }
   }
   ```
   - Attach this role to your Redshift cluster

**Create AWS Access Keys**: Create AWS access keys with permission to read and write to your S3 bucket. 


### Step 3: Configure Pontoon

1. Navigate to **Destinations** → **New Destination**
2. Select **Redshift** as the destination type
3. Enter connection details:
4. Click **Test Connection** to verify
5. Click **Save** to create the destination
