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
- **IAM Role**: IAM role with permissions to access S3 and Redshift

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
CREATE SCHEMA export;

-- Grant permissions
GRANT USAGE ON SCHEMA export TO pontoon_user;
GRANT CREATE ON SCHEMA export TO pontoon_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA export TO pontoon_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA export TO pontoon_user;
```

### Step 2: Configure AWS Resources

1. **Create S3 Bucket**: Create an S3 bucket for staging data during transfers

   ```bash
   aws s3 mb s3://my-pontoon-bucket --region us-east-1
   ```

2. **Create IAM Role**: Create an IAM role with the following permissions:

   - S3 read/write access to your bucket
   - Redshift access for COPY commands

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": [
           "s3:GetObject",
           "s3:PutObject",
           "s3:DeleteObject",
           "s3:ListBucket"
         ],
         "Resource": [
           "arn:aws:s3:::my-pontoon-bucket",
           "arn:aws:s3:::my-pontoon-bucket/*"
         ]
       }
     ]
   }
   ```

3. **Attach IAM Role to Redshift**: Associate the IAM role with your Redshift cluster
   ```bash
   aws redshift modify-cluster-iam-roles \
     --cluster-identifier my-cluster \
     --add-iam-roles arn:aws:iam::123456789012:role/RedshiftS3Role
   ```

### Step 3: Configure Pontoon

1. Navigate to **Destinations** → **New Destination**
2. Select **Redshift** as the destination type
3. Enter connection details:
4. Click **Test Connection** to verify
5. Click **Save** to create the destination
