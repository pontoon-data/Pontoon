# Google BigQuery Destination

Configure [Google BigQuery](https://cloud.google.com/bigquery) as a destination for your data transfers in Pontoon.

## Prerequisites

Before configuring BigQuery as a destination, ensure you have:

- **Google Cloud Project**: Active GCP project with BigQuery enabled
- **Service Account**: Service account with appropriate BigQuery permissions
- **Dataset**: Target dataset for storing data
- **Credentials**: Service account key file or workload identity
- **Network Access**: Network connectivity to BigQuery (VPC connector or public internet)
- **GCS Bucket**: Google Cloud Storage bucket for staging data during transfer

## Configuration

### Connection Details

| Parameter         | Description                      | Required | Example          |
| ----------------- | -------------------------------- | -------- | ---------------- |
| `project_id`      | Google Cloud project ID          | Yes      | `my-project-123` |
| `dataset`         | BigQuery dataset ID              | Yes      | `customer_data`  |
| `target_schema`   | Target schema name               | Yes      | `export`         |
| `gcs_bucket`      | GCS bucket for staging data      | Yes      | `gs://mybucket`  |
| `gcs_prefix`      | GCS bucket prefix for data files | Yes      | `/exports`       |
| `service_account` | Service account credentials      | Yes      | JSON object      |

## Setup Instructions

### Step 1: Create BigQuery Resources

#### Create Dataset

```bash
# Create dataset
bq mk --location=US my-project-123:customer_data
```

#### Create Service Account

```bash
# Create service account
gcloud iam service-accounts create pontoon-sa \
  --display-name="Pontoon Service Account" \
  --project=my-project-123

# Get service account email
SA_EMAIL=$(gcloud iam service-accounts list \
  --filter="displayName:Pontoon Service Account" \
  --format="value(email)")
```

#### Assign BigQuery Permissions

```bash
# Grant BigQuery Data Editor role
gcloud projects add-iam-policy-binding my-project-123 \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/bigquery.dataEditor"

# Grant BigQuery Job User role
gcloud projects add-iam-policy-binding my-project-123 \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/bigquery.jobUser"

# Grant BigQuery User role
gcloud projects add-iam-policy-binding my-project-123 \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/bigquery.user"
```

#### Create Service Account Key

```bash
# Create and download service account key
gcloud iam service-accounts keys create pontoon-sa-key.json \
  --iam-account=$SA_EMAIL
```

### Step 2: Configure Google Cloud Storage

#### Create GCS Bucket

```bash
# Create GCS bucket for staging data
gsutil mb -l US gs://my-pontoon-bucket
```

#### Grant GCS Permissions

```bash
# Grant Storage Object Admin role to service account
gcloud projects add-iam-policy-binding my-project-123 \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/storage.objectAdmin"

# Grant Storage Object Viewer role to service account
gcloud projects add-iam-policy-binding my-project-123 \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/storage.objectViewer"
```

### Step 3: Configure Pontoon

1. Navigate to **Destinations** → **New Destination**
2. Select **BigQuery** as the destination type
3. Enter connection details:
4. Click **Test Connection** to verify
5. Click **Save** to create the destination
