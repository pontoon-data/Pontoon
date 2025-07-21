# Google BigQuery Source

Configure [Google BigQuery](https://cloud.google.com/bigquery) as a source for your data transfers in Pontoon.

## Prerequisites

Before configuring BigQuery as a source, ensure you have:

- **Google Cloud Project**: Active GCP project with BigQuery enabled
- **Service Account**: Service account with appropriate BigQuery permissions
- **Dataset**: Dataset for storing data
- **Credentials**: Service account JSON key file 
- **Network Access**: Network connectivity to BigQuery (VPC connector or public internet)

## Configuration

### Connection Details

| Parameter     | Description                 | Required | Example          |
| ------------- | --------------------------- | -------- | ---------------- |
| `project_id`  | Google Cloud project ID     | Yes      | `my-project-123` |
| `dataset`     | BigQuery dataset ID         | Yes      | `customer_data`  |
| `credentials` | Service account credentials | Yes      | JSON file      |

## Setup Instructions

### Step 1: Create BigQuery Resources

```bash
gcloud init # authenticate with a BigQuery admin role
bq mk --dataset --description "Pontoon Data" --location=US my-project-123:pontoon_data
```

### Step 2: Create a service account 

Create a service account with the `BigQuery Data Viewer` and `BigQuery Metadata Viewer` IAM roles attached and download the `service-account.json` key file.


### Step 3: Configure Pontoon

1. Navigate to **Sources** → **New Source**
2. Select **BigQuery** as the source type
3. Enter connection details:
4. Click **Test Connection** to verify
5. Click **Save** to create the source
