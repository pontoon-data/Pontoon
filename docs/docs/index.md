# Welcome to Pontoon

## What is Pontoon?

Pontoon is an open source, self-hosted data export platform that is built from the ground up to help teams send data to their customers, at scale.

> 💡 Want to spin up Pontoon in just a few minutes? Try our [Quick Start Guide](getting-started/quick-start.md)!

### Key Features

- **🚀 Easy Deployment**: [Get started](getting-started/quick-start.md) in minutes with Docker or deploy to AWS Fargate
- **🎯 Major Data Warehouses Integrations**: Supports Snowflake, BigQuery, Redshift, and Postgres as sources and destinations
- **☁️ Multi-cloud**: Send data from any cloud to any cloud. Amazon Redshift ➡️ Google BigQuery? No problem!
- **🏗️ Built for Scale**: Sync over 1 million records per minutea
- **⚡ Automated Syncs**: Schedule data transfers with automatic backfills and incremental loads
- **✨ Web Interface**: User-friendly dashboard for managing syncs, built with React/Nextjs
- **🔌 REST API**: Programmatic access to all Pontoon features, built with FastAPI

### Supported Platforms

#### Sources

- **Data Warehouses**: Snowflake, Google BigQuery, Amazon Redshift
- **SQL Databases**: Postgres

#### Destinations

- **Data Warehouses**: Snowflake, Google BigQuery, Amazon Redshift
- **SQL Databases**: Postgres
- _Coming Soon - Object Storage_: Amazon S3, Google Cloud Storage

### The Problem with APIs & Data

We built Pontoon because traditional APIs are becoming increasingly problematic at modern data scale:

- **Performance Issues**: APIs struggle with large datasets and complex queries
- **Poor Customer Experience**: Customers have to spend weeks building ETLs or pay for managed ETL tools ($$$)
- **Rate Limits**: Data workloads tend to be bursty, often triggering rate limits, resulting in a frustrating experience for everyone involved
- **Backfills**: Backfilling historical data through APIs is inherently slow, as most APIs are optimized for real-time, not bulk ingestion

Pontoon solves these problems with:

- **Direct Warehouse Integration**: Send data directly to customer's data warehouse. No more ETLs needed!
- **Scalable Architecture**: Handle millions of rows efficiently. Say goodbye to rate limits!
- **Scheduled Syncs**: Automate data delivery with automatic backfills on the first sync
- **Self-Hosted**: Full control over your data and infrastructure

### Getting Started

Choose your deployment method:

1. **[Quick Start](getting-started/quick-start.md)** - Get running in minutes
2. **[Docker Compose](getting-started/docker-compose.md)** - Local or production deployment
3. **[AWS Fargate](getting-started/aws-fargate.md)** - Scalable cloud deployment
