<div align="center">
    <img src="assets/logo-graphic-1-gradient.png" alt="Pontoon Logo" width="100" height="100"/>
    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
    <picture>
        <source media="(prefers-color-scheme: dark)" srcset="assets/logo-type-white.png">
        <img height="90" src="assets/logo-type-grey.png">
    </picture>
</div>

<h3 align="center">Build production ready data syncs that<br/>integrate with your customer's data warehouse.</h3>

<div align="center">
    <div>
        <a href="https://pontoon-data.github.io/Pontoon/"><strong>Docs</strong></a> ·
        <a href="https://pontoon-data.github.io/Pontoon/getting-started/quick-start/"><strong>Quick Start</strong></a> ·
        <a href="https://github.com/pontoon-data/Pontoon/issues"><strong>Report Bug</strong></a> ·
        <a href="https://getpontoon.com/contact"><strong>Contact</strong></a> ·
    </div>
    <br/>
    <p align="center">
        <a href="mailto:hello@getpontoon.com"><img src="https://img.shields.io/badge/Email%20Us-blue" /></a>
        <a href="https://github.com/pontoon-data/Pontoon/blob/master/LICENSE" target="_blank">
            <img src="https://img.shields.io/static/v1?label=license&message=MIT&color=blue" alt="License">
        </a>
        <a href="https://github.com/pontoon-data/Pontoon/blob/master/LICENSE" target="_blank">
            <img src="https://img.shields.io/static/v1?label=license&message=ELv2&color=blue" alt="License">
        </a>
    </p>
</div>

# About

Pontoon is an open source, self-hosted data export platform. Build data export features for your product without the hassle of moving data across cloud providers.

<div align="center" style="margin: 2em 0;">
  <img src="assets/pontoon-destinations.png" alt="Pontoon Destinations" width="980" />
</div>

## Key Features

- **🚀 Easy Deployment**: [Get started](https://pontoon-data.github.io/Pontoon/getting-started/quick-start/) in minutes with Docker or deploy to AWS Fargate
- **🎯 Major Data Warehouses Integrations**: Supports Snowflake, BigQuery, Redshift, and Postgres as sources and destinations
- **☁️ Multi-cloud**: Send data from any cloud to any cloud. Amazon Redshift ➡️ Google BigQuery? No problem!
- **⚡ Automated Syncs**: Schedule data transfers with automatic backfills and incremental loads
- **✨ Web Interface**: User-friendly dashboard for managing syncs, built with React/Nextjs
- **🔌 REST API**: Programmatic access to all Pontoon features, built with FastAPI

## Running Pontoon with Docker Compose

```sh
docker compose up --build
```

To view the Web UI: `localhost:3000`. To view the OpenAPI docs / test the API: `localhost:8000/docs`
