# Transfer Quick Start

Here's how to quickly set up your first transfer in Pontoon. Follow these steps to get your data flowing from source to destination.

## Step 1: Adding a Source

A source defines a connection to a database that holds data you want to sync. For detailed configuration instructions for each source type, see our [Sources & Destinations documentation](../sources-destinations/overview.md).

<!-- TODO: Add screenshot of adding a source form -->

To add a source:

1. Navigate to the Sources page in the Pontoon web interface
2. Click "Add New Source"
3. Select your source type (Snowflake, Redshift, BigQuery, etc.)
4. Configure the connection details for your data source
5. Test the connection to ensure it's working properly

## Step 2: Adding a Model

A model defines a dataset that is ready for export to recipients. Models are multi-tenant, with the `tenant_id` defining which row belongs to which customer.

### Important Model Fields

When creating a model, you'll need to configure three critical fields:

- **Primary Key**: A unique identifier for every row in your dataset. This field must be unique across all rows in a table.

- **Last Modified Key**: A timestamp field that indicates when each row was last updated. Pontoon uses this field to determine which rows need to be updated during future syncs.

- **Tenant ID**: An identifier used to associate data with specific customers. This same tenant ID will be used when adding recipients, creating the link between your data and who receives it.

## Step 3: Adding a Recipient

Recipients are the customers or organizations that will receive your data. Each recipient is associated with a specific tenant ID.

**Important**: The tenant ID you specify for a recipient must match the tenant ID used in any models that you want to send to this recipient. This creates the connection between your data models and the intended recipients.

To add a recipient:

1. Navigate to the Recipients page
2. Click "Add New Recipient"
3. Enter the recipient's details including their unique tenant ID
4. Save the recipient configuration

## Step 4: Adding a Destination

A destination defines where your data will be sent. This could be a data warehouse or another database.

<!-- TODO: Add screenshot of adding a destination form -->

For detailed configuration instructions for each destination type, see our [Sources & Destinations documentation](../sources-destinations/overview.md).

To add a destination:

1. Navigate to the Destinations page
2. Click "Add New Destination"
3. Select your destination type (Snowflake, Redshift, BigQuery, etc.)
4. Add a recipient
5. Configure the connection details
6. Test the connection to ensure it's working properly

## Step 5: Begin a Transfer

Once you have configured add a destination, it will kick off an initial sync to backfill data. Click on a destination and navigate to the transfers tab to see the status of the transfer

## Next Steps

Congrats, you've added your first destination! Some things to try include

- Add more models to share additional datasets
- Add more recipients / destinations to share data with additional customers
