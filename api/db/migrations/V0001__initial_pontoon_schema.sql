
-- enable uuid extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


-- using uuid4 primary keys 
-- timestamps are date + time in utc
-- no explicit foreign key constraints 
-- no triggers
-- no custom types


CREATE TABLE IF NOT EXISTS "organization" (
    organization_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    organization_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS "user" (
    user_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL,
    auth_token TEXT,
    organization_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS "source" (
    source_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_name TEXT NOT NULL,
    organization_id UUID NOT NULL,
    -- using check constraint instead of enum type:
    --      constraints are much easier to alter once in use
    --      expect valid values for source and destination vendor_type to differ,
    --      i.e would need two different enum types to model correctly
    vendor_type TEXT NOT NULL CHECK (vendor_type IN ('redshift', 'snowflake', 'bigquery', 'memory', 'postgresql')),
    connection_info JSONB NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('DRAFT', 'CREATED')),
    is_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    created_by UUID NOT NULL,
    modified_by UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);



CREATE TABLE IF NOT EXISTS "model" (
    model_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_id UUID NOT NULL,
    model_name TEXT NOT NULL,
    model_description TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    include_columns JSONB NOT NULL,
    primary_key_column TEXT NOT NULL,
    tenant_id_column TEXT NOT NULL,
    last_modified_at_column TEXT,
    created_by UUID NOT NULL,
    modified_by UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS "product" (
    product_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    product_name TEXT NOT NULL,
    organization_id UUID NOT NULL,
    models JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS "recipient" (
    recipient_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    recipient_name TEXT NOT NULL,
    organization_id UUID NOT NULL,
    tenant_id TEXT NOT NULL,
    created_by UUID NOT NULL,
    modified_by UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS "destination" (
    destination_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    destination_name TEXT NOT NULL,
    recipient_id UUID NOT NULL,
    primary_transfer_id UUID DEFAULT NULL,
    -- see notes on pontoon.source.vendor_type
    vendor_type TEXT NOT NULL CHECK (vendor_type IN ('redshift', 'snowflake', 'bigquery', 'console', 'postgresql')),
    schedule JSONB NOT NULL,
    models JSONB NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('DRAFT', 'CREATED')),
    is_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    connection_info JSONB NOT NULL,
    created_by UUID NOT NULL,
    modified_by UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- transfers represent pipeline infrastructure identified by transfer_id 
CREATE TABLE IF NOT EXISTS "transfer" (
    transfer_id UUID PRIMARY KEY,
    destination_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- fast lookups of transfer by destination_id
CREATE INDEX idx_transfer_destination_id_hash ON "transfer" USING hash(destination_id);


CREATE TABLE IF NOT EXISTS "transfer_run" (
    transfer_run_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    transfer_id UUID NOT NULL,
    meta JSONB NOT NULL,
    status TEXT CHECK (status in ('RUNNING', 'SUCCESS', 'FAILURE')),
    output JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- fast lookups of transfer runs by transfer_id
CREATE INDEX idx_transfer_id_hash ON "transfer_run" USING hash(transfer_id);


-- background/async API tasks 
CREATE TABLE IF NOT EXISTS "task" (
    task_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    meta JSONB NOT NULL,
    status TEXT CHECK (status in ('RUNNING', 'COMPLETE')),
    output JSONB,
    organization_id UUID NOT NULL,
    created_by UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_task_id_hash ON "task" USING hash(task_id);


-- special: memory source
INSERT INTO "source" (source_id, source_name, vendor_type, organization_id, connection_info, state, is_enabled, created_by, modified_by) VALUES (
    '8f491f6e-1883-43d4-8189-bc9db7276c43',
    'pontoon::memory',
    'memory',
    '22b251de-f9c3-55bc-8f57-b7ebd2e664e3',
    '{"vendor_type":"memory"}',
    'CREATED',
    TRUE,
    'b9020925-bd49-5160-8433-96aef951ca25',
    'b9020925-bd49-5160-8433-96aef951ca25'
);

-- special: memory-backed model
INSERT INTO "model" (model_id, source_id, model_name, model_description, schema_name, table_name, include_columns, primary_key_column, tenant_id_column, last_modified_at_column, created_by, modified_by) VALUES (
    'c871c3dd-0be2-45ae-b1a0-86a430095a32',
    '8f491f6e-1883-43d4-8189-bc9db7276c43',
    'Transfer Test',
    'Pontoon transfer test model (used for internal checks)',
    'pontoon',
    'pontoon_transfer_test',
    '[]',
    'id',
    'customer_id',
    'updated_at',
    'b9020925-bd49-5160-8433-96aef951ca25',
    'b9020925-bd49-5160-8433-96aef951ca25'
);

-- special: recipient for console destination
INSERT INTO "recipient" (recipient_id, recipient_name, organization_id, tenant_id, created_by, modified_by) VALUES (
    '83cd1b2d-5157-43b2-bcae-d828acb7bd07',
    'Pontoon, Inc.',
    '22b251de-f9c3-55bc-8f57-b7ebd2e664e3',
    'pontoon',
    'b9020925-bd49-5160-8433-96aef951ca25',
    'b9020925-bd49-5160-8433-96aef951ca25'
);

-- special: console destination
INSERT INTO "destination" (destination_id, destination_name, recipient_id, primary_transfer_id, vendor_type, schedule, models, state, is_enabled, connection_info, created_by, modified_by) VALUES (
    'df2267d6-fe65-43bb-b9ad-0c23b23a2c35',
    'pontoon::console',
    '83cd1b2d-5157-43b2-bcae-d828acb7bd07',
    NULL,
    'console',
    '{"type":"INCREMENTAL","frequency":"DAILY"}',
    '["c871c3dd-0be2-45ae-b1a0-86a430095a32"]',
    'CREATED',
    TRUE,
    '{"vendor_type":"console"}',
    'b9020925-bd49-5160-8433-96aef951ca25',
    'b9020925-bd49-5160-8433-96aef951ca25'
);
