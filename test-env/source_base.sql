-- Create user
CREATE USER pontoon_user WITH PASSWORD 'test';

-- Create schema
CREATE SCHEMA pontoon_data;

-- Grant permissions
GRANT CONNECT ON DATABASE analytics TO pontoon_user;
GRANT USAGE ON SCHEMA pontoon_data TO pontoon_user;
GRANT SELECT ON ALL TABLES IN SCHEMA pontoon_data TO pontoon_user;

-- Optionally, grant future permissions
ALTER DEFAULT PRIVILEGES IN SCHEMA pontoon_data GRANT SELECT ON TABLES TO pontoon_user;

-- Table: leads
CREATE TABLE pontoon_data.leads (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    last_modified TIMESTAMP NOT NULL DEFAULT NOW(),
    lead_source VARCHAR(255) NOT NULL,       -- e.g. "Webinar", "Google Ads"
    campaign_id INTEGER NOT NULL,            -- Internal campaign identifier
    lifecycle_stage VARCHAR(100),            -- e.g. "MQL", "SQL", "Customer"
    lead_status VARCHAR(100),                -- e.g. "Open", "Qualified"
    lead_score INTEGER DEFAULT 0,            -- Behavioral or fit score
    is_converted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_leads_customer_id ON pontoon_data.leads(customer_id);
CREATE INDEX idx_leads_last_modified ON pontoon_data.leads(last_modified);

-- Table: campaigns
CREATE TABLE pontoon_data.campaigns (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    last_modified TIMESTAMP NOT NULL DEFAULT NOW(),
    name VARCHAR(255) NOT NULL,
    start_date DATE,
    end_date DATE,
    budget NUMERIC(12,2),
    channel VARCHAR(100), -- e.g., 'email', 'social', 'search'
    status VARCHAR(50),   -- e.g., 'active', 'paused', 'completed'
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_campaigns_customer_id ON pontoon_data.campaigns(customer_id);
CREATE INDEX idx_campaigns_last_modified ON pontoon_data.campaigns(last_modified);

-- Table: multitouch_attribution
CREATE TABLE pontoon_data.multitouch_attribution (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    last_modified TIMESTAMP NOT NULL DEFAULT NOW(),
    lead_id INTEGER NOT NULL,
    campaign_id INTEGER NOT NULL,
    touchpoint_order INTEGER NOT NULL, -- 1 = first touch, 2 = second, etc.
    touchpoint_time TIMESTAMP NOT NULL,
    attribution_model VARCHAR(100), -- e.g., 'linear', 'first_touch', 'last_touch'
    attribution_value NUMERIC(5,2), -- e.g., 0.25 for 25% credit
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_mta_customer_id ON pontoon_data.multitouch_attribution(customer_id);
CREATE INDEX idx_mta_last_modified ON pontoon_data.multitouch_attribution(last_modified);