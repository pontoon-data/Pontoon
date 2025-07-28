-- Create user
CREATE USER pontoon_user WITH PASSWORD 'dest_pass';

-- Create schema
CREATE SCHEMA raw_data;

-- Grant permissions
GRANT CONNECT ON DATABASE destdb TO pontoon_user;
GRANT USAGE ON SCHEMA raw_data TO pontoon_user;
GRANT CREATE ON SCHEMA raw_data TO pontoon_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw_data TO pontoon_user;