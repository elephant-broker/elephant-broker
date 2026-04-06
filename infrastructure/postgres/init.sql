-- ElephantBroker PostgreSQL init script
-- Runs once on first container boot (mounted via postgres service).
-- Creates dedicated database + user if they don't already exist.

-- The default POSTGRES_DB=elephantbroker already creates the database.
-- This script ensures the user has the correct privileges.
DO
$$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'elephantbroker') THEN
      CREATE ROLE elephantbroker WITH LOGIN PASSWORD 'elephantbroker';
   END IF;
END
$$;

GRANT ALL PRIVILEGES ON DATABASE elephantbroker TO elephantbroker;

-- Allow the role to create tables (Alembic needs this)
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO elephantbroker;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO elephantbroker;
GRANT USAGE ON SCHEMA public TO elephantbroker;
GRANT CREATE ON SCHEMA public TO elephantbroker;
