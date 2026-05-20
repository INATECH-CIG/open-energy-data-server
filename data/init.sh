#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<EOSQL

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE SCHEMA IF NOT EXISTS postgrest;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'readonly') THEN
    CREATE ROLE readonly
      LOGIN
      PASSWORD '${READONLY_PW}'
      NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION
      VALID UNTIL 'infinity';
  END IF;
END
$$;

GRANT pg_read_all_data TO readonly;

ALTER ROLE readonly SET search_path TO public;
ALTER ROLE opendata SET search_path TO public;

CREATE TABLE IF NOT EXISTS public.metadata (
  schema_name TEXT PRIMARY KEY,
  crawl_date DATE,
  data_date DATE,
  data_source TEXT,
  license TEXT,
  description TEXT,
  contact TEXT,
  tables INTEGER,
  size BIGINT,
  temporal_start TIMESTAMP,
  temporal_end TIMESTAMP,
  concave_hull_geometry GEOMETRY
);

CREATE OR REPLACE FUNCTION postgrest.pre_config()
RETURNS TABLE(key text, value text)
LANGUAGE sql
AS $$
  SELECT
    'pgrst.db_schemas',
    string_agg(nspname, ',')
  FROM pg_namespace
  WHERE nspname NOT LIKE 'pg_%'
    AND nspname NOT LIKE 'information_schema'
    AND nspname NOT LIKE '%timescaledb%';
$$;

NOTIFY pgrst, 'reload config';
NOTIFY pgrst, 'reload schema';

EOSQL