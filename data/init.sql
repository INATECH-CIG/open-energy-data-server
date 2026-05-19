#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS postgrest;
CREATE ROLE readonly WITH LOGIN PASSWORD '${READONLY_PW}' NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION VALID UNTIL 'infinity';
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

create or replace function postgrest.pre_config()
returns void as $$
select
set_config('pgrst.db_schemas', string_agg(nspname, ','), true)
from pg_namespace
where nspname not like '%timescaledb%'
and nspname not like '%information_schema%'
and nspname not like '%pg%';
$$ language sql;

NOTIFY pgrst, 'reload config';
NOTIFY pgrst, 'reload schema';
EOSQL