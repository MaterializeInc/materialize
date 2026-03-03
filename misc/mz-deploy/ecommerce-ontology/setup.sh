#!/usr/bin/env bash
set -euo pipefail

MZ_HOST="${MZ_HOST:-localhost}"
MZ_PORT="${MZ_PORT:-6875}"
MZ_USER="${MZ_USER:-materialize}"

PSQL="psql -h $MZ_HOST -p $MZ_PORT -U $MZ_USER -v ON_ERROR_STOP=1"

echo "Creating clusters..."
$PSQL -c "CREATE CLUSTER ontology SIZE = '25cc'"
$PSQL -c "CREATE CLUSTER marketing SIZE = '25cc'"
$PSQL -c "CREATE CLUSTER finance SIZE = '25cc'"

echo "Creating databases..."
$PSQL -c "CREATE DATABASE raw"
$PSQL -c "CREATE DATABASE ontology"
$PSQL -c "CREATE DATABASE marketing"
$PSQL -c "CREATE DATABASE finance"

echo "Creating ontology.internal schema..."
$PSQL -d ontology -c "CREATE SCHEMA internal"

echo "Creating Postgres source..."
$PSQL -d raw -c "CREATE SECRET pgpass AS 'postgres'"
$PSQL -d raw -c "
CREATE CONNECTION pg_conn TO POSTGRES (
    HOST 'postgres',
    DATABASE 'postgres',
    USER 'postgres',
    PASSWORD SECRET pgpass
)"
$PSQL -d raw -c "
CREATE SOURCE pg_source
    FROM POSTGRES CONNECTION pg_conn (
        PUBLICATION 'mz_source'
    )"

echo "Setup complete."
