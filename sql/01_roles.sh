#!/bin/bash
# Runs once, on the first start of an empty database volume.
# Creates three least-privilege roles. Passwords come from .env via docker-compose.
set -euo pipefail

psql -v ON_ERROR_STOP=1 \
     -v loader_pw="${LOADER_DB_PASSWORD:?LOADER_DB_PASSWORD is not set}" \
     -v worker_pw="${WORKER_DB_PASSWORD:?WORKER_DB_PASSWORD is not set}" \
     -v app_pw="${APP_DB_PASSWORD:?APP_DB_PASSWORD is not set}" \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'EOSQL'
CREATE ROLE rule_loader LOGIN PASSWORD :'loader_pw';
CREATE ROLE embed_worker LOGIN PASSWORD :'worker_pw';
CREATE ROLE mcp_assistant LOGIN PASSWORD :'app_pw' NOINHERIT;
ALTER ROLE mcp_assistant SET statement_timeout = '2s';
GRANT CONNECT ON DATABASE bank_aidb TO rule_loader, embed_worker, mcp_assistant;
EOSQL