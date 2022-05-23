#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

set -euo pipefail

cat >> "$PGDATA/postgresql.conf" <<-EOCONF
ssl = on
ssl_cert_file = '/share/secrets/postgres.crt'
ssl_key_file = '/share/secrets/postgres.key'
ssl_ca_file = '/share/secrets/ca.crt'
hba_file = '/share/conf/pg_hba.conf'
wal_level = logical
max_wal_senders = 20
max_replication_slots = 20
EOCONF

pg_ctlcluster 14 materialize stop
pg_ctlcluster 14 materialize start

psql -Atc "CREATE SCHEMA IF NOT EXISTS consensus"
psql -Atc "CREATE SCHEMA IF NOT EXISTS catalog"

exec materialized \
    --listen-addr=0.0.0.0:6875 \
    --persist-consensus-url=postgresql://materialize@%2Ftmp?options=--search_path=consensus \
    --catalog-postgres-stash=postgresql://materialize@%2Ftmp?options=--search_path=catalog \
    "$@"
