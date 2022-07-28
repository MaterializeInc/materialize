#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

set -e

cat >> "$PGDATA/postgresql.conf" <<-EOCONF
ssl = on
ssl_cert_file = '/share/secrets/postgres.crt'
ssl_key_file = '/share/secrets/postgres.key'
ssl_ca_file = '/share/secrets/ca.crt'
hba_file = '/share/conf/pg_hba.conf'
wal_level = logical
max_wal_senders = 20
max_replication_slots = 20
max_connections = 1000
EOCONF
