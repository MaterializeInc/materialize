#!/usr/bin/env bash

# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

set -e

cat >> "$PGDATA/postgresql.conf" <<-EOCONF
ssl_cert_file = '/share/secrets/postgres.crt'
ssl_key_file = '/share/secrets/postgres.key'
ssl_ca_file = '/share/secrets/ca.crt'
EOCONF
