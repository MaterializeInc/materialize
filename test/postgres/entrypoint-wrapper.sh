#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Override baked-in TLS certificates with runtime certs from the test-certs
# container (shared via the secrets volume at /certs). This ensures postgres
# always uses the same CA that tests read from the test-certs container,
# eliminating mismatches when Docker images are rebuilt independently.

set -euo pipefail

if [ -f /certs/postgres.crt ]; then
    cp /certs/* /share/secrets/
    chown -R postgres:postgres /share/secrets
    chmod 600 /share/secrets/postgres.key
fi

exec docker-entrypoint.sh "$@"
