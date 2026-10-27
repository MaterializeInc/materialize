#!/bin/bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

set -euo pipefail

SQLCMD=/opt/mssql-tools18/bin/sqlcmd
if [ ! -x "$SQLCMD" ]; then
    SQLCMD=/opt/mssql-tools/bin/sqlcmd
fi

SEED_DIR=/tmp/mssql-seed
READY_QUERY='SET NOCOUNT ON; SELECT SUM(state) FROM sys.databases'

rm -rf "$SEED_DIR"
mkdir -p "$SEED_DIR"

/opt/mssql/bin/launch_sqlservr.sh /opt/mssql/bin/sqlservr &
pid="$!"

cleanup() {
    if kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid"
        wait "$pid" || true
    fi
}

trap cleanup EXIT

for _ in $(seq 1 180); do
    if db_status="$("$SQLCMD" -C -S localhost -U sa -P "$SA_PASSWORD" -h -1 -Q "$READY_QUERY" 2>/dev/null)"; then
        db_status="$(echo "$db_status" | tr -d '[:space:]')"
        if [ "$db_status" = "0" ]; then
            break
        fi
    fi
    sleep 1
done

if [ "${db_status:-}" != "0" ]; then
    echo "SQL Server did not become ready while seeding" >&2
    exit 1
fi

cleanup
trap - EXIT

cp -a /var/opt/mssql/. "$SEED_DIR"/
