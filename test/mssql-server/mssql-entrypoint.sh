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

SEED_DIR=/var/opt/mssql-seed
DATA_DIR=/var/opt/mssql/data

if [ ! -f "$DATA_DIR/master.mdf" ] && [ -d "$SEED_DIR" ]; then
    echo "Initializing SQL Server data directory from seed"
    cp -a "$SEED_DIR"/. /var/opt/mssql/
fi

exec /opt/mssql/bin/launch_sqlservr.sh "$@"
