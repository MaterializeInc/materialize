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

# Wait for materialized to be ready before signaling setup_complete.
echo "Waiting for materialized to become healthy..."
until curl -sf http://materialized:6878/api/readyz > /dev/null 2>&1; do
    sleep 1
done
echo "materialized is healthy."

# Emit setup_complete — Antithesis begins test commands after this.
/usr/local/bin/setup-complete.sh

# Sleep forever — Test Composer runs the test commands, not this entrypoint.
echo "Setup complete. Sleeping while Test Composer runs commands."
exec sleep infinity
