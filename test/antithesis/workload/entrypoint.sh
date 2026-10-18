#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Brings up the environment and signals setup_complete, then keeps the
# container alive so Antithesis can run the test commands in it. A failed setup
# exits non-zero and Kubernetes restarts the pod, which retries it.

set -euo pipefail

python3 -m materialize.antithesis.setup
exec sleep infinity
