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

# We pass default arguments as environment variables, and only if those
# environment variables do not already exist, to allow users to override these
# arguments when running the container via either environment variables or
# command-line arguments.
export CLUSTERD_STORAGE_CONTROLLER_LISTEN_ADDR=${CLUSTERD_STORAGE_CONTROLLER_LISTEN_ADDR:-0.0.0.0:2100}
export CLUSTERD_COMPUTE_CONTROLLER_LISTEN_ADDR=${CLUSTERD_COMPUTE_CONTROLLER_LISTEN_ADDR:-0.0.0.0:2101}
export CLUSTERD_INTERNAL_HTTP_LISTEN_ADDR=${CLUSTERD_INTERNAL_HTTP_LISTEN_ADDR:-0.0.0.0:6878}
export CLUSTERD_SECRETS_READER=${CLUSTERD_SECRETS_READER:-process}
export CLUSTERD_SECRETS_READER_PROCESS_DIR=${CLUSTERD_SECRETS_READER_PROCESS_DIR:-/mzdata/secrets}

exec clusterd "$@"
