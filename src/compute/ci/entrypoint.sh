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

args=(--controller-listen-addr=0.0.0.0:2100 --internal-http-listen-addr=0.0.0.0:6878)

exec computed "${args[@]}" "$@"
