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

. misc/shlib/shlib.bash

RESULT=0
{ stdbuf --output=L --error=L bin/xcompile cargo test --locked --profile=ci --doc |& tee run.log; } || RESULT=$?

if [[ $RESULT -ne 0 ]]; then
  bin/clear-corrupted-cargo-target-dir run.log
fi
