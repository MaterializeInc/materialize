#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

set -euo pipefail; shopt -s nullglob

if [[ ! -f "./Cargo.lock" ]]; then
  echo "Please run this script from the root of the repository."
  exit 1
fi

cmd="bin/environmentd --build-and-reset-only"
[[ "$(uname -s)" == "Darwin" ]] && cmd="$cmd --enable-mac-codesigning"
echo "Running $cmd..."
$cmd

echo "Reset environment-id to the value in the run configuration..."
echo local-az1-d205081d-4387-4daa-9f29-b70b1fc496e0-0 > ./mzdata/environment-id
