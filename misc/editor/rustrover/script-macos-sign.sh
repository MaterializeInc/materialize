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

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "Skipping codesign as macOS not detected."
  exit 0
fi

if [[ ! -f "./Cargo.lock" ]]; then
  echo "Please run this script from the root of the repository."
  exit 1
fi

for binary in ./target/debug/clusterd ./target/debug/environmentd; do
  echo "Signing $binary..."
  codesign -s - -f \
    --entitlements ./misc/editor/rustrover/macos-entitlements-data.xml \
    $binary
done
