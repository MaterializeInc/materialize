#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Reference fallback that emits Antithesis's `setup_complete` lifecycle event
# to the SDK output file. The test-driver entrypoint emits the same event
# through `antithesis.lifecycle.setup_complete` (the canonical path); this
# script exists for any container that cannot link the Python SDK directly.
#
# Antithesis sets `ANTITHESIS_OUTPUT_DIR` automatically. Outside Antithesis,
# `ANTITHESIS_SDK_LOCAL_OUTPUT` may override the destination.

set -euo pipefail

OUTPUT_PATH="/tmp/antithesis_sdk.jsonl"
if [[ -n "${ANTITHESIS_OUTPUT_DIR:-}" ]]; then
  OUTPUT_PATH="${ANTITHESIS_OUTPUT_DIR}/sdk.jsonl"
  echo "Running in Antithesis, emitting setup_complete to ${OUTPUT_PATH}"
elif [[ -n "${ANTITHESIS_SDK_LOCAL_OUTPUT:-}" ]]; then
  OUTPUT_PATH="${ANTITHESIS_SDK_LOCAL_OUTPUT}"
  echo "Antithesis SDK local output override detected, emitting setup_complete to ${OUTPUT_PATH}"
fi

mkdir -p "$(dirname "$OUTPUT_PATH")"
echo '{"antithesis_setup":{"status":"complete","details":{"message":"ready to go"}}}' >> "${OUTPUT_PATH}"
