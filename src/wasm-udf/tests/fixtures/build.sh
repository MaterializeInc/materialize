#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# build.sh — rebuilds udfs.wasm.gz from the guest crate in udfs/.
#
# The gzipped module is checked in so that tests do not need the wasm32-wasip1
# target. Rerun this after editing udfs/src/lib.rs.

set -euo pipefail

cd "$(dirname "$0")/udfs"
cargo build --release --target wasm32-wasip1
gzip -9 -n -c target/wasm32-wasip1/release/mz_wasm_udf_fixtures.wasm > ../udfs.wasm.gz
