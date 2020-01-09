#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# slt-fast-asan.sh — runs fast subset of sqllogictests under AddressSanitizer.

set -euo pipefail

export RUST_BACKTRACE=full
export RUSTFLAGS="-Z sanitizer=address"

cargo +nightly run --target x86_64-unknown-linux-gnu --bin=sqllogictest -- -v "$("$(dirname "$0")"/slt-fast-files.sh)"
