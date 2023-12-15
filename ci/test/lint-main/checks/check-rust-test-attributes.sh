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
# check-rust-test-attributes.sh — checks usage of test/tokio::test attributes.

set -euo pipefail

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash

rust_files=$(sort -u <(git_files '*.rs'))

try xargs misc/lint/test-attribute.sh <<< "$rust_files"

try_status_report
