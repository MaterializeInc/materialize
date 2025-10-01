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

curl https://sh.rustup.rs -sSf | sh -s -- -y
# shellcheck source=/dev/null
source /cargo/env
trap "rustup self uninstall -y" SIGTERM SIGINT EXIT
rustup install beta
rustup run beta cargo build --all-targets
