#!/usr/bin/env bash

# Copyright 2019-2020 Materialize Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# lint-slow.sh — slow linters that require building code.

set -euo pipefail

. misc/shlib/shlib.bash

ci_init

ci_try cargo test --locked --doc
# Intentionally run check last, since otherwise it won't use the cache.
# https://github.com/rust-lang/rust-clippy/issues/3840
ci_try bin/check

ci_status_report
