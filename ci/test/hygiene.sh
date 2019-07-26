#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# hygiene.sh — lint, clippy, etc.

set -euo pipefail

. misc/shlib/shlib.bash

ci_init

ci_try bin/lint
ci_try cargo fmt -- --check
ci_try cargo test --doc
# Intentionally run check last, since otherwise it won't use the cache.
# https://github.com/rust-lang/rust-clippy/issues/3840
ci_try bin/check

ci_status_report
