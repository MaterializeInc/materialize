#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# lint-fast.sh — fast linters that don't require building any code.

set -euo pipefail

. misc/shlib/shlib.bash

ci_init

ci_try bin/lint
ci_try cargo fmt -- --check

ci_status_report
