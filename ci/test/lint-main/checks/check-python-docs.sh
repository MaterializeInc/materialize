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
# check-python-docs.sh — check Python docs.

set -euo pipefail

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash

if [[ ! "${MZDEV_NO_PYTHON:-}" ]]; then
  if [[ ! "${MZDEV_NO_PYTHON_DOCTEST:-}" ]]; then
    try bin/pyactivate -m pytest -qq --doctest-modules misc/python
  fi
fi

try_status_report
