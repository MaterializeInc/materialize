#!/usr/bin/env bash

# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# mkpipeline.sh — dynamically renders a pipeline.yml for Buildkite.

# This wrapper script exists for compatibility with past revisions, where this
# script was written in Bash. Its path is hardcoded into the Buildkite UI.

set -euo pipefail

exec bin/ci-builder run stable bin/pyactivate -m ci.test.mkpipeline "$@"
