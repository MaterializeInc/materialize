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
# build-antithesis.sh — antithesis-flavored build entry point.
#
# Regenerates test/antithesis/config/docker-compose.yaml against the
# current source tree before invoking ci.test.build, so that the
# `antithesis-config` mzbuild image bakes in a compose YAML whose
# materialized/antithesis-workload fingerprints match the fingerprints
# this build is about to publish to GHCR.
#
# The committed YAML in test/antithesis/config/docker-compose.yaml is for
# human review (PR diffs); its fingerprints can drift on every materialized
# source change, and the staleness lint masks them by design. This script
# is what guarantees Antithesis sees a self-consistent compose.

set -euo pipefail

: "${CI_ANTITHESIS:?build-antithesis.sh expects CI_ANTITHESIS=1}"

echo "--- Regenerating test/antithesis/config/docker-compose.yaml"
bin/pyactivate test/antithesis/export-compose.py \
    > test/antithesis/config/docker-compose.yaml

exec bin/pyactivate -m ci.test.build
