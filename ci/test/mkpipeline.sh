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
# mkpipeline.sh — dynamically renders a pipeline.yml for Buildkite.

# This script's path is hardcoded into the Buildkite UI. It bootstraps the CI
# process by building the CI builder image, in which all other dependencies
# are installed. Unfortunately that means this script needs to be written in
# Bash, since our Python tools are only available once the CI builder image has
# been built.

set -euo pipefail

bootstrap_steps=

for toolchain in stable nightly; do
  if ! bin/ci-builder exists $toolchain; then
    bootstrap_steps+="
  - label: bootstrap $toolchain
    command: bin/ci-builder build $toolchain --push
    agents:
      queue: builder
"
  fi
done

exec buildkite-agent pipeline upload <<EOF
steps:
  $bootstrap_steps
  - wait
  - command: bin/ci-builder run stable bin/pyactivate --dev -m ci.test.mkpipeline
EOF
