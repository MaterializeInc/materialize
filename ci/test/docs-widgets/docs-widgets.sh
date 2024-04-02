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
# Example usages:
#
#     $ ci/test/docs-widgets/docs-widgets.sh

# This script is used to run tests for the Materialize Docs Widgets

set -euo pipefail

. misc/shlib/shlib.bash

# Change to the script's directory
cd "$(dirname "$0")"

# Install the dependencies
npm install

# Run the tests
npm run test -- --run
