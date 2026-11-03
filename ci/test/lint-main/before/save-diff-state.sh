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
# Snapshots the working tree's diff before the lint checks run, so that
# after/check-no-diff.sh can flag only changes made by the checks themselves.
# A tree that is dirty going in, such as the working copy of a colocated jj
# repo, which git always sees as uncommitted changes, must not fail the lint
# on its own.

set -euo pipefail

cd "$(dirname "$0")/../../../.."

mkdir -p target/lint
git diff > target/lint/diff-before.patch
git diff --compact-summary > target/lint/diff-before.summary
