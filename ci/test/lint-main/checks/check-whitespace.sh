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
# check-whitespace.sh — check that files end with an empty line and there are no trailing whitespaces

set -euo pipefail

cd "$(dirname "$0")/../../../.."

. misc/shlib/shlib.bash

files=$(git_files "$@")

# Only binary files are permitted to omit a trailing newline. If you're here to
# exclude a text file that is missing its trailing newline, like an SVG, add
# a trailing newline to the text file instead.
newline_files=$(grep -vE '(_scratch|\.(png|jpe?g|pb|avro|ico|so|patch))$' <<< "$files")

try xargs misc/lint/trailing-newline.sh <<< "$newline_files"
try xargs git --no-pager diff --check "$(git_empty_tree)" <<< "$newline_files"

try_status_report
