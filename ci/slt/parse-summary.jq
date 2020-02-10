# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# parse-summary.jq — helper to parse SLT summary for upload to civiz.

{
    "keys": keys_unsorted | join(", "),
    "values": values | map(tostring) | join(", ")
} | "INSERT INTO slt (commit, \(.keys)) VALUES ('\($commit)', \(.values))"
