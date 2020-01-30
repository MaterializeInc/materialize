# Copyright 2019-2020 Materialize Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# parse-summary.jq — helper to parse SLT summary for upload to civiz.

{
    "keys": keys_unsorted | join(", "),
    "values": values | map(tostring) | join(", ")
} | "INSERT INTO slt (commit, \(.keys)) VALUES ('\($commit)', \(.values))"
