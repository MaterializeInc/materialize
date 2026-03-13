# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Write batch B: replace rows 1-N with value "B" (same PKs as batch A)

$ fivetran-destination action=write_batch dbname=test host=materialized
{
    "schema_name": "tester",
    "table_name": "concurrent_test",
    "table": {
        "name": "concurrent_test",
        "columns": [
            {"name": "id", "type": 3, "primary_key": true},
            {"name": "val", "type": 13, "primary_key": false}
        ]
    },
    "keys": {},
    "replace_files": ["/data/batch_b.csv"],
    "update_files": [],
    "delete_files": [],
    "file_params": {
        "compression": 0,
        "encryption": 0,
        "null_string": "null-string",
        "unmodified_string": "unmodified-string"
    }
}
{
    "response": {
        "Success": true
    }
}
