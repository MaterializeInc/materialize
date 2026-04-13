# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Reproduces a bug where update_files uses raw column.primary_key from protobuf
# instead of ColumnMetadata::is_primary. This causes _fivetran_id to be treated
# as a non-PK column in UPDATE operations (placed in SET instead of WHERE),
# while replace_files and delete_files correctly treat it as a primary key.
#
# When _fivetran_id has primary_key=false in the protobuf (which is the case
# when Fivetran synthesizes it for tables that also have natural PKs), updates
# match on fewer columns than they should, potentially updating wrong rows.

# Create a schema and table in the test database.
> CREATE SCHEMA test.ft;

> CREATE TABLE test.ft.items (
    pk int,
    "_fivetran_id" text,
    value text,
    "_fivetran_deleted" boolean,
    "_fivetran_synced" timestamptz
  );

# Mark pk as the primary key (Fivetran destination uses magic comments).
> COMMENT ON COLUMN test.ft.items.pk IS 'mz_is_primary_key';

# --- Step 1: Insert initial rows via replace_files ---
# Two rows share the same natural PK (pk=1) but have distinct _fivetran_id values.
# replace_files correctly uses ColumnMetadata::is_primary, so it treats both pk
# AND _fivetran_id as primary keys, keeping these rows distinct.

$ file-append container=fivetran path=replace_data.csv
pk,_fivetran_id,value,_fivetran_deleted,_fivetran_synced
1,aaa,alpha,false,2024-01-01 00:00:00+00
1,bbb,beta,false,2024-01-01 00:00:00+00
2,ccc,gamma,false,2024-01-01 00:00:00+00

$ fivetran-destination action=write_batch dbname=test
{
    "schema_name": "ft",
    "table_name": "items",
    "table": {
        "name": "items",
        "columns": [
            {"name": "pk", "type": 3, "primary_key": true},
            {"name": "_fivetran_id", "type": 13, "primary_key": false},
            {"name": "value", "type": 13, "primary_key": false},
            {"name": "_fivetran_deleted", "type": 1, "primary_key": false},
            {"name": "_fivetran_synced", "type": 10, "primary_key": false}
        ]
    },
    "keys": {},
    "replace_files": ["${testdrive.fivetran-destination-files-path}/replace_data.csv"],
    "update_files": [],
    "delete_files": [],
    "file_params": {
        "compression": 0,
        "encryption": 0,
        "null_string": "null-value",
        "unmodified_string": "unmodified-value"
    }
}
{
    "response": {
        "Success": true
    }
}

# Verify initial state: three distinct rows.
> SELECT pk, "_fivetran_id", value FROM test.ft.items ORDER BY pk, "_fivetran_id";
pk  _fivetran_id  value
-------------------------
1   aaa           alpha
1   bbb           beta
2   ccc           gamma

# --- Step 2: Update targeting ONLY the row (pk=1, _fivetran_id=aaa) ---
# We want to change value from "alpha" to "UPDATED" for just that row.
# _fivetran_id is set to "aaa" (not unmodified), meaning it's the key for
# the target row. The other columns use the unmodified marker.
#
# BUG: update_files only uses column.primary_key for the WHERE clause, so
# _fivetran_id (primary_key=false) goes into SET instead of WHERE.
# The generated SQL is effectively:
#   UPDATE ft.items
#   SET _fivetran_id = 'aaa', value = 'UPDATED', ...
#   WHERE pk = 1
# This matches BOTH rows with pk=1, corrupting the (pk=1, _fivetran_id=bbb) row.

$ file-append container=fivetran path=update_data.csv
pk,_fivetran_id,value,_fivetran_deleted,_fivetran_synced
1,aaa,UPDATED,unmodified-value,unmodified-value

$ fivetran-destination action=write_batch dbname=test
{
    "schema_name": "ft",
    "table_name": "items",
    "table": {
        "name": "items",
        "columns": [
            {"name": "pk", "type": 3, "primary_key": true},
            {"name": "_fivetran_id", "type": 13, "primary_key": false},
            {"name": "value", "type": 13, "primary_key": false},
            {"name": "_fivetran_deleted", "type": 1, "primary_key": false},
            {"name": "_fivetran_synced", "type": 10, "primary_key": false}
        ]
    },
    "keys": {},
    "replace_files": [],
    "update_files": ["${testdrive.fivetran-destination-files-path}/update_data.csv"],
    "delete_files": [],
    "file_params": {
        "compression": 0,
        "encryption": 0,
        "null_string": "null-value",
        "unmodified_string": "unmodified-value"
    }
}
{
    "response": {
        "Success": true
    }
}

# --- Step 3: Verify correct behavior ---
# Only the (pk=1, _fivetran_id=aaa) row should be updated.
# The (pk=1, _fivetran_id=bbb) row must remain unchanged.
#
# BUG: update_files only uses column.primary_key for the WHERE clause, so
# _fivetran_id (primary_key=false) goes into SET instead of WHERE. The WHERE
# clause becomes just "WHERE pk = 1", matching both rows and corrupting the
# bbb row. This test will FAIL until update_files is fixed to use
# ColumnMetadata::is_primary (consistent with replace_files and delete_files).

> SELECT pk, "_fivetran_id", value FROM test.ft.items ORDER BY pk, "_fivetran_id";
pk  _fivetran_id  value
-------------------------
1   aaa           UPDATED
1   bbb           beta
2   ccc           gamma

# Cleanup.
$ file-delete container=fivetran path=replace_data.csv
$ file-delete container=fivetran path=update_data.csv
