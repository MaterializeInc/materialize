# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for BUG #41: COPY FROM Parquet Map keys not sorted.
#
# Materialize's Datum::Map requires keys to be in ascending sorted order.
# The COPY FROM parquet reader pushes map entries in their original order
# without sorting. If a parquet file has unsorted map keys, this produces
# silent data corruption: map equality, hashing, and comparisons all depend
# on sorted keys.
#
# This test creates a parquet file with deliberately unsorted map keys,
# COPY FROMs it, and verifies that the resulting maps are correctly sorted
# and compare equal to maps inserted via SQL (which are always sorted).

$ set-max-tries max-tries=1

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_copy_from_remote = true;

> CREATE SECRET aws_secret AS '${arg.aws-secret-access-key}'

> CREATE CONNECTION aws_conn
  TO AWS (
    ACCESS KEY ID = '${arg.aws-access-key-id}',
    SECRET ACCESS KEY = SECRET aws_secret,
    ENDPOINT = '${arg.aws-endpoint}',
    REGION = 'us-east-1'
  );

# Upload parquet file with deliberately unsorted map keys.
# Row 0: id=1, keys in reverse order: {"z": "val_z", "a": "val_a", "m": "val_m"}
# Row 1: id=2, keys unsorted: {"b": "val_b", "a": "val_a"}
# Row 2: id=3, single key (trivially sorted): {"x": "val_x"}
# Row 3: id=4, empty map: {}
# Row 4: id=5, duplicated keys: {"y": "val_y", "y": "val_y2", "y": "val_y3"}
$ s3-upload-parquet-unsorted-map bucket=copyfroms3 key=parquet/unsorted_map.parquet

> CREATE TABLE from_parquet (id int, map_col map[text => text]);

$ s3-set-presigned-url bucket=copyfroms3 key=parquet/unsorted_map.parquet var-name=unsorted_map_url

> COPY INTO from_parquet FROM '${unsorted_map_url}' (FORMAT PARQUET);

# Insert equivalent maps via SQL. SQL insertion always sorts keys correctly.
> CREATE TABLE from_sql (id int, map_col map[text => text]);

> INSERT INTO from_sql VALUES
  (1, '{a=>val_a,m=>val_m,z=>val_z}'),
  (2, '{a=>val_a,b=>val_b}'),
  (3, '{x=>val_x}'),
  (4, '{}'),
  (5, '{y=>val_y3}');

# Verify that COPY FROM'd maps have keys in sorted order.
# If the bug is present, keys will be in their original unsorted order
# (e.g., {z=>val_z,a=>val_a,m=>val_m} instead of {a=>val_a,m=>val_m,z=>val_z}).
> SELECT id, map_col::text FROM from_parquet ORDER BY id;
1 {a=>val_a,m=>val_m,z=>val_z}
2 {a=>val_a,b=>val_b}
3 {x=>val_x}
4 {}
5 {y=>val_y3}

# Verify equality: maps from COPY FROM should equal maps inserted via SQL.
# If unsorted, these will NOT match, demonstrating silent data corruption.
> SELECT p.id, p.map_col::text = s.map_col::text AS maps_equal
  FROM from_parquet p
  JOIN from_sql s ON p.id = s.id
  ORDER BY p.id;
1 true
2 true
3 true
4 true
5 true

# Verify that a join on map_col works correctly.
# If keys are unsorted, hash/equality is broken and this join will miss rows.
> SELECT p.id
  FROM from_parquet p
  JOIN from_sql s ON p.map_col::text = s.map_col::text
  ORDER BY p.id;
1
2
3
4
5
