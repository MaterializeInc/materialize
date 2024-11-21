# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

# Tests for the new PARTITION BY syntax for persisted collections.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_create_table_from_source = true

> CREATE SOURCE auction_house
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR AUCTION
  FOR TABLES (accounts);

# First, check that the flag is disabled by default.

! CREATE MATERIALIZED VIEW integers (n) WITH (PARTITION BY (n)) AS VALUES (3), (2), (1);
contains:PARTITION BY

! CREATE TABLE integers (n int) WITH (PARTITION BY (n));
contains:PARTITION BY

! CREATE TABLE bids FROM SOURCE auction_house (REFERENCE bids) WITH (PARTITION BY (id));
contains:PARTITION BY

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_collection_partition_by = true

> CREATE MATERIALIZED VIEW integers (n) WITH (PARTITION BY (n)) AS VALUES (3), (2), (1);

> CREATE MATERIALIZED VIEW integers_strings (n, m) WITH (PARTITION BY (n, m))
  AS VALUES (3, 'three'), (2, 'two'), (1, 'one');

! CREATE MATERIALIZED VIEW out_of_order (n, m) WITH (PARTITION BY (m, n))
  AS VALUES (3, 'three'), (2, 'two'), (1, 'one');
contains:PARTITION BY columns should be a prefix

! CREATE MATERIALIZED VIEW out_of_order (n, m) WITH (PARTITION BY (m))
  AS VALUES (3, 'three'), (2, 'two'), (1, 'one');
contains:PARTITION BY columns should be a prefix

! CREATE MATERIALIZED VIEW unsupported_type (n, m) WITH (PARTITION BY (n, m))
  AS VALUES (3, '[3]'::json), (2, '[2]'::json), (1, '[1]'::json);
contains:PARTITION BY column m has unsupported type

> CREATE TABLE integers_table (n int) WITH (PARTITION BY (n));

> CREATE TABLE integers_strings_table (n int, m text) WITH (PARTITION BY (n, m));

! CREATE TABLE out_of_order (n int, m text) WITH (PARTITION BY (m, n));
contains:PARTITION BY columns should be a prefix

! CREATE TABLE out_of_order (n int, m text) WITH (PARTITION BY (m));
contains:PARTITION BY columns should be a prefix

! CREATE TABLE unsupported_type (n int, m jsonb) WITH (PARTITION BY (n, m));
contains:PARTITION BY column m has unsupported type

> CREATE TABLE bids FROM SOURCE auction_house (REFERENCE bids) WITH (PARTITION BY (id));

> CREATE TABLE bids_2 FROM SOURCE auction_house (REFERENCE bids) WITH (PARTITION BY (id, buyer));

! CREATE TABLE out_of_order FROM SOURCE auction_house (REFERENCE bids) WITH (PARTITION BY (buyer, id));
contains:PARTITION BY columns should be a prefix

! CREATE TABLE out_of_order FROM SOURCE auction_house (REFERENCE bids) WITH (PARTITION BY (buyer));
contains:PARTITION BY columns should be a prefix
