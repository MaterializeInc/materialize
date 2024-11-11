# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests for the new PARTITION BY syntax for persisted collections.

# First, check that the flag is disabled by default.

! CREATE MATERIALIZED VIEW integers (n) WITH (PARTITION BY (n)) AS VALUES (3), (2), (1);
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
