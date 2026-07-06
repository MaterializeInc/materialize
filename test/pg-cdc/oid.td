# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that oid columns ingest the full u32 range, including values above
# i32::MAX, in both the snapshot and the replication path, and that deletes
# of such values retract cleanly. Newly created sources use the full-range
# text-to-oid cast (see cast_oid_full_range in the source export details).
#

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE t_oid (pk int PRIMARY KEY, o oid, oa oid[]);
ALTER TABLE t_oid REPLICA IDENTITY FULL;
INSERT INTO t_oid VALUES (1, 42, '{42}');
INSERT INTO t_oid VALUES (2, 4294967295, '{4294967295,2147483648}');
ANALYZE t_oid;

CREATE PUBLICATION mz_source FOR TABLE t_oid;

> BEGIN
> CREATE SOURCE mz_source FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');
> CREATE TABLE t_oid FROM SOURCE mz_source (REFERENCE t_oid);
> COMMIT

# Snapshot path: the above-i32::MAX values ingest as values, not errors.
> SELECT * FROM t_oid ORDER BY pk;
1 42 {42}
2 4294967295 {4294967295,2147483648}

# Replication path.
$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t_oid VALUES (3, 2147483648, '{4294967294}');

> SELECT * FROM t_oid ORDER BY pk;
1 42 {42}
2 4294967295 {4294967295,2147483648}
3 2147483648 {4294967294}

# Deletes re-cast the old tuple, so the above-i32::MAX rows must retract
# cleanly.
$ postgres-execute connection=postgres://postgres:postgres@postgres
DELETE FROM t_oid WHERE pk IN (2, 3);

> SELECT * FROM t_oid ORDER BY pk;
1 42 {42}
