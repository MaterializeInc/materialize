# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test progress statistics
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

CREATE TABLE t1 (f1 TEXT);
ALTER TABLE t1 REPLICA IDENTITY FULL;

INSERT INTO t1 VALUES ('one');

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;

> SELECT COUNT(*) > 0 FROM t1;
true


> SELECT
    s.name,
    SUM(u.offset_committed) > 0,
    SUM(u.offset_known) >= SUM(u.offset_committed)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('mz_source')
  GROUP BY s.name
  ORDER BY s.name
mz_source true true

$ set-from-sql var=previous-offset-committed
SELECT
    (SUM(u.offset_committed))::text
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('mz_source')


$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t1 VALUES ('two');


> SELECT
    s.name,
    SUM(u.offset_committed) > ${previous-offset-committed},
    SUM(u.offset_known) >= SUM(u.offset_committed)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('mz_source')
  GROUP BY s.name
  ORDER BY s.name
mz_source true true
