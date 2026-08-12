# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test a declaratively partitioned upstream table. The publication lists each
# leaf partition individually and leaves `publish_via_partition_root` at its
# default of `false`, so the upstream reports changes against the leaf that
# physically holds the row. Materialize therefore replicates one table per leaf
# and the partitioned root is not a valid reference.
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

CREATE TABLE events (id INTEGER NOT NULL, region INTEGER NOT NULL, payload TEXT) PARTITION BY LIST (region);
CREATE TABLE events_r1 PARTITION OF events FOR VALUES IN (1);
CREATE TABLE events_r2 PARTITION OF events FOR VALUES IN (2);
ALTER TABLE events_r1 REPLICA IDENTITY FULL;
ALTER TABLE events_r2 REPLICA IDENTITY FULL;

INSERT INTO events VALUES (1, 1, 'r1-one'), (2, 2, 'r2-two');
INSERT INTO events_r1 VALUES (3, 1, 'r1-three');

ANALYZE;

CREATE PUBLICATION mz_source FOR TABLE events_r1, events_r2;

> CREATE SOURCE mz_source FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');

# A partitioned root has no storage of its own and was not added to the
# publication, so it is not part of the source.
! CREATE TABLE events FROM SOURCE mz_source (REFERENCE events);
contains:reference to events not found in source

> CREATE TABLE events_r1 FROM SOURCE mz_source (REFERENCE events_r1);
> CREATE TABLE events_r2 FROM SOURCE mz_source (REFERENCE events_r2);

> SELECT * FROM events_r1;
1 1 r1-one
3 1 r1-three

> SELECT * FROM events_r2;
2 2 r2-two

#
# Replicating the leaves reassembles into the logical partitioned table.
#

> CREATE VIEW events AS
  SELECT * FROM events_r1
  UNION ALL
  SELECT * FROM events_r2;

> SELECT * FROM events;
1 1 r1-one
2 2 r2-two
3 1 r1-three

#
# DML issued against the root is routed to the owning leaf upstream, and arrives
# in Materialize attributed to that leaf.
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO events VALUES (4, 2, 'r2-four');
UPDATE events SET payload = 'r1-one-updated' WHERE id = 1;
DELETE FROM events WHERE id = 3;

> SELECT * FROM events_r1;
1 1 r1-one-updated

> SELECT * FROM events_r2;
2 2 r2-two
4 2 r2-four

# An UPDATE of the partition key moves the row between leaves, which the
# upstream publishes as a DELETE on the old leaf and an INSERT on the new one.

$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE events SET region = 2 WHERE id = 1;

> SELECT * FROM events_r1;

> SELECT * FROM events_r2;
1 2 r1-one-updated
2 2 r2-two
4 2 r2-four

> SELECT * FROM events;
1 2 r1-one-updated
2 2 r2-two
4 2 r2-four

#
# A partition added upstream after the source was created is picked up by adding
# it to the publication and running CREATE TABLE ... FROM SOURCE. Rows already
# present in the new leaf come in via its snapshot.
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE events_r3 PARTITION OF events FOR VALUES IN (3);
ALTER TABLE events_r3 REPLICA IDENTITY FULL;
INSERT INTO events VALUES (10, 3, 'r3-ten');
ALTER PUBLICATION mz_source ADD TABLE events_r3;

> CREATE TABLE events_r3 FROM SOURCE mz_source (REFERENCE events_r3);

> SELECT * FROM events_r3;
10 3 r3-ten

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO events VALUES (11, 3, 'r3-eleven');

> SELECT * FROM events_r3;
10 3 r3-ten
11 3 r3-eleven

> CREATE OR REPLACE VIEW events AS
  SELECT * FROM events_r1
  UNION ALL
  SELECT * FROM events_r2
  UNION ALL
  SELECT * FROM events_r3;

> SELECT * FROM events;
1 2 r1-one-updated
10 3 r3-ten
11 3 r3-eleven
2 2 r2-two
4 2 r2-four

#
# Detaching a partition upstream leaves the leaf a standalone table. It stays in
# the publication because it was added there by name, so Materialize keeps
# replicating it and neither the source nor the other leaves are disturbed.
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE events DETACH PARTITION events_r2;
INSERT INTO events_r2 VALUES (99, 2, 'r2-detached');
INSERT INTO events VALUES (12, 3, 'r3-twelve');

> SELECT * FROM events_r2;
1 2 r1-one-updated
2 2 r2-two
4 2 r2-four
99 2 r2-detached

> SELECT * FROM events_r3;
10 3 r3-ten
11 3 r3-eleven
12 3 r3-twelve

> SELECT status, error FROM mz_internal.mz_source_statuses WHERE name = 'mz_source';
running <null>

#
# The upstream table for a detached partition can only be dropped once nothing
# in Materialize replicates it, so the Materialize table goes first. Dropping it
# does not disturb the remaining leaves.
#

> CREATE OR REPLACE VIEW events AS
  SELECT * FROM events_r1
  UNION ALL
  SELECT * FROM events_r3;

> DROP TABLE events_r2;

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP TABLE events_r2;
INSERT INTO events VALUES (13, 3, 'r3-thirteen');
INSERT INTO events VALUES (14, 1, 'r1-fourteen');

> SELECT * FROM events;
13 3 r3-thirteen
14 1 r1-fourteen
10 3 r3-ten
11 3 r3-eleven
12 3 r3-twelve

> SELECT status, error FROM mz_internal.mz_source_statuses WHERE name = 'mz_source';
running <null>

> SELECT status FROM mz_internal.mz_source_statuses WHERE name IN ('events_r1', 'events_r3');
running
running
