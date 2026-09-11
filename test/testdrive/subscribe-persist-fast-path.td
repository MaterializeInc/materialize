# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# SUBSCRIBE on a table or materialized view served by tailing the
# persist shard from environmentd (`enable_subscribe_persist_fast_path`). The
# same statements run with the flag off at the end and must produce identical
# output, which is what makes the two paths interchangeable for clients.

$ postgres-execute connection=mz_system
ALTER SYSTEM SET enable_subscribe_persist_fast_path = true
ALTER SYSTEM SET enable_within_timestamp_order_by_in_subscribe = true
ALTER SYSTEM SET enable_logical_compaction_window = true

$ set-regex match=\d{13,20} replacement=<TIMESTAMP>

# Writes while a cursor is open come from a second connection, since a
# transaction holding a SUBSCRIBE is read-only.
$ postgres-connect name=writer url=postgres://materialize:materialize@${testdrive.materialize-sql-addr}

> CREATE TABLE t (a int, b text)
> INSERT INTO t VALUES (1, 'one'), (2, 'two')
> CREATE MATERIALIZED VIEW mv AS SELECT a, b FROM t
> CREATE DEFAULT INDEX ON t

# Snapshot and later updates on a table, which is txn-wal backed, so progress
# between writes comes from the txns shard.
> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE t
> FETCH 2 c
<TIMESTAMP> 1 1 one
<TIMESTAMP> 1 2 two
$ postgres-execute connection=writer
INSERT INTO t VALUES (3, 'three')
> FETCH 1 c WITH (TIMEOUT = '30s')
<TIMESTAMP> 1 3 three
$ postgres-execute connection=writer
DELETE FROM t WHERE a = 1
> FETCH 1 c WITH (TIMEOUT = '30s')
<TIMESTAMP> -1 1 one
> COMMIT

# The subscribe appears in mz_subscriptions and is retired with the
# connection like a dataflow-backed one.
> SELECT count(*) FROM mz_internal.mz_subscriptions
0

# A materialized view, which is a plain persist shard written by compute.
> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE mv
> FETCH 2 c
<TIMESTAMP> 1 2 two
<TIMESTAMP> 1 3 three
$ postgres-execute connection=writer
INSERT INTO t VALUES (4, 'four')
> FETCH 1 c WITH (TIMEOUT = '30s')
<TIMESTAMP> 1 4 four
> COMMIT

# Progress messages, ENVELOPE UPSERT, and WITHIN TIMESTAMP ORDER BY are
# formatted the same way as for a dataflow.
> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE mv WITH (PROGRESS) ENVELOPE UPSERT (KEY (a))
> FETCH 4 c
<TIMESTAMP> true <null> <null> <null>
<TIMESTAMP> false upsert 2 two
<TIMESTAMP> false upsert 3 three
<TIMESTAMP> false upsert 4 four
> FETCH 1 c
<TIMESTAMP> true <null> <null> <null>
> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE mv ENVELOPE UPSERT (KEY (a))
> FETCH 3 c
<TIMESTAMP> upsert 2 two
<TIMESTAMP> upsert 3 three
<TIMESTAMP> upsert 4 four
$ postgres-execute connection=writer
UPDATE t SET b = 'FOUR' WHERE a = 4
> FETCH 1 c WITH (TIMEOUT = '30s')
<TIMESTAMP> upsert 4 FOUR
$ postgres-execute connection=writer
DELETE FROM t WHERE a = 2
> FETCH 1 c WITH (TIMEOUT = '30s')
<TIMESTAMP> delete 2 <null>
> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE t WITHIN TIMESTAMP ORDER BY a DESC
> FETCH 2 c
<TIMESTAMP> 1 4 FOUR
<TIMESTAMP> 1 3 three
> COMMIT

# SNAPSHOT false skips the snapshot, AS OF and UP TO bound the stream.
$ set-from-sql var=as_of
SELECT mz_now()::text

> INSERT INTO t VALUES (5, 'five')

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE t WITH (SNAPSHOT false) AS OF ${as_of}
> FETCH 1 c WITH (TIMEOUT = '30s')
<TIMESTAMP> 1 5 five
> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE t AS OF ${as_of} UP TO ${as_of}
> FETCH ALL c WITH (TIMEOUT = '30s')
> COMMIT

# Dropping the subscribed object ends the subscribe with an error.
> CREATE TABLE dropped (a int)
> INSERT INTO dropped VALUES (1)

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE dropped
> FETCH 1 c
<TIMESTAMP> 1 1

$ postgres-execute connection=writer
DROP TABLE dropped

! FETCH 1 c WITH (TIMEOUT = '30s')
contains:was dropped

> ROLLBACK

# A client that stops fetching while more than the buffer budget is written
# resumes where it left off, without gaps or duplicates, as long as the
# collection retains the history. Wide rows keep the row count small.
$ postgres-execute connection=mz_system
ALTER SYSTEM SET subscribe_max_buffered_bytes = 16384

> CREATE TABLE stalled (a int, b text) WITH (RETAIN HISTORY FOR '10 minutes')
> INSERT INTO stalled VALUES (0, 'start')

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE stalled
> FETCH 1 c
<TIMESTAMP> 1 0 start

$ postgres-execute connection=writer
INSERT INTO stalled SELECT g, repeat('x', 2000) FROM generate_series(1, 10) g
INSERT INTO stalled SELECT g, repeat('x', 2000) FROM generate_series(11, 20) g
INSERT INTO stalled VALUES (21, 'end')

$ set-regex match=\d{13,20}|x{2000} replacement=<X>

> FETCH 21 c WITH (TIMEOUT = '60s')
<X> 1 1 <X>
<X> 1 2 <X>
<X> 1 3 <X>
<X> 1 4 <X>
<X> 1 5 <X>
<X> 1 6 <X>
<X> 1 7 <X>
<X> 1 8 <X>
<X> 1 9 <X>
<X> 1 10 <X>
<X> 1 11 <X>
<X> 1 12 <X>
<X> 1 13 <X>
<X> 1 14 <X>
<X> 1 15 <X>
<X> 1 16 <X>
<X> 1 17 <X>
<X> 1 18 <X>
<X> 1 19 <X>
<X> 1 20 <X>
<X> 1 21 end
> COMMIT

$ set-regex match=\d{13,20} replacement=<TIMESTAMP>

$ postgres-execute connection=mz_system
ALTER SYSTEM RESET subscribe_max_buffered_bytes

# A snapshot larger than one chunk is handed out in pieces as the client
# fetches, rather than formatted whole. Rows of a timestamp come out in row
# order on both paths, so the first of them are deterministic; that the whole
# of a large snapshot arrives exactly once is covered by the unit tests.
> CREATE TABLE big (a text)

> INSERT INTO big SELECT lpad(g::text, 5, '0') FROM generate_series(1, 5000) g

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE big
> FETCH 3 c
<TIMESTAMP> 1 00001
<TIMESTAMP> 1 00002
<TIMESTAMP> 1 00003
> COMMIT

# A snapshot spanning several chunks arrives complete and in order. The chunk
# size is lowered so that a handful of rows spans them; it reaches the storage
# controller asynchronously, so at worst this runs with the default size.
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET storage_subscribe_snapshot_chunk_size = 2

> CREATE TABLE chunked (a text)

> INSERT INTO chunked SELECT lpad(g::text, 2, '0') FROM generate_series(1, 7) g

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE chunked
> FETCH 7 c
<TIMESTAMP> 1 01
<TIMESTAMP> 1 02
<TIMESTAMP> 1 03
<TIMESTAMP> 1 04
<TIMESTAMP> 1 05
<TIMESTAMP> 1 06
<TIMESTAMP> 1 07
> COMMIT

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM RESET storage_subscribe_snapshot_chunk_size

# The snapshot is consolidated whatever it is split into: a row written and
# deleted before the as_of never appears.
> CREATE TABLE consolidated (a int)

> INSERT INTO consolidated VALUES (1), (2)

> DELETE FROM consolidated WHERE a = 1

> INSERT INTO consolidated VALUES (3)

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE consolidated
> FETCH 2 c
<TIMESTAMP> 1 2
<TIMESTAMP> 1 3
> COMMIT

# The same statements with the persist fast path off produce the same output.
$ postgres-execute connection=mz_system
ALTER SYSTEM SET enable_subscribe_persist_fast_path = false

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE big
> FETCH 3 c
<TIMESTAMP> 1 00001
<TIMESTAMP> 1 00002
<TIMESTAMP> 1 00003
> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE consolidated
> FETCH 2 c
<TIMESTAMP> 1 2
<TIMESTAMP> 1 3
> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE t
> FETCH 3 c
<TIMESTAMP> 1 3 three
<TIMESTAMP> 1 4 FOUR
<TIMESTAMP> 1 5 five
$ postgres-execute connection=writer
INSERT INTO t VALUES (6, 'six')
> FETCH 1 c WITH (TIMEOUT = '30s')
<TIMESTAMP> 1 6 six
> COMMIT

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE mv WITH (PROGRESS) ENVELOPE UPSERT (KEY (a))
> FETCH 5 c
<TIMESTAMP> true <null> <null> <null>
<TIMESTAMP> false upsert 3 three
<TIMESTAMP> false upsert 4 FOUR
<TIMESTAMP> false upsert 5 five
<TIMESTAMP> false upsert 6 six
> FETCH 1 c
<TIMESTAMP> true <null> <null> <null>
> COMMIT

$ postgres-execute connection=mz_system
ALTER SYSTEM RESET enable_subscribe_persist_fast_path
