# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

# Assertions on the Postgres side go through `pgsink.wait_for`, which polls
# until a query returns the expected text, because the sink works
# asynchronously. It is a procedure so that it can COMMIT between polls, and
# each CALL has to be the only statement on its line, since statements that
# share a line share an implicit transaction that the COMMIT may not end.

# The publication makes this test cover a target database configured for
# logical replication. `FOR ALL TABLES` covers every table created after it,
# and PostgreSQL rejects DELETE on a published table with no replica identity.

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP PUBLICATION IF EXISTS pgsink_all_tables;
DROP SCHEMA IF EXISTS pgsink CASCADE;
DROP SCHEMA IF EXISTS materialize CASCADE;
CREATE SCHEMA pgsink;
CREATE PUBLICATION pgsink_all_tables FOR ALL TABLES;
CREATE PROCEDURE pgsink.wait_for(query text, expected text) LANGUAGE plpgsql AS $$ DECLARE actual text; BEGIN FOR i IN 1..600 LOOP EXECUTE query INTO actual; IF actual IS NOT DISTINCT FROM expected THEN RETURN; END IF; COMMIT; PERFORM pg_sleep(0.1); END LOOP; RAISE EXCEPTION 'query % returned % but expected %', query, actual, expected; END $$;

> CREATE SECRET pgsink_pass AS 'postgres'

> CREATE CONNECTION pgsink_conn TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgsink_pass
  )

> CREATE TABLE pgsink_t (a int NOT NULL, b text, c numeric(38, 2), d timestamptz, e jsonb, f uuid, g int[])

> INSERT INTO pgsink_t VALUES
  (1, 'one', 1.50, '2024-01-01 00:00:00+00', '{"k": 1}', '00000000-0000-0000-0000-000000000001', ARRAY[1, 2]),
  (2, 'tab' || chr(9) || 'nl' || chr(10) || 'bs\ quote''s', -2.25, NULL, NULL, NULL, NULL),
  (3, NULL, NULL, NULL, 'null', NULL, ARRAY[]::int[])

# A Postgres sink needs a KEY, and the KEY has to be a unique key of the
# relation unless the user says NOT ENFORCED.

! CREATE SINK pgsink_nokey_snk
  IN CLUSTER ${arg.single-replica-cluster}
  FROM pgsink_t
  INTO POSTGRES CONNECTION pgsink_conn (SCHEMA = 'pgsink', TABLE = 'target')
contains:Postgres sinks must specify a KEY

! CREATE SINK pgsink_badkey_snk
  IN CLUSTER ${arg.single-replica-cluster}
  FROM pgsink_t
  INTO POSTGRES CONNECTION pgsink_conn (SCHEMA = 'pgsink', TABLE = 'target')
  KEY (a)
contains:upsert key could not be validated as unique

> CREATE SINK pgsink_snk
  IN CLUSTER ${arg.single-replica-cluster}
  FROM pgsink_t
  INTO POSTGRES CONNECTION pgsink_conn (SCHEMA = 'pgsink', TABLE = 'target')
  KEY (a) NOT ENFORCED

$ set-from-sql var=sink-id
SELECT id FROM mz_sinks WHERE name = 'pgsink_snk'

> SELECT status FROM mz_internal.mz_sink_statuses WHERE name = 'pgsink_snk'
running

# The target table mirrors the source relation with the key as its primary
# key. Staging happens in a session temporary table, so it is the only table
# the sink creates in the schema.

$ postgres-execute connection=postgres://postgres:postgres@postgres
SET TIME ZONE 'UTC';
CALL pgsink.wait_for('SELECT string_agg(attname || '' '' || format_type(atttypid, atttypmod) || CASE WHEN attnotnull THEN '' not null'' ELSE '''' END, '', '' ORDER BY attnum) FROM pg_attribute WHERE attrelid = ''pgsink.target''::regclass AND attnum > 0 AND NOT attisdropped', 'a integer not null, b text, c numeric(39,2), d timestamp with time zone, e jsonb, f uuid, g integer[]');
CALL pgsink.wait_for('SELECT string_agg(a.attname, '', '') FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = ''pgsink.target''::regclass AND i.indisprimary', 'a');
CALL pgsink.wait_for('SELECT string_agg(tablename, '', '') FROM pg_tables WHERE schemaname = ''pgsink''', 'target');

# A halted sink is reported here, with its error, rather than being left to
# time out one of the polls below against an empty target table.
> SELECT status, error FROM mz_internal.mz_sink_statuses WHERE name = 'pgsink_snk'
running <null>

# The progress row records the frontier and the epoch of the incarnation that
# holds the sink.

$ postgres-execute connection=postgres://postgres:postgres@postgres
CALL pgsink.wait_for('SELECT (mz_frontier > 0 AND epoch > 0)::text FROM materialize.sink_progress WHERE sink_id = ''${sink-id}''', 'true');
CALL pgsink.wait_for('SELECT string_agg(sink_id || '' '' || sink_schema || '' '' || sink_table, '', '') FROM materialize.sink_progress', '${sink-id} pgsink target');

# Every column round-trips into the target, including NULLs and text with
# tabs, newlines, backslashes and quotes.

$ postgres-execute connection=postgres://postgres:postgres@postgres
CALL pgsink.wait_for('SELECT string_agg(a || ''|'' || coalesce(replace(replace(b, chr(9), ''<TAB>''), chr(10), ''<NL>''), ''<NULL>'') || ''|'' || coalesce(c::text, ''<NULL>'') || ''|'' || coalesce(d::text, ''<NULL>'') || ''|'' || coalesce(e::text, ''<NULL>'') || ''|'' || coalesce(f::text, ''<NULL>'') || ''|'' || coalesce(g::text, ''<NULL>''), '' ; '' ORDER BY a) FROM pgsink.target', '1|one|1.50|2024-01-01 00:00:00+00|{"k": 1}|00000000-0000-0000-0000-000000000001|{1,2} ; 2|tab<TAB>nl<NL>bs\ quote''s|-2.25|<NULL>|<NULL>|<NULL>|<NULL> ; 3|<NULL>|<NULL>|<NULL>|null|<NULL>|{}');

> SELECT write_frontier > 0 FROM mz_internal.mz_frontiers WHERE object_id = '${sink-id}'
true

# An update replaces the row for its key and a delete removes it.

> UPDATE pgsink_t SET b = 'uno' WHERE a = 1

> DELETE FROM pgsink_t WHERE a = 3

$ postgres-execute connection=postgres://postgres:postgres@postgres
CALL pgsink.wait_for('SELECT string_agg(a || ''|'' || coalesce(replace(replace(b, chr(9), ''<TAB>''), chr(10), ''<NL>''), ''<NULL>''), '' ; '' ORDER BY a, b) FROM pgsink.target', '1|uno ; 2|tab<TAB>nl<NL>bs\ quote''s');

# An update puts a retraction and an insertion for one key at the same
# timestamp, so a single window holds both and the apply statement's ordering
# has to pick the insertion. One row, with the new value.

> UPDATE pgsink_t SET b = 'second' WHERE a = 1

$ postgres-execute connection=postgres://postgres:postgres@postgres
CALL pgsink.wait_for('SELECT string_agg(a || ''|'' || coalesce(b, ''<NULL>''), '' ; '' ORDER BY a, b) FROM pgsink.target WHERE a = 1', '1|second');

# A relation with a natural unique key needs no NOT ENFORCED.

> CREATE MATERIALIZED VIEW pgsink_agg AS SELECT a, count(*) AS n FROM pgsink_t GROUP BY a

> CREATE SINK pgsink_agg_snk
  IN CLUSTER ${arg.single-replica-cluster}
  FROM pgsink_agg
  INTO POSTGRES CONNECTION pgsink_conn (SCHEMA = 'pgsink', TABLE = 'agg_target')
  KEY (a)

$ postgres-execute connection=postgres://postgres:postgres@postgres
CALL pgsink.wait_for('SELECT string_agg(a || ''|'' || n, '' ; '' ORDER BY a) FROM pgsink.agg_target', '1|1 ; 2|1');

> DROP SINK pgsink_agg_snk

# A restart claims a new epoch and resumes from the recorded frontier without
# applying anything twice.

$ postgres-execute connection=mz_system
ALTER CLUSTER ${arg.single-replica-cluster} SET (REPLICATION FACTOR 0);
ALTER CLUSTER ${arg.single-replica-cluster} SET (REPLICATION FACTOR 1);

> INSERT INTO pgsink_t VALUES (5, 'after restart', NULL, NULL, NULL, NULL, NULL)

$ postgres-execute connection=postgres://postgres:postgres@postgres
CALL pgsink.wait_for('SELECT string_agg(a || ''|'' || coalesce(b, ''<NULL>''), '' ; '' ORDER BY a, b) FROM pgsink.target WHERE a IN (1, 5)', '1|second ; 5|after restart');
CALL pgsink.wait_for('SELECT (epoch > 1)::text FROM materialize.sink_progress WHERE sink_id = ''${sink-id}''', 'true');

> SELECT status FROM mz_internal.mz_sink_statuses WHERE name = 'pgsink_snk'
running

# The progress table is the source of truth. A recorded frontier ahead of the
# progress shard, which is what a crash between the transaction and the shard
# append leaves behind, pulls the shard forward instead of halting. Bumping it
# by a millisecond while the sink is down is safe because no data lives in
# that window.

$ postgres-execute connection=mz_system
ALTER CLUSTER ${arg.single-replica-cluster} SET (REPLICATION FACTOR 0);

$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE materialize.sink_progress SET mz_frontier = mz_frontier + 1 WHERE sink_id = '${sink-id}';

$ postgres-execute connection=mz_system
ALTER CLUSTER ${arg.single-replica-cluster} SET (REPLICATION FACTOR 1);

> SELECT status FROM mz_internal.mz_sink_statuses WHERE name = 'pgsink_snk'
running

> INSERT INTO pgsink_t VALUES (6, 'after bump', NULL, NULL, NULL, NULL, NULL)

$ postgres-execute connection=postgres://postgres:postgres@postgres
CALL pgsink.wait_for('SELECT count(*)::text FROM pgsink.target WHERE a = 6', '1');

# A progress row claimed by a newer version of the sink fences this one off.

$ postgres-execute connection=mz_system
ALTER CLUSTER ${arg.single-replica-cluster} SET (REPLICATION FACTOR 0);

$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE materialize.sink_progress SET sink_version = 999 WHERE sink_id = '${sink-id}';

$ postgres-execute connection=mz_system
ALTER CLUSTER ${arg.single-replica-cluster} SET (REPLICATION FACTOR 1);

> SELECT status, error ~ 'fenced off by a newer version' FROM mz_internal.mz_sink_statuses WHERE name = 'pgsink_snk'
stalled true

$ postgres-execute connection=mz_system
ALTER CLUSTER ${arg.single-replica-cluster} SET (REPLICATION FACTOR 0);

$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE materialize.sink_progress SET sink_version = 0 WHERE sink_id = '${sink-id}';

$ postgres-execute connection=mz_system
ALTER CLUSTER ${arg.single-replica-cluster} SET (REPLICATION FACTOR 1);

> SELECT status FROM mz_internal.mz_sink_statuses WHERE name = 'pgsink_snk'
running

# A recorded frontier behind the shard means the target database lost writes
# the sink already committed, and those rows can no longer be replayed. The
# sink has to say so rather than silently skip them.

$ postgres-execute connection=mz_system
ALTER CLUSTER ${arg.single-replica-cluster} SET (REPLICATION FACTOR 0);

$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE materialize.sink_progress SET mz_frontier = 0 WHERE sink_id = '${sink-id}';

$ postgres-execute connection=mz_system
ALTER CLUSTER ${arg.single-replica-cluster} SET (REPLICATION FACTOR 1);

> SELECT status, error ~ 'is behind its progress shard' FROM mz_internal.mz_sink_statuses WHERE name = 'pgsink_snk'
stalled true

> DROP SINK pgsink_snk

# There is no envelope to state, so stating one is an error whichever it is.

! CREATE SINK pgsink_envelope_snk
  IN CLUSTER ${arg.single-replica-cluster}
  FROM pgsink_t
  INTO POSTGRES CONNECTION pgsink_conn (SCHEMA = 'pgsink', TABLE = 'envelope_target')
  KEY (a) NOT ENFORCED
  ENVELOPE UPSERT
contains:ENVELOPE is not supported for Postgres sinks

! CREATE SINK pgsink_envelope_snk
  IN CLUSTER ${arg.single-replica-cluster}
  FROM pgsink_t
  INTO POSTGRES CONNECTION pgsink_conn (SCHEMA = 'pgsink', TABLE = 'envelope_target')
  KEY (a) NOT ENFORCED
  ENVELOPE DEBEZIUM
contains:ENVELOPE is not supported for Postgres sinks

# Column types that stock PostgreSQL does not have are rejected before any
# table is created.

> CREATE TABLE pgsink_bad (a uint8)

> CREATE SINK pgsink_bad_snk
  IN CLUSTER ${arg.single-replica-cluster}
  FROM pgsink_bad
  INTO POSTGRES CONNECTION pgsink_conn (SCHEMA = 'pgsink', TABLE = 'bad_target')
  KEY (a) NOT ENFORCED

> SELECT status, error FROM mz_internal.mz_sink_statuses WHERE name = 'pgsink_bad_snk'
stalled "postgres: column a: type uint8 is not supported by Postgres sinks"

$ postgres-execute connection=postgres://postgres:postgres@postgres
CALL pgsink.wait_for('SELECT count(*)::text FROM pg_tables WHERE schemaname = ''pgsink'' AND tablename LIKE ''bad_target%''', '0');

> DROP SINK pgsink_bad_snk

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP PUBLICATION IF EXISTS pgsink_all_tables;
