# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

# Postgres sinks copy every update into a staging table tagged with the
# Materialize timestamp and a diff of 1 or -1, then move each completed
# timestamp into the target table. Assertions on the Postgres side go through
# `pgsink.wait_for`, which polls until a query returns the expected text,
# because the sink works asynchronously.
#
# `wait_for` is a procedure rather than a function so that it can COMMIT
# between polls. A function would hold its transaction, and with it ACCESS
# SHARE on the staging table, for the whole poll loop, which blocks the
# TRUNCATE a restarting sink runs and turns a failed assertion into a minutes
# long stall. Each CALL has to be the only statement on its line, since
# multiple statements in one line share an implicit transaction that a COMMIT
# inside the procedure is not allowed to end.

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP SCHEMA IF EXISTS pgsink CASCADE;
DROP SCHEMA IF EXISTS materialize CASCADE;
CREATE SCHEMA pgsink;
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

# Postgres sinks have no envelope. KEY is what the user chooses when they want
# the target table keyed.
> CREATE SINK pgsink_snk
  IN CLUSTER ${arg.single-replica-cluster}
  FROM pgsink_t
  INTO POSTGRES CONNECTION pgsink_conn (SCHEMA = 'pgsink', TABLE = 'target')
  KEY (a) NOT ENFORCED

$ set-from-sql var=sink-id
SELECT id FROM mz_sinks WHERE name = 'pgsink_snk'

> SELECT status FROM mz_internal.mz_sink_statuses WHERE name = 'pgsink_snk'
running

# The target table mirrors the source relation and the staging table adds the
# indexed timestamp and diff columns.

$ postgres-execute connection=postgres://postgres:postgres@postgres
SET TIME ZONE 'UTC';
CALL pgsink.wait_for('SELECT string_agg(attname || '' '' || format_type(atttypid, atttypmod) || CASE WHEN attnotnull THEN '' not null'' ELSE '''' END, '', '' ORDER BY attnum) FROM pg_attribute WHERE attrelid = ''pgsink.target''::regclass AND attnum > 0 AND NOT attisdropped', 'a integer not null, b text, c numeric(39,2), d timestamp with time zone, e jsonb, f uuid, g integer[]');
CALL pgsink.wait_for('SELECT string_agg(attname || '' '' || format_type(atttypid, atttypmod) || CASE WHEN attnotnull THEN '' not null'' ELSE '''' END, '', '' ORDER BY attnum) FROM pg_attribute WHERE attrelid = ''pgsink.target_mz_staging_${sink-id}''::regclass AND attnum > 0 AND NOT attisdropped', 'a integer not null, b text, c numeric(39,2), d timestamp with time zone, e jsonb, f uuid, g integer[], mz_timestamp bigint not null, mz_diff smallint not null');
CALL pgsink.wait_for('SELECT string_agg(indexname, '', '' ORDER BY indexname) FROM pg_indexes WHERE schemaname = ''pgsink'' AND tablename = ''target_mz_staging_${sink-id}''', 'target_mz_staging_${sink-id}_mz_diff_idx, target_mz_staging_${sink-id}_mz_timestamp_idx');

# Every column round-trips into the target, including NULLs and text with
# tabs, newlines, backslashes and quotes. Staging drains as windows are
# applied, and the recorded frontier moves off zero.

$ postgres-execute connection=postgres://postgres:postgres@postgres
CALL pgsink.wait_for('SELECT string_agg(a || ''|'' || coalesce(replace(replace(b, chr(9), ''<TAB>''), chr(10), ''<NL>''), ''<NULL>'') || ''|'' || coalesce(c::text, ''<NULL>'') || ''|'' || coalesce(d::text, ''<NULL>'') || ''|'' || coalesce(e::text, ''<NULL>'') || ''|'' || coalesce(f::text, ''<NULL>'') || ''|'' || coalesce(g::text, ''<NULL>''), '' ; '' ORDER BY a) FROM pgsink.target', '1|one|1.50|2024-01-01 00:00:00+00|{"k": 1}|00000000-0000-0000-0000-000000000001|{1,2} ; 2|tab<TAB>nl<NL>bs\ quote''s|-2.25|<NULL>|<NULL>|<NULL>|<NULL> ; 3|<NULL>|<NULL>|<NULL>|null|<NULL>|{}');
CALL pgsink.wait_for('SELECT count(*)::text FROM pgsink.target_mz_staging_${sink-id}', '0');
CALL pgsink.wait_for('SELECT (mz_frontier > 0)::text FROM materialize.sink_progress WHERE sink_id = ''${sink-id}''', 'true');
CALL pgsink.wait_for('SELECT string_agg(sink_id || '' '' || sink_schema || '' '' || sink_table, '', '') FROM materialize.sink_progress', '${sink-id} pgsink target');

> SELECT write_frontier > 0 FROM mz_internal.mz_frontiers WHERE object_id = '${sink-id}'
true

# An update replaces the row for its key, a delete removes it, and inserting
# the same key twice leaves one row: the sink is an upsert, so the key the
# user declared decides how many rows the target holds, even though the
# Materialize table holds two.

> UPDATE pgsink_t SET b = 'uno' WHERE a = 1

> DELETE FROM pgsink_t WHERE a = 3

> INSERT INTO pgsink_t VALUES
  (4, 'dup', NULL, NULL, NULL, NULL, NULL),
  (4, 'dup', NULL, NULL, NULL, NULL, NULL)

$ postgres-execute connection=postgres://postgres:postgres@postgres
CALL pgsink.wait_for('SELECT string_agg(a || ''|'' || coalesce(replace(replace(b, chr(9), ''<TAB>''), chr(10), ''<NL>''), ''<NULL>''), '' ; '' ORDER BY a, b) FROM pgsink.target', '1|uno ; 2|tab<TAB>nl<NL>bs\ quote''s ; 4|dup');
CALL pgsink.wait_for('SELECT count(*)::text FROM pgsink.target_mz_staging_${sink-id}', '0');

# An update puts a retraction and an insertion for one key at the same
# timestamp, so a single window holds both and the apply statement's ordering
# has to pick the insertion. One row, with the new value.

> UPDATE pgsink_t SET b = 'second' WHERE a = 1

$ postgres-execute connection=postgres://postgres:postgres@postgres
CALL pgsink.wait_for('SELECT string_agg(a || ''|'' || coalesce(b, ''<NULL>''), '' ; '' ORDER BY a, b) FROM pgsink.target WHERE a = 1', '1|second');
CALL pgsink.wait_for('SELECT count(*)::text FROM pgsink.target WHERE a = 1', '1');

# A sink without a KEY matches retractions on every column and counts them, so
# a genuine duplicate row survives retracting one copy of it. The view projects
# away the id, which is the only way to retract one of two identical rows.

> CREATE TABLE pgsink_kl_src (id int, v text)

> INSERT INTO pgsink_kl_src VALUES (1, 'dup'), (2, 'dup'), (3, 'solo')

> CREATE MATERIALIZED VIEW pgsink_kl AS SELECT v FROM pgsink_kl_src

> CREATE SINK pgsink_kl_snk
  IN CLUSTER ${arg.single-replica-cluster}
  FROM pgsink_kl
  INTO POSTGRES CONNECTION pgsink_conn (SCHEMA = 'pgsink', TABLE = 'keyless_target')

$ set-from-sql var=kl-sink-id
SELECT id FROM mz_sinks WHERE name = 'pgsink_kl_snk'

$ postgres-execute connection=postgres://postgres:postgres@postgres
CALL pgsink.wait_for('SELECT string_agg(v, '' ; '' ORDER BY v) FROM pgsink.keyless_target', 'dup ; dup ; solo');

> DELETE FROM pgsink_kl_src WHERE id = 3

# Retract one of the two identical rows. The other has to remain.
> DELETE FROM pgsink_kl_src WHERE id = 1

$ postgres-execute connection=postgres://postgres:postgres@postgres
CALL pgsink.wait_for('SELECT string_agg(v, '' ; '' ORDER BY v) FROM pgsink.keyless_target', 'dup');
CALL pgsink.wait_for('SELECT count(*)::text FROM pgsink.keyless_target_mz_staging_${kl-sink-id}', '0');

> SELECT status FROM mz_internal.mz_sink_statuses WHERE name = 'pgsink_kl_snk'
running

> DROP SINK pgsink_kl_snk

# Restarting the sink truncates the staging table before copying resumes, and
# the rows already applied are not applied again.

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO pgsink.target_mz_staging_${sink-id} (a, mz_timestamp, mz_diff) VALUES (999, 0, 1);

$ postgres-execute connection=mz_system
ALTER CLUSTER ${arg.single-replica-cluster} SET (REPLICATION FACTOR 0);
ALTER CLUSTER ${arg.single-replica-cluster} SET (REPLICATION FACTOR 1);

> INSERT INTO pgsink_t VALUES (5, 'after restart', NULL, NULL, NULL, NULL, NULL)

$ postgres-execute connection=postgres://postgres:postgres@postgres
CALL pgsink.wait_for('SELECT string_agg(a || ''|'' || coalesce(b, ''<NULL>''), '' ; '' ORDER BY a, b) FROM pgsink.target WHERE a IN (1, 4, 5, 999)', '1|second ; 4|dup ; 5|after restart');
CALL pgsink.wait_for('SELECT count(*)::text FROM pgsink.target_mz_staging_${sink-id}', '0');

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

# A recorded frontier behind the shard means the target database lost writes
# the sink already committed, and those rows can no longer be replayed. The
# sink has to say so rather than silently skip them.

$ postgres-execute connection=mz_system
ALTER CLUSTER ${arg.single-replica-cluster} SET (REPLICATION FACTOR 0);

$ postgres-execute connection=postgres://postgres:postgres@postgres
UPDATE materialize.sink_progress SET mz_frontier = 0 WHERE sink_id = '${sink-id}';

$ postgres-execute connection=mz_system
ALTER CLUSTER ${arg.single-replica-cluster} SET (REPLICATION FACTOR 1);

> SELECT status FROM mz_internal.mz_sink_statuses WHERE name = 'pgsink_snk'
stalled

> SELECT error ~ 'is behind its progress shard' FROM mz_internal.mz_sink_statuses WHERE name = 'pgsink_snk'
true

> DROP SINK pgsink_snk

# There is no envelope to state, so stating one is an error whichever it is.

! CREATE SINK pgsink_envelope_snk
  IN CLUSTER ${arg.single-replica-cluster}
  FROM pgsink_t
  INTO POSTGRES CONNECTION pgsink_conn (SCHEMA = 'pgsink', TABLE = 'envelope_target')
  ENVELOPE UPSERT
contains:ENVELOPE is not supported for Postgres sinks

! CREATE SINK pgsink_envelope_snk
  IN CLUSTER ${arg.single-replica-cluster}
  FROM pgsink_t
  INTO POSTGRES CONNECTION pgsink_conn (SCHEMA = 'pgsink', TABLE = 'envelope_target')
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
