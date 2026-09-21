# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

# Postgres sinks copy every update into a staging table in the target
# database, tagged with the Materialize timestamp and a diff of 1 or -1.
# The assertions on the Postgres side go through `pgsink.wait_for`, which
# polls a query until it returns the expected text, because the sink copies
# asynchronously.

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP SCHEMA IF EXISTS pgsink CASCADE;
DROP SCHEMA IF EXISTS materialize CASCADE;
CREATE SCHEMA pgsink;
CREATE FUNCTION pgsink.wait_for(query text, expected text) RETURNS void LANGUAGE plpgsql AS $$ DECLARE actual text; BEGIN FOR i IN 1..600 LOOP EXECUTE query INTO actual; IF actual IS NOT DISTINCT FROM expected THEN RETURN; END IF; PERFORM pg_sleep(0.1); END LOOP; RAISE EXCEPTION 'query % returned % but expected %', query, actual, expected; END $$;

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

> CREATE SINK pgsink_snk
  IN CLUSTER ${arg.single-replica-cluster}
  FROM pgsink_t
  INTO POSTGRES CONNECTION pgsink_conn (SCHEMA = 'pgsink', TABLE = 'target')
  KEY (a) NOT ENFORCED
  ENVELOPE UPSERT

$ set-from-sql var=sink-id
SELECT id FROM mz_sinks WHERE name = 'pgsink_snk'

> SELECT status FROM mz_internal.mz_sink_statuses WHERE name = 'pgsink_snk'
running

# The target table mirrors the source relation, the staging table adds the
# indexed timestamp and diff columns, and the progress table has one row for
# the sink.

$ postgres-execute connection=postgres://postgres:postgres@postgres
SET TIME ZONE 'UTC';
SELECT pgsink.wait_for('SELECT count(*)::text FROM pgsink.target_mz_staging_${sink-id}', '3');
SELECT pgsink.wait_for('SELECT string_agg(attname || '' '' || format_type(atttypid, atttypmod) || CASE WHEN attnotnull THEN '' not null'' ELSE '''' END, '', '' ORDER BY attnum) FROM pg_attribute WHERE attrelid = ''pgsink.target''::regclass AND attnum > 0 AND NOT attisdropped', 'a integer not null, b text, c numeric(39,2), d timestamp with time zone, e jsonb, f uuid, g integer[]');
SELECT pgsink.wait_for('SELECT string_agg(attname || '' '' || format_type(atttypid, atttypmod) || CASE WHEN attnotnull THEN '' not null'' ELSE '''' END, '', '' ORDER BY attnum) FROM pg_attribute WHERE attrelid = ''pgsink.target_mz_staging_${sink-id}''::regclass AND attnum > 0 AND NOT attisdropped', 'a integer not null, b text, c numeric(39,2), d timestamp with time zone, e jsonb, f uuid, g integer[], mz_timestamp bigint not null, mz_diff smallint not null');
SELECT pgsink.wait_for('SELECT string_agg(indexname, '', '' ORDER BY indexname) FROM pg_indexes WHERE schemaname = ''pgsink'' AND tablename = ''target_mz_staging_${sink-id}''', 'target_mz_staging_${sink-id}_mz_diff_idx, target_mz_staging_${sink-id}_mz_timestamp_idx');
SELECT pgsink.wait_for('SELECT count(*)::text FROM pgsink.target', '0');
SELECT pgsink.wait_for('SELECT string_agg(sink_id || '' '' || sink_schema || '' '' || sink_table || '' '' || mz_frontier, '', '') FROM materialize.sink_progress', '${sink-id} pgsink target 0');
SELECT pgsink.wait_for('SELECT count(DISTINCT mz_timestamp)::text FROM pgsink.target_mz_staging_${sink-id}', '1');
SELECT pgsink.wait_for('SELECT string_agg(a || ''|'' || coalesce(replace(replace(b, chr(9), ''<TAB>''), chr(10), ''<NL>''), ''<NULL>'') || ''|'' || coalesce(c::text, ''<NULL>'') || ''|'' || coalesce(d::text, ''<NULL>'') || ''|'' || coalesce(e::text, ''<NULL>'') || ''|'' || coalesce(f::text, ''<NULL>'') || ''|'' || coalesce(g::text, ''<NULL>'') || ''|'' || mz_diff, '' ; '' ORDER BY a) FROM pgsink.target_mz_staging_${sink-id}', '1|one|1.50|2024-01-01 00:00:00+00|{"k": 1}|00000000-0000-0000-0000-000000000001|{1,2}|1 ; 2|tab<TAB>nl<NL>bs\ quote''s|-2.25|<NULL>|<NULL>|<NULL>|<NULL>|1 ; 3|<NULL>|<NULL>|<NULL>|null|<NULL>|{}|1');

# An update produces a retraction and an insertion at the same timestamp, a
# delete produces a retraction, and an update with multiplicity two produces
# two staging rows with a diff of 1 each.

> UPDATE pgsink_t SET b = 'uno' WHERE a = 1

> DELETE FROM pgsink_t WHERE a = 3

> INSERT INTO pgsink_t VALUES
  (4, 'dup', NULL, NULL, NULL, NULL, NULL),
  (4, 'dup', NULL, NULL, NULL, NULL, NULL)

$ postgres-execute connection=postgres://postgres:postgres@postgres
SELECT pgsink.wait_for('SELECT string_agg(a || ''|'' || coalesce(replace(replace(b, chr(9), ''<TAB>''), chr(10), ''<NL>''), ''<NULL>'') || ''|'' || mz_diff, '' ; '' ORDER BY mz_timestamp, a, mz_diff, b) FROM pgsink.target_mz_staging_${sink-id}', '1|one|1 ; 2|tab<TAB>nl<NL>bs\ quote''s|1 ; 3|<NULL>|1 ; 1|one|-1 ; 1|uno|1 ; 3|<NULL>|-1 ; 4|dup|1 ; 4|dup|1');
SELECT pgsink.wait_for('SELECT bool_and(mz_diff IN (1, -1))::text FROM pgsink.target_mz_staging_${sink-id}', 'true');
SELECT pgsink.wait_for('SELECT (count(DISTINCT mz_timestamp) BETWEEN 2 AND 4)::text FROM pgsink.target_mz_staging_${sink-id}', 'true');
SELECT pgsink.wait_for('SELECT (min(mz_timestamp) FILTER (WHERE mz_diff = -1) > min(mz_timestamp))::text FROM pgsink.target_mz_staging_${sink-id}', 'true');

# Restarting the sink truncates the staging table before copying resumes.

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO pgsink.target_mz_staging_${sink-id} (a, mz_timestamp, mz_diff) VALUES (999, 0, 1);

$ postgres-execute connection=mz_system
ALTER CLUSTER ${arg.single-replica-cluster} SET (REPLICATION FACTOR 0);
ALTER CLUSTER ${arg.single-replica-cluster} SET (REPLICATION FACTOR 1);

> INSERT INTO pgsink_t VALUES (5, 'after restart', NULL, NULL, NULL, NULL, NULL)

$ postgres-execute connection=postgres://postgres:postgres@postgres
SELECT pgsink.wait_for('SELECT count(*)::text FROM pgsink.target_mz_staging_${sink-id} WHERE a = 5', '1');
SELECT pgsink.wait_for('SELECT count(*)::text FROM pgsink.target_mz_staging_${sink-id} WHERE a = 999', '0');
SELECT pgsink.wait_for('SELECT string_agg(sink_id || '' '' || sink_schema || '' '' || sink_table || '' '' || mz_frontier, '', '') FROM materialize.sink_progress', '${sink-id} pgsink target 0');

> SELECT status FROM mz_internal.mz_sink_statuses WHERE name = 'pgsink_snk'
running

# Column types that stock PostgreSQL does not have are rejected before any
# table is created.

> CREATE TABLE pgsink_bad (a uint8)

> CREATE SINK pgsink_bad_snk
  IN CLUSTER ${arg.single-replica-cluster}
  FROM pgsink_bad
  INTO POSTGRES CONNECTION pgsink_conn (SCHEMA = 'pgsink', TABLE = 'bad_target')
  KEY (a) NOT ENFORCED
  ENVELOPE UPSERT

> SELECT status, error FROM mz_internal.mz_sink_statuses WHERE name = 'pgsink_bad_snk'
stalled "column a: type uint8 is not supported by Postgres sinks"

$ postgres-execute connection=postgres://postgres:postgres@postgres
SELECT pgsink.wait_for('SELECT count(*)::text FROM pg_tables WHERE schemaname = ''pgsink'' AND tablename LIKE ''bad_target%''', '0');

> DROP SINK pgsink_bad_snk

> DROP SINK pgsink_snk
