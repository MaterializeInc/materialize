# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Postgres sinks on a cluster with more than one replica. The controller runs
# a sink on one replica at a time, and a replica that takes it over claims the
# sink's row in `materialize.sink_progress` and resumes from the frontier
# recorded there. The target has to end up holding each update exactly once,
# and the sink has to keep running while replicas come and go.
#
# The keyless sink is the one that catches a window applied twice, because it
# counts rows. The keyed sink moves each key to its final state, so applying a
# window again changes nothing or fails the sink, and the checks for a stalled
# sink cover the second case. The replicas have different worker counts so
# that the active worker for setup and apply differs between them.
#
# See postgres-sink.td for why `wait_for` is a procedure and has to be called
# on its own line.

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP SCHEMA IF EXISTS pgsink_mr CASCADE;
DROP SCHEMA IF EXISTS materialize CASCADE;
CREATE SCHEMA pgsink_mr;
CREATE PROCEDURE pgsink_mr.wait_for(query text, expected text) LANGUAGE plpgsql AS $$ DECLARE actual text; BEGIN FOR i IN 1..600 LOOP EXECUTE query INTO actual; IF actual IS NOT DISTINCT FROM expected THEN RETURN; END IF; COMMIT; PERFORM pg_sleep(0.1); END LOOP; RAISE EXCEPTION 'query % returned % but expected %', query, actual, expected; END $$;

> DROP CLUSTER IF EXISTS pgsink_mr_cluster CASCADE

> CREATE CLUSTER pgsink_mr_cluster REPLICAS (
    r1 (SIZE 'scale=1,workers=1'),
    r2 (SIZE 'scale=1,workers=2')
  )

> CREATE SECRET pgsink_mr_pass AS 'postgres'

> CREATE CONNECTION pgsink_mr_conn TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgsink_mr_pass
  )

> CREATE TABLE pgsink_mr_t (a int NOT NULL, b text)

> INSERT INTO pgsink_mr_t VALUES (1, 'one'), (2, 'two'), (3, 'three')

> CREATE SINK pgsink_mr_keyed
  IN CLUSTER pgsink_mr_cluster
  FROM pgsink_mr_t
  INTO POSTGRES CONNECTION pgsink_mr_conn (SCHEMA = 'pgsink_mr', TABLE = 'keyed')
  KEY (a) NOT ENFORCED

> CREATE SINK pgsink_mr_keyless
  IN CLUSTER pgsink_mr_cluster
  FROM pgsink_mr_t
  INTO POSTGRES CONNECTION pgsink_mr_conn (SCHEMA = 'pgsink_mr', TABLE = 'keyless')

$ set-from-sql var=keyed-id
SELECT id FROM mz_sinks WHERE name = 'pgsink_mr_keyed'

$ set-from-sql var=keyless-id
SELECT id FROM mz_sinks WHERE name = 'pgsink_mr_keyless'

# The snapshot lands once, not once per replica.

$ postgres-execute connection=postgres://postgres:postgres@postgres
CALL pgsink_mr.wait_for('SELECT string_agg(a || ''|'' || b, '' ; '' ORDER BY a) FROM pgsink_mr.keyed', '1|one ; 2|two ; 3|three');
CALL pgsink_mr.wait_for('SELECT string_agg(a || ''|'' || b, '' ; '' ORDER BY a) FROM pgsink_mr.keyless', '1|one ; 2|two ; 3|three');
CALL pgsink_mr.wait_for('SELECT string_agg(sink_id || '' '' || sink_schema || '' '' || sink_table, '', '' ORDER BY sink_table) FROM materialize.sink_progress', '${keyed-id} pgsink_mr keyed, ${keyless-id} pgsink_mr keyless');
CALL pgsink_mr.wait_for('SELECT string_agg(tablename, '', '' ORDER BY tablename) FROM pg_tables WHERE schemaname = ''pgsink_mr''', 'keyed, keyless');

> SELECT name, status FROM mz_internal.mz_sink_statuses WHERE name LIKE 'pgsink_mr_%'
pgsink_mr_keyed running
pgsink_mr_keyless running

# Updates spread over many timestamps give the replicas many windows to race
# on. Each row has to appear exactly once in the keyless target.

> INSERT INTO pgsink_mr_t VALUES (4, 'four')
> INSERT INTO pgsink_mr_t VALUES (5, 'five')
> UPDATE pgsink_mr_t SET b = 'uno' WHERE a = 1
> DELETE FROM pgsink_mr_t WHERE a = 2
> INSERT INTO pgsink_mr_t SELECT g, 'bulk' FROM generate_series(100, 199) AS g
> INSERT INTO pgsink_mr_t VALUES (6, 'six')
> DELETE FROM pgsink_mr_t WHERE a BETWEEN 150 AND 159

$ postgres-execute connection=postgres://postgres:postgres@postgres
CALL pgsink_mr.wait_for('SELECT string_agg(a || ''|'' || b, '' ; '' ORDER BY a) FROM pgsink_mr.keyed WHERE a < 100', '1|uno ; 3|three ; 4|four ; 5|five ; 6|six');
CALL pgsink_mr.wait_for('SELECT string_agg(a || ''|'' || b, '' ; '' ORDER BY a) FROM pgsink_mr.keyless WHERE a < 100', '1|uno ; 3|three ; 4|four ; 5|five ; 6|six');
CALL pgsink_mr.wait_for('SELECT count(*) || '' '' || count(DISTINCT a) FROM pgsink_mr.keyed WHERE a >= 100', '90 90');
CALL pgsink_mr.wait_for('SELECT count(*) || '' '' || count(DISTINCT a) FROM pgsink_mr.keyless WHERE a >= 100', '90 90');

# Replicas fighting over a sink fence each other off through the epoch in its
# progress row, which shows up as a halt on some replica even when the target
# eventually converges.

> SELECT count(*) FROM mz_internal.mz_sink_status_history h JOIN mz_sinks s ON s.id = h.sink_id WHERE s.name LIKE 'pgsink_mr_%' AND h.status = 'stalled'
0

# Losing a replica leaves the sink running on the other one.

> DROP CLUSTER REPLICA pgsink_mr_cluster.r1

> INSERT INTO pgsink_mr_t VALUES (7, 'after drop')

$ postgres-execute connection=postgres://postgres:postgres@postgres
CALL pgsink_mr.wait_for('SELECT string_agg(a || ''|'' || b, '' ; '' ORDER BY a) FROM pgsink_mr.keyless WHERE a = 7', '7|after drop');

# A replica joining a running sink must not re-apply windows the other replica
# already committed, nor fence it off.

> CREATE CLUSTER REPLICA pgsink_mr_cluster.r3 SIZE 'scale=1,workers=1'

> SELECT count(*) FROM mz_internal.mz_cluster_replica_statuses s JOIN mz_cluster_replicas r ON r.id = s.replica_id WHERE r.name = 'r3' AND s.status = 'online'
1

> INSERT INTO pgsink_mr_t VALUES (8, 'after add')
> INSERT INTO pgsink_mr_t VALUES (9, 'after add 2')

$ postgres-execute connection=postgres://postgres:postgres@postgres
CALL pgsink_mr.wait_for('SELECT string_agg(a || ''|'' || b, '' ; '' ORDER BY a) FROM pgsink_mr.keyless WHERE a < 100', '1|uno ; 3|three ; 4|four ; 5|five ; 6|six ; 7|after drop ; 8|after add ; 9|after add 2');
CALL pgsink_mr.wait_for('SELECT count(*) || '' '' || count(DISTINCT a) FROM pgsink_mr.keyless WHERE a >= 100', '90 90');
CALL pgsink_mr.wait_for('SELECT string_agg(a || ''|'' || b, '' ; '' ORDER BY a) FROM pgsink_mr.keyed WHERE a < 100', '1|uno ; 3|three ; 4|four ; 5|five ; 6|six ; 7|after drop ; 8|after add ; 9|after add 2');

> SELECT name, status FROM mz_internal.mz_sink_statuses WHERE name LIKE 'pgsink_mr_%'
pgsink_mr_keyed running
pgsink_mr_keyless running

> SELECT count(*) FROM mz_internal.mz_sink_status_history h JOIN mz_sinks s ON s.id = h.sink_id WHERE s.name LIKE 'pgsink_mr_%' AND h.status = 'stalled'
0

> DROP CLUSTER pgsink_mr_cluster CASCADE
