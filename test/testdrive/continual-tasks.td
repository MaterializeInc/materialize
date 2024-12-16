# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default default-storage-size=1

$ kafka-create-topic topic=append_only partitions=1

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CLUSTER ct_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE append_only
  IN CLUSTER ct_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-append_only-${testdrive.seed}')

> CREATE TABLE append_only_tbl FROM SOURCE append_only (REFERENCE "testdrive-append_only-${testdrive.seed}")
  KEY FORMAT TEXT
  VALUE FORMAT TEXT
  INCLUDE KEY
  ENVELOPE NONE

$ kafka-ingest topic=append_only format=bytes key-format=bytes key-terminator=:
x:0

# Wait for the ingestion to have completed before creating the CT, we want the x:0
# value to be in the continual task, since it only starts after it is ingested already.
> SELECT * FROM append_only_tbl
x 0

> CREATE CONTINUAL TASK ct_max FROM TRANSFORM append_only_tbl USING
    (SELECT MAX(text::int) FROM append_only_tbl)

$ kafka-ingest topic=append_only format=bytes key-format=bytes key-terminator=:
a:1

> SELECT * FROM ct_max
0
1

$ kafka-ingest topic=append_only format=bytes key-format=bytes key-terminator=:
b:2

> SELECT * FROM ct_max
0
1
2

$ kafka-ingest topic=append_only format=bytes key-format=bytes key-terminator=:
c:3

> SELECT * FROM ct_max
0
1
2
3

$ kafka-ingest topic=append_only format=bytes key-format=bytes key-terminator=:
d:1

> SELECT * FROM ct_max
0
1
2
3

! INSERT INTO ct_max VALUES (1)
contains: cannot insert into continual task 'materialize.public.ct_max'

! UPDATE ct_max SET value = 0
contains: cannot mutate continual task 'materialize.public.ct_max'

! DELETE FROM ct_max
contains: cannot mutate continual task 'materialize.public.ct_max'

! DROP TABLE append_only_tbl;
contains: cannot drop table "append_only_tbl": still depended upon by continual task "ct_max"

> DROP TABLE append_only_tbl CASCADE;

> CREATE VIEW v AS SELECT 1 AS value;

! CREATE CONTINUAL TASK ct_max FROM TRANSFORM v USING (SELECT MIN() FROM v)
contains: cannot use view as an input

> CREATE MATERIALIZED VIEW mv AS SELECT 1 AS value;

> CREATE CONTINUAL TASK ct_min FROM TRANSFORM mv USING (SELECT MIN(value) FROM mv)

> SELECT * FROM ct_min
1

> CREATE TABLE t (c int)

> CREATE CONTINUAL TASK ct_table FROM TRANSFORM t USING (SELECT c FROM t)

> CREATE CONTINUAL TASK ct_table2 FROM TRANSFORM ct_table USING (SELECT c FROM ct_table)

! CREATE CONTINUAL TASK ct_table_recursive FROM TRANSFORM ct_table_recursive USING (SELECT c FROM ct_table_recursive);
contains: non-user item

> CREATE MATERIALIZED VIEW mv_ct AS SELECT * FROM ct_table2

> CREATE CONTINUAL TASK ct_table3 FROM TRANSFORM mv_ct USING (SELECT c FROM mv_ct)

> INSERT INTO t VALUES (1)

> SELECT * FROM ct_table
1

> SELECT * FROM ct_table2
1

> SELECT * FROM ct_table3
1

> SELECT * FROM mv_ct;
1

> DELETE FROM t

> INSERT INTO t VALUES (2)

> SELECT * FROM ct_table
1
2

> SELECT * FROM ct_table2
1
2

> SELECT * FROM ct_table3
1
2

> SELECT * FROM mv_ct;
1
2
