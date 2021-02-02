# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE VIEW v AS VALUES (1, 'foo'), (2, 'bar'), (3, 'foo'), (1, 'bar')

> SELECT * FROM v
1 "foo"
1 "bar"
2 "bar"
3 "foo"

> CREATE TEMPORARY VIEW temp_v AS SELECT * FROM v

> SELECT * FROM temp_v
1 "foo"
1 "bar"
2 "bar"
3 "foo"

> SELECT * FROM mz_temp.temp_v
1 "foo"
1 "bar"
2 "bar"
3 "foo"

! CREATE VIEW non_temp AS SELECT * FROM temp_v
non-temporary items cannot depend on temporary item

> CREATE TEMP VIEW double_temp_v AS SELECT * FROM temp_v

! CREATE TEMP VIEW double_temp_v AS SELECT * FROM temp_v
catalog item 'double_temp_v' already exists

> CREATE OR REPLACE TEMP VIEW double_temp_v AS SELECT * FROM temp_v

! CREATE OR REPLACE VIEW double_temp_v AS SELECT * FROM temp_v
non-temporary items cannot depend on temporary item

> SELECT * FROM double_temp_v
1 "foo"
1 "bar"
2 "bar"
3 "foo"

> SELECT * FROM mz_temp.double_temp_v
1 "foo"
1 "bar"
2 "bar"
3 "foo"

> CREATE TEMPORARY MATERIALIZED VIEW foo AS SELECT * FROM v

> SHOW INDEXES FROM foo
 on_name    key_name         seq_in_index  column_name  expression  nullable
---------------------------------------------------------------------------
 foo        foo_primary_idx  1             column1      <null>      false
 foo        foo_primary_idx  2             column2      <null>      false

! CREATE TEMP MATERIALIZED VIEW foo AS SELECT * FROM v
catalog item 'foo' already exists

> CREATE OR REPLACE TEMPORARY MATERIALIZED VIEW foo AS SELECT * FROM v

#####################################################################
# Test things we shouldn't be able to make temporary.

##### Temporary sources.
$ set schema={
    "type": "record",
    "name": "envelope",
    "fields": [{},]
  }

! CREATE TEMPORARY SOURCE data_schema_inline
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM
Expected VIEW or MATERIALIZED VIEW after CREATE TEMPORARY, found SOURCE


##### Temporary sinks.
! CREATE TEMPORARY SINK data_sink FROM data
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'data-sink'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
Expected VIEW or MATERIALIZED VIEW after CREATE TEMPORARY, found SINK

#####################################################################

! DROP VIEW temp_v;
cannot drop mz_temp.temp_v: still depended upon by catalog item 'mz_temp.double_temp_v'

> DROP VIEW double_temp_v;

! SELECT * FROM double_temp_v;
unknown catalog item 'double_temp_v'

> DISCARD TEMP

> SELECT * FROM mz_indexes WHERE name = 'foo_primary_idx'

! SELECT * FROM temp_v;
unknown catalog item 'temp_v'

! CREATE TEMP VIEW mz_foo.a AS SELECT 1
cannot create temporary item in non-temporary schema
