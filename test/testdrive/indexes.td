# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-regex match=cluster1|default|\[\d\] replacement=<VARIABLE_OUTPUT>

$ set writer-schema={
    "name": "row",
    "type": "record",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "int"}
    ]
  }

$ kafka-create-topic topic=data
$ kafka-ingest topic=data format=avro schema=${writer-schema}
{"a": 1, "b": 1}

# Materialized sources are synonymous with having an index automatically created
> CREATE MATERIALIZED SOURCE mz_data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${writer-schema}'

> SELECT index_position FROM mz_index_columns WHERE index_id LIKE '%u%'
index_position
--------------
1
2

> SELECT position, name FROM mz_columns where id LIKE '%u%';
position         name
----------------------
1                a
2                b

> SHOW INDEXES FROM mz_data
cluster on_name  key_name             seq_in_index  column_name  expression  nullable
---------------------------------------------------------------------------------------------
<VARIABLE_OUTPUT> mz_data  mz_data_primary_idx  1             a            <null>      false
<VARIABLE_OUTPUT> mz_data  mz_data_primary_idx  2             b            <null>      false

# Non-materialized views do not have indexes automatically created
> CREATE SOURCE data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${writer-schema}'

> SHOW INDEXES FROM data
cluster on_name  key_name  seq_in_index  column_name  expression  nullable
------------------------------------------------------------------------------------------

# Views can have default indexes added
> CREATE DEFAULT INDEX ON data

> SHOW INDEXES FROM data
cluster on_name  key_name          seq_in_index column_name  expression  nullable
----------------------------------------------------------------------------------------
<VARIABLE_OUTPUT> data     data_primary_idx  1            a            <null>      false
<VARIABLE_OUTPUT> data     data_primary_idx  2            b            <null>      false

> CREATE DEFAULT INDEX ON mz_data

> SHOW INDEXES FROM mz_data
cluster on_name  key_name              seq_in_index  column_name  expression  nullable
---------------------------------------------------------------------------------------------
<VARIABLE_OUTPUT> mz_data  mz_data_primary_idx   1             a            <null>      false
<VARIABLE_OUTPUT> mz_data  mz_data_primary_idx   2             b            <null>      false
<VARIABLE_OUTPUT> mz_data  mz_data_primary_idx1  1             a            <null>      false
<VARIABLE_OUTPUT> mz_data  mz_data_primary_idx1  2             b            <null>      false

# Materialized views are synonymous with having an index automatically created
> CREATE MATERIALIZED VIEW matv AS
  SELECT b, sum(a) FROM data GROUP BY b

> SHOW INDEXES FROM matv
cluster on_name   key_name          seq_in_index column_name  expression  nullable
-----------------------------------------------------------------------------------------
<VARIABLE_OUTPUT> matv      matv_primary_idx  1            b            <null>      false

# Non-materialized views do not have indexes automatically created
> CREATE VIEW data_view as SELECT * from data

> SHOW INDEXES FROM data_view
cluster on_name  key_name  seq_in_index  column_name  expression  nullable
------------------------------------------------------------------------------------------

# Views can have default indexes added
> CREATE DEFAULT INDEX ON data_view

> SHOW INDEXES FROM data_view
cluster on_name    key_name               seq_in_index  column_name  expression  nullable
-----------------------------------------------------------------------------------------------
<VARIABLE_OUTPUT> data_view  data_view_primary_idx  1             a            <null>      false
<VARIABLE_OUTPUT> data_view  data_view_primary_idx  2             b            <null>      false

# Default indexes are equivalent in structure to indexes added automatically with the "MATERIALIZED" keyword
> CREATE MATERIALIZED VIEW mz_data_view as SELECT * from data

> SHOW INDEXES FROM mz_data_view
cluster on_name       key_name                  seq_in_index  column_name  expression  nullable
-------------------------------------------------------------------------------------------------------
<VARIABLE_OUTPUT> mz_data_view  mz_data_view_primary_idx  1             a            <null>      false
<VARIABLE_OUTPUT> mz_data_view  mz_data_view_primary_idx  2             b            <null>      false

# IF NOT EXISTS prevents adding multiple default indexes
> CREATE DEFAULT INDEX IF NOT EXISTS ON data_view

> SHOW INDEXES FROM data_view
cluster on_name    key_name               seq_in_index  column_name  expression  nullable
-------------------------------------------------------------------------------------------------
<VARIABLE_OUTPUT> data_view  data_view_primary_idx  1             a            <null>      false
<VARIABLE_OUTPUT> data_view  data_view_primary_idx  2             b            <null>      false

# IF NOT EXISTS works for both automatically and explicitly created default indexes
> CREATE DEFAULT INDEX IF NOT EXISTS ON matv

> SHOW INDEXES FROM matv
cluster on_name  key_name          seq_in_index  column_name  expression  nullable
----------------------------------------------------------------------------------------------
<VARIABLE_OUTPUT> matv     matv_primary_idx  1             b            <null>      false

# Additional default indexes have the same structure as the first
> CREATE DEFAULT INDEX ON matv

> SHOW INDEXES FROM matv
cluster on_name  key_name           seq_in_index  column_name  expression  nullable
------------------------------------------------------------------------------------------------
<VARIABLE_OUTPUT> matv     matv_primary_idx   1             b            <null>      false
<VARIABLE_OUTPUT> matv     matv_primary_idx1  1             b            <null>      false

# Default indexes can be named
> CREATE DEFAULT INDEX named_idx ON data_view

> SHOW INDEXES FROM data_view
cluster on_name    key_name               seq_in_index  column_name  expression  nullable
-----------------------------------------------------------------------------------------------
<VARIABLE_OUTPUT> data_view  data_view_primary_idx  1             a            <null>      false
<VARIABLE_OUTPUT> data_view  data_view_primary_idx  2             b            <null>      false
<VARIABLE_OUTPUT> data_view  named_idx              1             a            <null>      false
<VARIABLE_OUTPUT> data_view  named_idx              2             b            <null>      false

> DROP INDEX data_view_primary_idx
> DROP INDEX named_idx

# Indexes with specified columns can be automatically named
> CREATE INDEX ON data_view(a)

> SHOW INDEXES FROM data_view
cluster on_name    key_name           seq_in_index  column_name  expression  nullable
-------------------------------------------------------------------------------------------
<VARIABLE_OUTPUT> data_view  data_view_a_idx    1             a            <null>      false

> DROP INDEX data_view_a_idx

# Automatically named indexes rename expression columns to "expr" and join all columns with underscores.
> CREATE INDEX ON data_view(b, a)
> CREATE INDEX ON data_view(b - a, a)

> SHOW INDEXES FROM data_view
cluster on_name    key_name               seq_in_index  column_name  expression  nullable
-----------------------------------------------------------------------------------------------
<VARIABLE_OUTPUT> data_view  data_view_b_a_idx      2             a            <null>      false
<VARIABLE_OUTPUT> data_view  data_view_b_a_idx      1             b            <null>      false
<VARIABLE_OUTPUT> data_view  data_view_expr_a_idx   1             <null>       "b - a"     false
<VARIABLE_OUTPUT> data_view  data_view_expr_a_idx   2             a            <null>      false

> DROP INDEX data_view_b_a_idx
> DROP INDEX data_view_expr_a_idx

# Indexes can be both explicitly named and explicitly structured
> CREATE INDEX named_idx ON data_view (b - a, a)

> SHOW INDEXES FROM data_view
cluster on_name    key_name    seq_in_index  column_name  expression  nullable
---------------------------------------------------------------------------------------------
<VARIABLE_OUTPUT> data_view  named_idx   1             <null>       "b - a"     false
<VARIABLE_OUTPUT> data_view  named_idx   2             a            <null>      false

> DROP INDEX named_idx

# Default indexes only check for names, not structures
> CREATE INDEX data_view_primary_idx ON data_view (b - a, a)
> CREATE DEFAULT INDEX IF NOT EXISTS ON data_view

> SHOW INDEXES FROM data_view
cluster on_name    key_name                seq_in_index  column_name  expression  nullable
------------------------------------------------------------------------------------------------------
<VARIABLE_OUTPUT> data_view  data_view_primary_idx   1             <null>       "b - a"     false
<VARIABLE_OUTPUT> data_view  data_view_primary_idx   2             a            <null>      false

> SHOW CREATE INDEX data_view_primary_idx
Index                                    "Create Index"
--------------------------------------------------------------------------------------------------------------------------------------
materialize.public.data_view_primary_idx "CREATE INDEX \"data_view_primary_idx\" IN CLUSTER <VARIABLE_OUTPUT> ON \"materialize\".\"public\".\"data_view\" (\"b\" - \"a\", \"a\")"

> CREATE TABLE foo (
    a int NOT NULL,
    b decimal(13, 1),
    z text
  )
> CREATE DEFAULT INDEX ON foo
> CREATE INDEX ON foo (a + b)
> CREATE INDEX ON foo (substr(z, 3))
> SHOW INDEXES FROM foo
<VARIABLE_OUTPUT> foo  foo_primary_idx   1  a       <null>                     false
<VARIABLE_OUTPUT> foo  foo_primary_idx   2  b       <null>                     true
<VARIABLE_OUTPUT> foo  foo_primary_idx   3  z       <null>                     true
<VARIABLE_OUTPUT> foo  foo_expr_idx      1  <null>  "a + b"                    true
<VARIABLE_OUTPUT> foo  foo_expr_idx1     1  <null>  "pg_catalog.substr(z, 3)"  true
> SHOW INDEXES FROM foo WHERE Column_name = 'b'
<VARIABLE_OUTPUT> foo  foo_primary_idx   2  b       <null>          true
> SHOW INDEXES FROM foo WHERE Column_name = 'noexist'
> SHOW INDEXES FROM foo WHERE Key_name = 'foo_expr_idx'
<VARIABLE_OUTPUT> foo  foo_expr_idx      1  <null>  "a + b"         true
# TODO(justin): not handled in parser yet:
#   SHOW INDEXES FROM v LIKE '%v'

! SHOW INDEX FROM nonexistent
contains:unknown catalog item 'nonexistent'

! SHOW INDEX FROM foo_primary_idx
contains:cannot show indexes on materialize.public.foo_primary_idx because it is a index

> CREATE CLUSTER clstr REPLICAS (r1 (REMOTE ('localhost:1234')))
> CREATE DEFAULT INDEX IN CLUSTER clstr ON foo;
> SHOW INDEXES IN CLUSTER clstr WHERE on_name = 'foo'
clstr foo  foo_primary_idx1   1  a       <null>                     false
clstr foo  foo_primary_idx1   2  b       <null>                     true
clstr foo  foo_primary_idx1   3  z       <null>                     true
