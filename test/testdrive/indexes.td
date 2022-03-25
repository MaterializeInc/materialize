# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set writer-schema={
    "name": "row",
    "type": "record",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "int"}
    ]
  }

$ avro-ocf-write path=data.ocf schema=${writer-schema} codec=null
{"a": 1, "b": 1}

# Materialized sources are synonymous with having an index automatically created
> CREATE MATERIALIZED SOURCE mz_data
  FROM AVRO OCF '${testdrive.temp-dir}/data.ocf'

> SELECT index_position FROM mz_index_columns WHERE index_id LIKE '%u%'
index_position
--------------
1
2
3

> SELECT position, name FROM mz_columns where id LIKE '%u%';
position         name
----------------------
1                a
2                b
3                mz_obj_no

> SHOW INDEXES FROM mz_data
cluster on_name  key_name             seq_in_index  column_name  expression  nullable enabled
--------------------------------------------------------------------------------------------------
default mz_data  mz_data_primary_idx  1             a            <null>      false    true
default mz_data  mz_data_primary_idx  2             b            <null>      false    true
default mz_data  mz_data_primary_idx  3             mz_obj_no    <null>      false    true

# Non-materialized views do not have indexes automatically created
> CREATE SOURCE data
  FROM AVRO OCF '${testdrive.temp-dir}/data.ocf'

> SHOW INDEXES FROM data
cluster on_name  key_name  seq_in_index  column_name  expression  nullable  enabled
--------------------------------------------------------------------------------------

# Views can have default indexes added
> CREATE DEFAULT INDEX ON data

> SHOW INDEXES FROM data
cluster on_name  key_name          seq_in_index column_name  expression  nullable enabled
----------------------------------------------------------------------------------------------
default data     data_primary_idx  1            a            <null>      false    true
default data     data_primary_idx  2            b            <null>      false    true
default data     data_primary_idx  3            mz_obj_no    <null>      false    true

> CREATE DEFAULT INDEX ON mz_data

> SHOW INDEXES FROM mz_data
cluster on_name  key_name              seq_in_index  column_name  expression  nullable  enabled
----------------------------------------------------------------------------------------------------
default mz_data  mz_data_primary_idx   1             a            <null>      false     true
default mz_data  mz_data_primary_idx   2             b            <null>      false     true
default mz_data  mz_data_primary_idx   3             mz_obj_no    <null>      false     true
default mz_data  mz_data_primary_idx1  1             a            <null>      false     true
default mz_data  mz_data_primary_idx1  2             b            <null>      false     true
default mz_data  mz_data_primary_idx1  3             mz_obj_no    <null>      false     true

# Materialized views are synonymous with having an index automatically created
> CREATE MATERIALIZED VIEW matv AS
  SELECT b, sum(a) FROM data GROUP BY b

> SHOW INDEXES FROM matv
cluster on_name   key_name          seq_in_index column_name  expression  nullable  enabled
----------------------------------------------------------------------------------------------
default matv      matv_primary_idx  1            b            <null>      false     true

# Non-materialized views do not have indexes automatically created
> CREATE VIEW data_view as SELECT * from data

> SHOW INDEXES FROM data_view
cluster on_name  key_name  seq_in_index  column_name  expression  nullable  enabled
--------------------------------------------------------------------------------------

# Views can have default indexes added
> CREATE DEFAULT INDEX ON data_view

> SHOW INDEXES FROM data_view
cluster on_name    key_name               seq_in_index  column_name  expression  nullable enabled
----------------------------------------------------------------------------------------------------
default data_view  data_view_primary_idx  1             a            <null>      false    true
default data_view  data_view_primary_idx  2             b            <null>      false    true
default data_view  data_view_primary_idx  3             mz_obj_no    <null>      false    true

# Default indexes are equivalent in structure to indexes added automatically with the "MATERIALIZED" keyword
> CREATE MATERIALIZED VIEW mz_data_view as SELECT * from data

> SHOW INDEXES FROM mz_data_view
cluster on_name       key_name                  seq_in_index  column_name  expression  nullable enabled
------------------------------------------------------------------------------------------------------------
default mz_data_view  mz_data_view_primary_idx  1             a            <null>      false    true
default mz_data_view  mz_data_view_primary_idx  2             b            <null>      false    true
default mz_data_view  mz_data_view_primary_idx  3             mz_obj_no    <null>      false    true

# IF NOT EXISTS prevents adding multiple default indexes
> CREATE DEFAULT INDEX IF NOT EXISTS ON data_view

> SHOW INDEXES FROM data_view
cluster on_name    key_name               seq_in_index  column_name  expression  nullable enabled
------------------------------------------------------------------------------------------------------
default data_view  data_view_primary_idx  1             a            <null>      false    true
default data_view  data_view_primary_idx  2             b            <null>      false    true
default data_view  data_view_primary_idx  3             mz_obj_no    <null>      false    true

# IF NOT EXISTS works for both automatically and explicitly created default indexes
> CREATE DEFAULT INDEX IF NOT EXISTS ON matv

> SHOW INDEXES FROM matv
cluster on_name  key_name          seq_in_index  column_name  expression  nullable  enabled
----------------------------------------------------------------------------------------------
default matv     matv_primary_idx  1             b            <null>      false     true

# Additional default indexes have the same structure as the first
> CREATE DEFAULT INDEX ON matv

> SHOW INDEXES FROM matv
cluster on_name  key_name           seq_in_index  column_name  expression  nullable enabled
------------------------------------------------------------------------------------------------
default matv     matv_primary_idx   1             b            <null>      false    true
default matv     matv_primary_idx1  1             b            <null>      false    true

# Default indexes can be named
> CREATE DEFAULT INDEX named_idx ON data_view

> SHOW INDEXES FROM data_view
cluster on_name    key_name               seq_in_index  column_name  expression  nullable enabled
----------------------------------------------------------------------------------------------------
default data_view  data_view_primary_idx  1             a            <null>      false    true
default data_view  data_view_primary_idx  2             b            <null>      false    true
default data_view  data_view_primary_idx  3             mz_obj_no    <null>      false    true
default data_view  named_idx              1             a            <null>      false    true
default data_view  named_idx              2             b            <null>      false    true
default data_view  named_idx              3             mz_obj_no    <null>      false    true

> DROP INDEX data_view_primary_idx
> DROP INDEX named_idx

# Indexes with specified columns can be automatically named
> CREATE INDEX ON data_view(a)

> SHOW INDEXES FROM data_view
cluster on_name    key_name           seq_in_index  column_name  expression  nullable enabled
------------------------------------------------------------------------------------------------
default data_view  data_view_a_idx    1             a            <null>      false    true

> DROP INDEX data_view_a_idx

# Automatically named indexes rename expression columns to "expr" and join all columns with underscores.
> CREATE INDEX ON data_view(b, a)
> CREATE INDEX ON data_view(b - a, a)

> SHOW INDEXES FROM data_view
cluster on_name    key_name               seq_in_index  column_name  expression  nullable enabled
----------------------------------------------------------------------------------------------------
default data_view  data_view_b_a_idx      2             a            <null>      false    true
default data_view  data_view_b_a_idx      1             b            <null>      false    true
default data_view  data_view_expr_a_idx   1             <null>       "b - a"     false    true
default data_view  data_view_expr_a_idx   2             a            <null>      false    true

> DROP INDEX data_view_b_a_idx
> DROP INDEX data_view_expr_a_idx

# Indexes can be both explicitly named and explicitly structured
> CREATE INDEX named_idx ON data_view (b - a, a)

> SHOW INDEXES FROM data_view
cluster on_name    key_name    seq_in_index  column_name  expression  nullable  enabled
-----------------------------------------------------------------------------------------
default data_view  named_idx   1             <null>       "b - a"     false     true
default data_view  named_idx   2             a            <null>      false     true

> DROP INDEX named_idx

# Default indexes only check for names, not structures
> CREATE INDEX data_view_primary_idx ON data_view (b - a, a)
> CREATE DEFAULT INDEX IF NOT EXISTS ON data_view

> SHOW INDEXES FROM data_view
cluster on_name    key_name                seq_in_index  column_name  expression  nullable  enabled
------------------------------------------------------------------------------------------------------
default data_view  data_view_primary_idx   1             <null>       "b - a"     false     true
default data_view  data_view_primary_idx   2             a            <null>      false     true

> SHOW CREATE INDEX data_view_primary_idx
Index                                    "Create Index"
--------------------------------------------------------------------------------------------------------------------------------------
materialize.public.data_view_primary_idx "CREATE INDEX \"data_view_primary_idx\" IN CLUSTER [1] ON \"materialize\".\"public\".\"data_view\" (\"b\" - \"a\", \"a\")"

> CREATE TABLE foo (
    a int NOT NULL,
    b decimal(13, 1),
    z text
  )
> CREATE DEFAULT INDEX ON foo
> CREATE INDEX ON foo (a + b)
> CREATE INDEX ON foo (substr(z, 3))
> SHOW INDEXES FROM foo
default foo  foo_primary_idx   1  a       <null>                     false  true
default foo  foo_primary_idx   2  b       <null>                     true   true
default foo  foo_primary_idx   3  z       <null>                     true   true
default foo  foo_expr_idx      1  <null>  "a + b"                    true   true
default foo  foo_expr_idx1     1  <null>  "pg_catalog.substr(z, 3)"  true   true
> SHOW INDEXES FROM foo WHERE Column_name = 'b'
default foo  foo_primary_idx   2  b       <null>          true  true
> SHOW INDEXES FROM foo WHERE Column_name = 'noexist'
> SHOW INDEXES FROM foo WHERE Key_name = 'foo_expr_idx'
default foo  foo_expr_idx      1  <null>  "a + b"         true  true
# TODO(justin): not handled in parser yet:
#   SHOW INDEXES FROM v LIKE '%v'

! SHOW INDEX FROM nonexistent
contains:unknown catalog item 'nonexistent'

! SHOW INDEX FROM foo_primary_idx
contains:cannot show indexes on materialize.public.foo_primary_idx because it is a index

> CREATE CLUSTER clstr
> CREATE DEFAULT INDEX IN CLUSTER clstr ON foo;
> SHOW INDEXES IN CLUSTER clstr
clstr foo  foo_primary_idx1   1  a       <null>                     false  true
clstr foo  foo_primary_idx1   2  b       <null>                     true   true
clstr foo  foo_primary_idx1   3  z       <null>                     true   true
