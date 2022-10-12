# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-regex match=cluster1|default replacement=<VARIABLE_OUTPUT>

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

# Sources do not have indexes automatically created
> CREATE CONNECTION kafka_conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}';

> CREATE SOURCE data
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${writer-schema}'

> SHOW INDEXES ON data
name    on  cluster key
--------------------------------------------------------------------------

# Sources can have default indexes added
> CREATE DEFAULT INDEX ON data

> SHOW INDEXES ON data
name                on      cluster             key
-------------------------------------------------------------------------------------------
data_primary_idx    data    <VARIABLE_OUTPUT>   {a,b}

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

# Views do not have indexes automatically created
> CREATE VIEW data_view as SELECT * from data

> SHOW INDEXES ON data_view
name    on  cluster key
--------------------------------------------------------------------------

# Views can have default indexes added
> CREATE DEFAULT INDEX ON data_view

> SHOW INDEXES ON data_view
name                    on          cluster             key
---------------------------------------------------------------------------------------------------
data_view_primary_idx   data_view   <VARIABLE_OUTPUT>   {a,b}

# Materialized views do not have indexes automatically created
> CREATE MATERIALIZED VIEW matv AS
  SELECT b, sum(a) FROM data GROUP BY b

> SHOW INDEXES ON matv
name    on  cluster key
--------------------------------------------------------------------------

# Materialized views can have default indexes added
> CREATE DEFAULT INDEX ON matv

> SHOW INDEXES ON matv
name                on      cluster             key
--------------------------------------------------------------------------------------------
matv_primary_idx    matv    <VARIABLE_OUTPUT>   {b}

# IF NOT EXISTS prevents adding multiple default indexes
> CREATE DEFAULT INDEX IF NOT EXISTS ON data_view

> SHOW INDEXES ON data_view
name                    on          cluster             key
-------------------------------------------------------------------------------------------------
data_view_primary_idx   data_view   <VARIABLE_OUTPUT>   {a,b}

# Additional default indexes have the same structure as the first
> CREATE DEFAULT INDEX ON matv

> SHOW INDEXES ON matv
name                on      cluster             key
------------------------------------------------------------------------------------------------
matv_primary_idx    matv    <VARIABLE_OUTPUT>   {b}
matv_primary_idx1   matv    <VARIABLE_OUTPUT>   {b}

# Default indexes can be named
> CREATE DEFAULT INDEX named_idx ON data_view

> SHOW INDEXES ON data_view
name                    on          cluster             key
-----------------------------------------------------------------------------------------------
data_view_primary_idx   data_view   <VARIABLE_OUTPUT>   {a,b}
named_idx               data_view   <VARIABLE_OUTPUT>   {a,b}

> DROP INDEX data_view_primary_idx
> DROP INDEX named_idx

# Indexes with specified columns can be automatically named
> CREATE INDEX ON data_view(a)

> SHOW INDEXES ON data_view
name            on          cluster             key
-------------------------------------------------------------------------------------------
data_view_a_idx data_view   <VARIABLE_OUTPUT>   {a}

> DROP INDEX data_view_a_idx

# Automatically named indexes rename expression columns to "expr" and join all columns with underscores.
> CREATE INDEX ON data_view(b, a)
> CREATE INDEX ON data_view(b - a, a)

> SHOW INDEXES ON data_view
name                    on          cluster             key
-----------------------------------------------------------------------------------------------
data_view_b_a_idx       data_view   <VARIABLE_OUTPUT>   {b,a}
data_view_expr_a_idx    data_view   <VARIABLE_OUTPUT>   "{b - a,a}"

> DROP INDEX data_view_b_a_idx
> DROP INDEX data_view_expr_a_idx

# Indexes can be both explicitly named and explicitly structured
> CREATE INDEX named_idx ON data_view (b - a, a)

> SHOW INDEXES ON data_view
name        on          cluster             key
---------------------------------------------------------------------------------------------
named_idx   data_view   <VARIABLE_OUTPUT>   "{b - a,a}"

> DROP INDEX named_idx

# Default indexes only check for names, not structures
> CREATE INDEX data_view_primary_idx ON data_view (b - a, a)
> CREATE DEFAULT INDEX IF NOT EXISTS ON data_view

> SHOW INDEXES ON data_view
name                    on          cluster             key
------------------------------------------------------------------------------------------------------
data_view_primary_idx   data_view   <VARIABLE_OUTPUT>   "{b - a,a}"

> SHOW CREATE INDEX data_view_primary_idx
name                                     create_sql
--------------------------------------------------------------------------------------------------------------------------------------
materialize.public.data_view_primary_idx "CREATE INDEX \"data_view_primary_idx\" IN CLUSTER \"<VARIABLE_OUTPUT>\" ON \"materialize\".\"public\".\"data_view\" (\"b\" - \"a\", \"a\")"

> CREATE TABLE foo (
    a int NOT NULL,
    b decimal(13, 1),
    z text
  )
> CREATE DEFAULT INDEX ON foo
> CREATE INDEX ON foo (a + b)
> CREATE INDEX ON foo (substr(z, 3))
> SHOW INDEXES ON foo
foo_primary_idx foo <VARIABLE_OUTPUT>   {a,b,z}
foo_expr_idx    foo <VARIABLE_OUTPUT>   "{a + b}"
foo_expr_idx1   foo <VARIABLE_OUTPUT>   "{pg_catalog.substr(z, 3)}"
> SHOW INDEXES ON foo WHERE name = 'noexist'
> SHOW INDEXES ON foo WHERE name = 'foo_expr_idx'
foo_expr_idx    foo <VARIABLE_OUTPUT>   "{a + b}"

> SHOW INDEXES ON foo LIKE 'foo_primary%'
foo_primary_idx foo <VARIABLE_OUTPUT>   {a,b,z}

! SHOW INDEXES ON nonexistent
contains:unknown catalog item 'nonexistent'

! SHOW INDEXES ON foo_primary_idx
contains:cannot show indexes on materialize.public.foo_primary_idx because it is a index

> CREATE CLUSTER clstr REPLICAS (r1 (REMOTE ['localhost:1234']))
> CREATE DEFAULT INDEX IN CLUSTER clstr ON foo;
> SHOW INDEXES IN CLUSTER clstr WHERE on = 'foo'
foo_primary_idx1    foo clstr   {a,b,z}

> SHOW INDEXES FROM public WHERE name = 'foo_primary_idx1'
foo_primary_idx1    foo clstr   {a,b,z}

> DROP TABLE foo CASCADE
> DROP SOURCE data CASCADE

! SHOW INDEXES FROM public ON foo
contains:Cannot specify both FROM and ON

! SHOW INDEXES FROM nonexistent
contains:unknown schema 'nonexistent'

> CREATE SCHEMA foo
> CREATE TABLE foo.bar (a INT)
> CREATE INDEX bar_ind ON foo.bar (a)

> SHOW INDEXES ON foo.bar
bar_ind bar <VARIABLE_OUTPUT> {a}
