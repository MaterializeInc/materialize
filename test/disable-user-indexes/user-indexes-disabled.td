# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Note that this requires `user-indexes-enabled.td` to have run first to
# populate the catalog.

# 🔬 Views + Sources

# 🔬🔬 Successes

# System views are selectable
> SELECT count(*) FROM mz_databases
1

# Along with views derived from that view
> SELECT * FROM logging_derived
1

# ...even if the view was materialized, because it will share the logging
# table's arrangement
> SELECT * FROM logging_derived_mat
1

> SHOW INDEXES FROM logging_derived_mat;
on_name   key_name      seq_in_index  column_name  expression  nullable enabled
-------------------------------------------------------------------------------

# Views that embed a constant are selectable
> SELECT * FROM constant
1

# 🔬🔬 Failures

# Materialized views

# mat_view is not considered materialized
> SHOW MATERIALIZED VIEWS

! SELECT * FROM mat_view
Unable to automatically determine a timestamp for your query; this can happen if your query depends on non-materialized sources

> SHOW INDEXES FROM mat_view;
on_name   key_name              seq_in_index  column_name  expression  nullable enabled
---------------------------------------------------------------------------------------
mat_view  mat_view_primary_idx  1             sum          <null>      true     false
mat_view  mv_drop_idx           1             sum          <null>      true     false

> DROP INDEX mv_drop_idx;

> SHOW INDEXES FROM mat_view;
on_name   key_name              seq_in_index  column_name  expression  nullable enabled
---------------------------------------------------------------------------------------
mat_view  mat_view_primary_idx  1             sum          <null>      true     false

# Materialized sources

# mat_data is not considered materialized
> SHOW MATERIALIZED SOURCES

! SELECT * FROM mat_data
Unable to automatically determine a timestamp for your query; this can happen if your query depends on non-materialized sources

> SHOW INDEXES FROM mat_data;
on_name   key_name              seq_in_index  column_name  expression  nullable enabled
---------------------------------------------------------------------------------------
mat_data  mat_data_primary_idx  1             a            <null>      false    false
mat_data  ms_drop_idx           1             a            <null>      false    false

> DROP INDEX ms_drop_idx;

> SHOW INDEXES FROM mat_data;
on_name   key_name              seq_in_index  column_name  expression  nullable enabled
---------------------------------------------------------------------------------------
mat_data  mat_data_primary_idx  1             a            <null>      false    false

# 🔬 Sinks

> CREATE SINK snk_indexes_disabled FROM data
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'snk_indexes_disabled'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

# No data written to sink without indexes
$ kafka-verify format=avro sink=materialize.public.snk_indexes_enabled sort-messages=true

# 🔬 Tables

! INSERT INTO t VALUES (1)
cannot access materialize.public.t because it has no indexes enabled

# Selects work but are always empty
> SELECT * FROM t

> SHOW INDEXES FROM t;
on_name   key_name      seq_in_index  column_name  expression  nullable enabled
-----------------------------------------------------------------------------
t         t_primary_idx 1             a            <null>      true     false
t         t_drop_idx    1             a            <null>      true     false

# 🔬 Indexes

# Index are disabled upon creation
> CREATE INDEX t_secondary_idx ON t(a+a);

> SHOW INDEXES FROM t;
on_name   key_name        seq_in_index  column_name  expression  nullable enabled
-----------------------------------------------------------------------------
t         t_primary_idx   1             a            <null>      true     false
t         t_drop_idx      1             a            <null>      true     false
t         t_secondary_idx 1             <null>       "a + a"     true     false

> DROP INDEX t_drop_idx;

> SHOW INDEXES FROM t;
on_name   key_name        seq_in_index  column_name  expression  nullable enabled
-----------------------------------------------------------------------------
t         t_primary_idx   1             a            <null>      true     false
t         t_secondary_idx 1             <null>       "a + a"     true     false
