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

# 🔬 Boot into --disable-user-indexes

# 🔬🔬 Views + Sources

# 🔬🔬🔬 Successes

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
cluster on_name             key_name                        seq_in_index  column_name expression  nullable enabled
------------------------------------------------------------------------------------------------------------------
default logging_derived_mat logging_derived_mat_primary_idx 1             count       <null>      false    false

# Views that embed a constant are selectable
> SELECT * FROM constant
1

# 🔬🔬🔬 Failures

# 🔬🔬🔬🔬 Materialized views

# mat_view is not considered materialized
> SHOW MATERIALIZED VIEWS

! SELECT * FROM mat_view
contains:unable to automatically determine a query timestamp

> SHOW INDEXES FROM mat_view;
cluster on_name   key_name              seq_in_index  column_name  expression  nullable enabled
-----------------------------------------------------------------------------------------------
default mat_view  mat_view_primary_idx  1             sum          <null>      true     false
default mat_view  mv_drop_idx           1             sum          <null>      true     false

> DROP INDEX mv_drop_idx;

> SHOW INDEXES FROM mat_view;
cluster on_name   key_name              seq_in_index  column_name  expression  nullable enabled
-----------------------------------------------------------------------------------------------
default mat_view  mat_view_primary_idx  1             sum          <null>      true     false

# Cannot alter disabled index
! ALTER INDEX mat_view_primary_idx RESET (logical_compaction_window)
contains:invalid ALTER on disabled index "materialize.public.mat_view_primary_idx"

# 🔬🔬🔬🔬 Materialized sources

# mat_data is not considered materialized
> SHOW MATERIALIZED SOURCES

! SELECT * FROM mat_data
contains:unable to automatically determine a query timestamp

> SHOW INDEXES FROM mat_data;
cluster on_name   key_name              seq_in_index  column_name  expression  nullable enabled
-----------------------------------------------------------------------------------------------
default mat_data  mat_data_primary_idx  1             a            <null>      false    false
default mat_data  ms_drop_idx           1             a            <null>      false    false

> DROP INDEX ms_drop_idx;

> SHOW INDEXES FROM mat_data;
cluster on_name   key_name              seq_in_index  column_name  expression  nullable enabled
-----------------------------------------------------------------------------------------------
default mat_data  mat_data_primary_idx  1             a            <null>      false    false

# 🔬🔬🔬🔬 Non-materialized views

! SELECT * FROM join_view
contains:unable to automatically determine a query timestamp

# 🔬🔬 Sinks

> CREATE SINK IF NOT EXISTS snk_indexes_disabled FROM data
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'snk_indexes_disabled'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

# 🔬🔬 Tables

> INSERT INTO t VALUES (1)

# Selects work.
> SELECT * FROM t
1

> SHOW INDEXES FROM t;
cluster on_name   key_name      seq_in_index  column_name  expression  nullable enabled
-------------------------------------------------------------------------------------
default t         t_drop_idx    1             a            <null>      true     false

# 🔬🔬 Indexes

# Index are disabled upon creation
> CREATE INDEX t_secondary_idx ON t(a+a);

> SHOW INDEXES FROM t;
cluster on_name   key_name        seq_in_index  column_name  expression  nullable enabled
-------------------------------------------------------------------------------------
default t         t_drop_idx      1             a            <null>      true     false
default t         t_secondary_idx 1             <null>       "a + a"     true     false

> DROP INDEX t_drop_idx;

> SHOW INDEXES FROM t;
cluster on_name   key_name        seq_in_index  column_name  expression  nullable enabled
-------------------------------------------------------------------------------------
default t         t_secondary_idx 1             <null>       "a + a"     true     false

# 🔬 Enabling indexes

# 🔬🔬 Views + Sources

> ALTER INDEX logging_derived_mat_primary_idx SET ENABLED;

> SELECT * FROM logging_derived_mat
1

> SHOW INDEXES FROM logging_derived_mat;
cluster on_name             key_name                        seq_in_index  column_name expression  nullable enabled
------------------------------------------------------------------------------------------------------------------
default logging_derived_mat logging_derived_mat_primary_idx 1             count       <null>      false    true

# Alter index is functionally idempotent
> ALTER INDEX logging_derived_mat_primary_idx SET ENABLED;

> SELECT * FROM logging_derived_mat
1

# 🔬🔬🔬 Materialized views

> ALTER INDEX mat_view_primary_idx  SET ENABLED;

> SHOW INDEXES FROM mat_view;
cluster on_name   key_name              seq_in_index  column_name  expression  nullable enabled
-----------------------------------------------------------------------------------------------
default mat_view  mat_view_primary_idx  1             sum          <null>      true     true

> SHOW MATERIALIZED VIEWS
logging_derived_mat
mat_view

> SELECT * FROM mat_view
1

# 🔬🔬🔬 Materialized sources

> ALTER INDEX mat_data_primary_idx  SET ENABLED;

> SHOW INDEXES FROM mat_data;
cluster on_name   key_name              seq_in_index  column_name  expression  nullable enabled
-----------------------------------------------------------------------------------------------
default mat_data  mat_data_primary_idx  1             a            <null>      false    true

> SHOW MATERIALIZED SOURCES
mat_data

> SELECT * FROM mat_data
1

# 🔬🔬🔬 Non-materialized views

# Still cannot select from non-materialized view because some of its indexes are disabled

! SELECT * FROM join_view
contains:unable to automatically determine a query timestamp

> ALTER INDEX join_data_primary_idx SET ENABLED

> SELECT * FROM join_view;
1 a

# 🔬🔬 Sinks

# With indexes enabled, data starts flowing to sinks
$ kafka-verify format=avro sink=materialize.public.snk_indexes_disabled sort-messages=true
{"before": null, "after": {"row":{"a": 1}}}

# 🔬🔬 Tables

# Tables still work with non-primary indexes enabled because all indexes are covering
> SHOW INDEXES FROM t;
cluster on_name   key_name        seq_in_index  column_name  expression  nullable enabled
-------------------------------------------------------------------------------------
default t         t_secondary_idx 1             <null>       "a + a"     true     false

> INSERT INTO t VALUES (2)

> SELECT * FROM t
1
2

# Check idempotency
> ALTER INDEX t_secondary_idx SET ENABLED

> INSERT INTO t VALUES (3)

> SELECT * FROM t
1
2
3
