# Copyright Materialize, Inc. All rights reserved.
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

> SHOW INDEXES FROM mz_data
Source_or_view                Key_name                              Column_name  Expression  Null   Seq_in_index
----------------------------------------------------------------------------------------------------------------
materialize.public.mz_data  materialize.public.mz_data_primary_idx  a            <null>      false             1
materialize.public.mz_data  materialize.public.mz_data_primary_idx  b            <null>      false             2
materialize.public.mz_data  materialize.public.mz_data_primary_idx  mz_obj_no    <null>      false             3

# Non-materialized views do not have indexes automatically created
> CREATE SOURCE data
  FROM AVRO OCF '${testdrive.temp-dir}/data.ocf'

> SHOW INDEXES FROM data
Source_or_view            Key_name                              Column_name  Expression  Null  Seq_in_index
-----------------------------------------------------------------------------------------------------------

# Views can have default indexes added
> CREATE DEFAULT INDEX ON data

> SHOW INDEXES FROM data
Source_or_view           Key_name                             Column_name  Expression  Null   Seq_in_index
----------------------------------------------------------------------------------------------------------
materialize.public.data  materialize.public.data_primary_idx  a            <null>      false             1
materialize.public.data  materialize.public.data_primary_idx  b            <null>      false             2
materialize.public.data  materialize.public.data_primary_idx  mz_obj_no    <null>      false             3

> CREATE DEFAULT INDEX ON mz_data

> SHOW INDEXES FROM mz_data
Source_or_view                Key_name                               Column_name  Expression  Null   Seq_in_index
------------------------------------------------------------------------------------------------------------------
materialize.public.mz_data  materialize.public.mz_data_primary_idx   a            <null>      false             1
materialize.public.mz_data  materialize.public.mz_data_primary_idx   b            <null>      false             2
materialize.public.mz_data  materialize.public.mz_data_primary_idx   mz_obj_no    <null>      false             3
materialize.public.mz_data  materialize.public.mz_data_primary_idx1  a            <null>      false             1
materialize.public.mz_data  materialize.public.mz_data_primary_idx1  b            <null>      false             2
materialize.public.mz_data  materialize.public.mz_data_primary_idx1  mz_obj_no    <null>      false             3

# Materialized views are synonymous with having an index automatically created
> CREATE MATERIALIZED VIEW matv AS
  SELECT b, sum(a) FROM data GROUP BY b

> SHOW INDEXES FROM matv
Source_or_view            Key_name                              Column_name  Expression  Null  Seq_in_index
-----------------------------------------------------------------------------------------------------------
materialize.public.matv  materialize.public.matv_primary_idx  b            <null>      false             1

# Non-materialized views do not have indexes automatically created
> CREATE VIEW data_view as SELECT * from data

> SHOW INDEXES FROM data_view
Source_or_view            Key_name                              Column_name  Expression  Null  Seq_in_index
-----------------------------------------------------------------------------------------------------------

# Views can have default indexes added
> CREATE DEFAULT INDEX ON data_view

> SHOW INDEXES FROM data_view
Source_or_view                Key_name                                  Column_name  Expression  Null   Seq_in_index
--------------------------------------------------------------------------------------------------------------------
materialize.public.data_view  materialize.public.data_view_primary_idx  a            <null>      false             1
materialize.public.data_view  materialize.public.data_view_primary_idx  b            <null>      false             2
materialize.public.data_view  materialize.public.data_view_primary_idx  mz_obj_no    <null>      false             3

# Default indexes are equivalent in structure to indexes added automatically with the "MATERIALIZED" keyword
> CREATE MATERIALIZED VIEW mz_data_view as SELECT * from data

> SHOW INDEXES FROM mz_data_view
Source_or_view                   Key_name                                     Column_name  Expression  Null   Seq_in_index
--------------------------------------------------------------------------------------------------------------------------
materialize.public.mz_data_view  materialize.public.mz_data_view_primary_idx  a            <null>      false             1
materialize.public.mz_data_view  materialize.public.mz_data_view_primary_idx  b            <null>      false             2
materialize.public.mz_data_view  materialize.public.mz_data_view_primary_idx  mz_obj_no    <null>      false             3

# IF NOT EXISTS prevents adding multiple default indexes
> CREATE DEFAULT INDEX IF NOT EXISTS ON data_view

> SHOW INDEXES FROM data_view
Source_or_view                Key_name                                  Column_name  Expression  Null   Seq_in_index
--------------------------------------------------------------------------------------------------------------------
materialize.public.data_view  materialize.public.data_view_primary_idx  a            <null>      false             1
materialize.public.data_view  materialize.public.data_view_primary_idx  b            <null>      false             2
materialize.public.data_view  materialize.public.data_view_primary_idx  mz_obj_no    <null>      false             3

# IF NOT EXISTS works for both automatically and explicitly created default indexes
> CREATE DEFAULT INDEX IF NOT EXISTS ON matv

> SHOW INDEXES FROM matv
Source_or_view            Key_name                              Column_name  Expression  Null  Seq_in_index
-----------------------------------------------------------------------------------------------------------
materialize.public.matv  materialize.public.matv_primary_idx  b            <null>      false             1

# Additional default indexes have the same structure as the first
> CREATE DEFAULT INDEX ON matv

> SHOW INDEXES FROM matv
Source_or_view            Key_name                               Column_name  Expression  Null   Seq_in_index
-------------------------------------------------------------------------------------------------------------
materialize.public.matv  materialize.public.matv_primary_idx   b            <null>      false             1
materialize.public.matv  materialize.public.matv_primary_idx1  b            <null>      false             1

# Default indexes can be named
> CREATE DEFAULT INDEX named_idx ON data_view

> SHOW INDEXES FROM data_view
Source_or_view                Key_name                                  Column_name  Expression  Null   Seq_in_index
--------------------------------------------------------------------------------------------------------------------
materialize.public.data_view  materialize.public.data_view_primary_idx  a            <null>      false             1
materialize.public.data_view  materialize.public.data_view_primary_idx  b            <null>      false             2
materialize.public.data_view  materialize.public.data_view_primary_idx  mz_obj_no    <null>      false             3
materialize.public.data_view  materialize.public.named_idx              a            <null>      false             1
materialize.public.data_view  materialize.public.named_idx              b            <null>      false             2
materialize.public.data_view  materialize.public.named_idx              mz_obj_no    <null>      false             3

> DROP INDEX materialize.public.data_view_primary_idx
> DROP INDEX materialize.public.named_idx

# Indexes with specified columns can be automatically named
> CREATE INDEX ON data_view(a)

> SHOW INDEXES FROM data_view
Source_or_view                Key_name                                  Column_name  Expression  Null   Seq_in_index
--------------------------------------------------------------------------------------------------------------------
materialize.public.data_view  materialize.public.data_view_a_idx        a            <null>      false             1

> DROP INDEX materialize.public.data_view_a_idx

# Automatically named indexes rename expression columns to "expr" and join all columns with underscores.
> CREATE INDEX ON data_view(b, a)
> CREATE INDEX ON data_view(b - a, a)

> SHOW INDEXES FROM data_view
Source_or_view                Key_name                                  Column_name  Expression  Null   Seq_in_index
--------------------------------------------------------------------------------------------------------------------
materialize.public.data_view  materialize.public.data_view_b_a_idx      a            <null>      false             2
materialize.public.data_view  materialize.public.data_view_b_a_idx      b            <null>      false             1
materialize.public.data_view  materialize.public.data_view_expr_a_idx   <null>       "b - a"     false             1
materialize.public.data_view  materialize.public.data_view_expr_a_idx   a            <null>      false             2

> DROP INDEX materialize.public.data_view_b_a_idx
> DROP INDEX materialize.public.data_view_expr_a_idx

# Indexes can be both explicitly named and explicitly structured
> CREATE INDEX named_idx ON data_view (b - a, a)

> SHOW INDEXES FROM data_view
Source_or_view                Key_name                       Column_name  Expression  Null   Seq_in_index
---------------------------------------------------------------------------------------------------------
materialize.public.data_view  materialize.public.named_idx   <null>       "b - a"     false             1
materialize.public.data_view  materialize.public.named_idx   a            <null>      false             2

> DROP INDEX materialize.public.named_idx

# Default indexes only check for names, not structures
> CREATE INDEX data_view_primary_idx ON data_view (b - a, a)
> CREATE DEFAULT INDEX IF NOT EXISTS ON data_view

> SHOW INDEXES FROM data_view
Source_or_view                Key_name                                   Column_name  Expression  Null   Seq_in_index
---------------------------------------------------------------------------------------------------------------------
materialize.public.data_view  materialize.public.data_view_primary_idx   <null>       "b - a"     false             1
materialize.public.data_view  materialize.public.data_view_primary_idx   a            <null>      false             2
