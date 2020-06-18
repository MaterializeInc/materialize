# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test creating and dropping various views and sources that depend upon
# on another, and indices on those views and sources.

$ set schema={
    "type": "record",
    "name": "row",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "long"}
    ]
  }

$ kafka-create-topic topic=data

> CREATE SOURCE data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'

! SELECT * FROM data
Unable to automatically determine a timestamp for your query; this can happen if your query depends on non-materialized sources

> CREATE VIEW data_view as SELECT * from data

! SELECT * FROM data_view
Unable to automatically determine a timestamp for your query; this can happen if your query depends on non-materialized sources

> CREATE MATERIALIZED VIEW test1 AS
  SELECT b, sum(a) FROM data GROUP BY b

> SHOW FULL VIEWS
name        type     queryable   materialized
------------------------------------------------
data_view   USER     false       false
test1       USER     true        true

> SHOW MATERIALIZED VIEWS
test1

> SELECT * FROM test1
b  sum
------

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"a": 1, "b": 1}
{"a": 2, "b": 1}
{"a": 3, "b": 1}
{"a": 1, "b": 2}

> SELECT * FROM test1
b  sum
------
1  6
2  1

> SHOW COLUMNS FROM test1
Field Nullable Type
-------------------
b     NO       int8
sum   YES      int8

# Test default and unnamed indexes

> CREATE INDEX ON data_view

> SELECT b, sum(a) FROM data_view GROUP BY b;
b  sum
------
1  6
2  1

> SHOW INDEXES FROM data_view
Source_or_view                Key_name                                  Column_name  Expression  Null   Seq_in_index
--------------------------------------------------------------------------------------------------------------------
materialize.public.data_view  materialize.public.data_view_primary_idx  a            <null>      false             1
materialize.public.data_view  materialize.public.data_view_primary_idx  b            <null>      false             2

> CREATE INDEX IF NOT EXISTS ON data_view

> SHOW INDEXES FROM data_view
Source_or_view                Key_name                                  Column_name  Expression  Null   Seq_in_index
--------------------------------------------------------------------------------------------------------------------
materialize.public.data_view  materialize.public.data_view_primary_idx  a            <null>      false             1
materialize.public.data_view  materialize.public.data_view_primary_idx  b            <null>      false             2

> CREATE INDEX named_idx ON data_view

> SHOW INDEXES FROM data_view
Source_or_view                Key_name                                  Column_name  Expression  Null   Seq_in_index
--------------------------------------------------------------------------------------------------------------------
materialize.public.data_view  materialize.public.data_view_primary_idx  a            <null>      false             1
materialize.public.data_view  materialize.public.data_view_primary_idx  b            <null>      false             2
materialize.public.data_view  materialize.public.named_idx              a            <null>      false             1
materialize.public.data_view  materialize.public.named_idx              b            <null>      false             2

> CREATE INDEX ON data_view(a)

> CREATE INDEX IF NOT EXISTS ON data_view(a)

> SHOW INDEXES FROM data_view
Source_or_view                Key_name                                  Column_name  Expression  Null   Seq_in_index
--------------------------------------------------------------------------------------------------------------------
materialize.public.data_view  materialize.public.data_view_primary_idx  a            <null>      false             1
materialize.public.data_view  materialize.public.data_view_primary_idx  b            <null>      false             2
materialize.public.data_view  materialize.public.named_idx              a            <null>      false             1
materialize.public.data_view  materialize.public.named_idx              b            <null>      false             2
materialize.public.data_view  materialize.public.data_view_a_idx        a            <null>      false             1

> SHOW INDEXES FROM test1
Source_or_view            Key_name                              Column_name  Expression  Null  Seq_in_index
-----------------------------------------------------------------------------------------------------------
materialize.public.test1  materialize.public.test1_primary_idx  b            <null>      false             1

> CREATE INDEX ON test1

> SHOW INDEXES FROM test1
Source_or_view            Key_name                               Column_name  Expression  Null   Seq_in_index
-------------------------------------------------------------------------------------------------------------
materialize.public.test1  materialize.public.test1_primary_idx   b            <null>      false             1
materialize.public.test1  materialize.public.test1_primary_idx1  b            <null>      false             1
