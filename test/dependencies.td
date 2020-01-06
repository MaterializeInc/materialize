# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

# Test that a dependency chain with multiple links is properly maintained
# across creation and deletion.

$ set schema={
    "type": "record",
    "name": "envelope",
    "fields": [
      {
        "name": "before",
        "type": [
          {
            "name": "row",
            "type": "record",
            "fields": [
              {"name": "x", "type": "long"},
              {"name": "y", "type": "string"}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

> CREATE SOURCE s FROM 'kafka://${testdrive.kafka-addr}/testdrive-data-${testdrive.seed}' USING SCHEMA '${schema}'

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=33
{"before": null, "after": {"x": 1, "y": "mouse"}}
{"before": null, "after": {"x": 2, "y": "keyboard"}}
{"before": null, "after": {"x": 3, "y": "monitor"}}
{"before": null, "after": {"x": 1, "y": "keyboard"}}

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=34
{"before": null, "after": null}

> CREATE MATERIALIZED VIEW test1 AS SELECT 1;

> CREATE MATERIALIZED VIEW test2 AS SELECT * FROM test1;

> CREATE MATERIALIZED VIEW test3a AS SELECT * FROM test2;

> CREATE MATERIALIZED VIEW test3b AS SELECT * FROM test2;

! DROP VIEW test1;
cannot delete test1: still depended upon by catalog item 'test2'

! DROP VIEW test2;
cannot delete test2: still depended upon by catalog item 'test3a'

> DROP VIEW test3a;

! DROP VIEW test1;
cannot delete test1: still depended upon by catalog item 'test2'

! DROP VIEW test2;
cannot delete test2: still depended upon by catalog item 'test3b'

> DROP VIEW test3b;

! DROP VIEW test1;
cannot delete test1: still depended upon by catalog item 'test2'

> DROP VIEW test2;

> DROP VIEW test1;

# Test that CASCADE causes all dependent views to be dropped along with the
# named view.

> CREATE MATERIALIZED VIEW test1 AS SELECT 1;

> CREATE MATERIALIZED VIEW test2 AS SELECT * FROM test1;

> DROP VIEW test1 CASCADE;

# TODO(benesch): when we have a system stream that has the names of all
# available views, we can verify there that views are actually dropped,
# rather than verifying the drop by checking whether DROP VIEW fails.

! DROP VIEW test1;
catalog item 'test1' does not exist

! DROP VIEW test2;
catalog item 'test2' does not exist

# Test that DROP VIEW IF EXISTS succeeds even if the view does not exist.

> DROP VIEW IF EXISTS test1;

# Test that CASCADE causes all dependent views to be dropped along with the
# named source.

> CREATE VIEW test4 as SELECT * FROM s;

> DROP SOURCE s CASCADE;

! DROP VIEW test4;
catalog item 'test4' does not exist

> CREATE SOURCE s FROM 'kafka://${testdrive.kafka-addr}/testdrive-data-${testdrive.seed}' USING SCHEMA '${schema}'

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=33
{"before": null, "after": {"x": 1, "y": "mouse"}}
{"before": null, "after": {"x": 2, "y": "keyboard"}}
{"before": null, "after": {"x": 3, "y": "monitor"}}
{"before": null, "after": {"x": 1, "y": "keyboard"}}

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=34
{"before": null, "after": null}

# TODO@jldlaughlin: Add this back when we can drop sinks
# > CREATE SINK s1 FROM s INTO 'kafka://${testdrive.kafka-addr}/v' WITH (schema_registry_url = 'http://localhost:8081');

# Test that sinks cannot be depended upon.
# ! CREATE MATERIALIZED VIEW v2 AS SELECT * FROM s1;
# catalog item 's1' is a sink and so cannot be depended upon

> CREATE MATERIALIZED VIEW v2 AS SELECT X from s;

> CREATE MATERIALIZED VIEW v2a AS SELECT X+1 as X from v2;

> CREATE INDEX i1 ON v2(x);

# Test that indexes cannot be depended upon

! CREATE MATERIALIZED VIEW v3 as SELECT * FROM i1;
catalog item 'i1' is an index and so cannot be depended upon

> CREATE INDEX i2 ON v2a(x*2);

> SHOW INDEX in v2a;
View  Key_name              Column_name Expression Null  Seq_in_index
---------------------------------------------------------------------
v2a   i2                    <null>      "x * 2"    false 1
v2a   "\"v2a_PRIMARY_IDX\"" x           <null>     false 1

> SHOW INDEX in v2;
View  Key_name             Column_name Expression Null  Seq_in_index
--------------------------------------------------------------------
v2    i1                   x           <null>     false 1
v2    "\"v2_PRIMARY_IDX\"" x           <null>     false 1

# Test that dependent indexes do not prevent view deletion when restrict is specified
# but do not cause deletion of dependent views
> DROP VIEW v2a;

! DROP VIEW v2a;
catalog item 'v2a' does not exist

> SHOW INDEX in v2;
View  Key_name             Column_name Expression Null  Seq_in_index
--------------------------------------------------------------------
v2    i1                   x           <null>     false 1
v2    "\"v2_PRIMARY_IDX\"" x           <null>     false 1

! DROP INDEX i2;
catalog item 'i2' does not exist

> CREATE MATERIALIZED VIEW v4 AS SELECT x, y from s;

> CREATE MATERIALIZED VIEW v4a AS SELECT y from v4;

> CREATE INDEX i3 ON v4a(y);

> SHOW INDEX in v4a;
View  Key_name              Column_name Expression Null  Seq_in_index
---------------------------------------------------------------------
v4a   i3                    y           <null>     false 1
v4a   "\"v4a_PRIMARY_IDX\"" y           <null>     false 1

> CREATE INDEX i4 ON v4(x);

> SHOW INDEX in v4;
View  Key_name            Column_name Expression Null  Seq_in_index
---------------------------------------------------------------------
v4    i4                  x           <null>     false 1
v4   "\"v4_PRIMARY_IDX\"" x           <null>     false 1
v4   "\"v4_PRIMARY_IDX\"" y           <null>     false 2

# Test cascade deletes associated indexes as well
> DROP VIEW v4a CASCADE;

! DROP VIEW v4a;
catalog item 'v4a' does not exist

! DROP INDEX i3;
catalog item 'i3' does not exist

> SHOW INDEX in v4;
View  Key_name            Column_name Expression Null  Seq_in_index
-------------------------------------------------------------------
v4     i4                 x           <null>     false 1
v4   "\"v4_PRIMARY_IDX\"" x           <null>     false 1
v4   "\"v4_PRIMARY_IDX\"" y           <null>     false 2

> CREATE MATERIALIZED VIEW v5 AS SELECT substr(y, 3, 2) as substr from v4;

> CREATE INDEX i5 ON v5(substr);

> SHOW INDEX in v5;
View  Key_name            Column_name Expression Null  Seq_in_index
------------------------------------------------------------
v5    i5                  substr      <null>     true 1
v5   "\"v5_PRIMARY_IDX\"" substr      <null>     true 1

# Test cascade deletes all indexes associated with cascaded views
> DROP VIEW v4 CASCADE;

! DROP VIEW v4;
catalog item 'v4' does not exist

! DROP INDEX i5;
catalog item 'i5' does not exist

! DROP VIEW v5;
catalog item 'v5' does not exist

! DROP INDEX i4;
catalog item 'i4' does not exist

#cleanup
> DROP VIEW v2;

# TODO(@jldlaughlin): Implement DROP SINK and add these back
#> DROP SINK s1;
#> DROP SOURCE s;
