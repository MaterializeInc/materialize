# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that the knowledge that a given set of fields forms a primary key is used by the optimizer to remove grouping and distinct operations
#

# Remove both newlines, references to internal table identifiers, and "materialize.public" strings, all with a single regexp

$ set-regex match=(\s\(u\d+\)|\n|materialize\.public\.) replacement=

$ set keyschema-2keys={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "key1", "type": "string"},
        {"name": "key2", "type": "string"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"nokey", "type":"string"}
        ]
    }

$ kafka-create-topic topic=t1

$ kafka-ingest format=avro topic=t1 key-format=avro key-schema=${keyschema-2keys} schema=${schema} publish=true

> CREATE SOURCE t1
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-t1-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE UPSERT;

# Optimization is possible - no distinct is mentioned in the plan

> EXPLAIN SELECT DISTINCT key1, key2 FROM t1;
"%0 =| Get t1| Project (#0, #1)"

> EXPLAIN SELECT DISTINCT key2, key1 FROM t1;
"%0 =| Get t1| Project (#1, #0)"

> EXPLAIN SELECT DISTINCT key2, key1, key2 FROM t1;
"%0 =| Get t1| Project (#1, #0, #1)"

> EXPLAIN SELECT key2, key1 FROM t1 GROUP BY key1, key2;
"%0 =| Get t1| Project (#1, #0)"

> EXPLAIN SELECT key2, key1 FROM t1 GROUP BY key1, key2, key2 || 'a';
"%0 =| Get t1| Project (#1, #0)"

> EXPLAIN SELECT DISTINCT key1, key2, nokey FROM t1;
"%0 =| Get t1"

> EXPLAIN SELECT key1, key2, nokey FROM t1 GROUP BY key1, key2, nokey;
"%0 =| Get t1"

> EXPLAIN SELECT key1, key2 FROM t1 GROUP BY key1, key2 HAVING key1 = 'a';
"%0 =| Get t1| Map \"a\"| Filter (#0 = \"a\")| Project (#3, #1)"

# Optimization not possible - explicit distinct is present in plan

> EXPLAIN SELECT DISTINCT key1 FROM t1;
"%0 =| Get t1| Project (#0)| Distinct group=(#0)"

> EXPLAIN SELECT DISTINCT key2 FROM t1;
"%0 =| Get t1| Project (#1)| Distinct group=(#0)"

> EXPLAIN SELECT DISTINCT key1, upper(key2) FROM t1;
"%0 =| Get t1| Project (#0, #1)| Distinct group=(#0, upper(#1))"

> EXPLAIN SELECT DISTINCT key1, key2 || 'a' FROM t1;
"%0 =| Get t1| Project (#0, #1)| Distinct group=(#0, (#1 || \"a\"))"

> EXPLAIN SELECT key1 FROM t1 GROUP BY key1;
"%0 =| Get t1| Project (#0)| Distinct group=(#0)"

> EXPLAIN SELECT key2 FROM t1 GROUP BY key2;
"%0 =| Get t1| Project (#1)| Distinct group=(#0)"

> EXPLAIN SELECT COUNT(DISTINCT key1) FROM t1;
"%0 = Let l0 =| Get t1| Project (#0)| Reduce group=()| | agg count(distinct #0)%1 =| Get %0 (l0)| Project ()| Negate%2 =| Constant ()%3 =| Union %1 %2| Map 0%4 =| Union %0 %3"
# Make sure that primary key information is inherited from the source

> CREATE VIEW v1 AS SELECT * FROM t1;

> EXPLAIN SELECT DISTINCT key1, key2 FROM v1;
"%0 =| Get t1| Project (#0, #1)"

> CREATE MATERIALIZED VIEW v2 AS SELECT * FROM t1;

> EXPLAIN SELECT DISTINCT key1, key2 FROM v2;
"%0 =| Get v2| Project (#0, #1)"

# Make sure that having a DISTINCT or GROUP BY confers PK semantics on upstream views

> CREATE MATERIALIZED VIEW distinct_view AS SELECT DISTINCT nokey FROM t1;

> EXPLAIN SELECT DISTINCT nokey FROM distinct_view
"%0 =| Get distinct_view"

> CREATE MATERIALIZED VIEW group_by_view AS SELECT nokey || 'a' AS f1 , nokey || 'b' AS f2 FROM t1 GROUP BY nokey || 'a', nokey || 'b';

> EXPLAIN SELECT DISTINCT f1, f2 FROM group_by_view;
"%0 =| Get group_by_view"

# Redundant table is eliminated from an inner join using PK information

> EXPLAIN SELECT a1.* FROM t1 AS a1, t1 AS a2 WHERE a1.key1 = a2.key1 AND a1.key2 = a2.key2;
"%0 =| Get t1"

> EXPLAIN SELECT a1.* FROM v1 AS a1, v1 AS a2 WHERE a1.key1 = a2.key1 AND a1.key2 = a2.key2;
"%0 =| Get t1"

> EXPLAIN SELECT a1.* FROM v2 AS a1, v2 AS a2 WHERE a1.key1 = a2.key1 AND a1.key2 = a2.key2;
"%0 =| Get v2"

# Declare a key constraint (PRIMARY KEY NOT ENFORCED); otherwise identical tests as above.

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"key1","type":"string"},
            {"name":"key2","type":"string"},
            {"name":"nokey", "type":"string"}
        ]
    }

$ kafka-create-topic topic=t1-pkne

$ kafka-ingest format=avro topic=t1-pkne schema=${schema} publish=true

> CREATE SOURCE t1_pkne (PRIMARY KEY (key1, key2) NOT ENFORCED)
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-t1-pkne-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE NONE;

# Optimization is possible - no distinct is mentioned in the plan

> EXPLAIN SELECT DISTINCT key1, key2 FROM t1_pkne;
"%0 =| Get t1_pkne| Project (#0, #1)"

> EXPLAIN SELECT DISTINCT key2, key1 FROM t1_pkne;
"%0 =| Get t1_pkne| Project (#1, #0)"

> EXPLAIN SELECT DISTINCT key2, key1, key2 FROM t1_pkne;
"%0 =| Get t1_pkne| Project (#1, #0, #1)"

> EXPLAIN SELECT key2, key1 FROM t1_pkne GROUP BY key1, key2;
"%0 =| Get t1_pkne| Project (#1, #0)"

> EXPLAIN SELECT key2, key1 FROM t1_pkne GROUP BY key1, key2, key2 || 'a';
"%0 =| Get t1_pkne| Project (#1, #0)"

> EXPLAIN SELECT DISTINCT key1, key2, nokey FROM t1_pkne;
"%0 =| Get t1_pkne"

> EXPLAIN SELECT key1, key2, nokey FROM t1_pkne GROUP BY key1, key2, nokey;
"%0 =| Get t1_pkne"

> EXPLAIN SELECT key1, key2 FROM t1_pkne GROUP BY key1, key2 HAVING key1 = 'a';
"%0 =| Get t1_pkne| Map \"a\"| Filter (#0 = \"a\")| Project (#3, #1)"

# Optimization not possible - explicit distinct is present in plan

> EXPLAIN SELECT DISTINCT key1 FROM t1_pkne;
"%0 =| Get t1_pkne| Project (#0)| Distinct group=(#0)"

> EXPLAIN SELECT DISTINCT key2 FROM t1_pkne;
"%0 =| Get t1_pkne| Project (#1)| Distinct group=(#0)"

> EXPLAIN SELECT DISTINCT key1, upper(key2) FROM t1_pkne;
"%0 =| Get t1_pkne| Project (#0, #1)| Distinct group=(#0, upper(#1))"

> EXPLAIN SELECT DISTINCT key1, key2 || 'a' FROM t1_pkne;
"%0 =| Get t1_pkne| Project (#0, #1)| Distinct group=(#0, (#1 || \"a\"))"

> EXPLAIN SELECT key1 FROM t1_pkne GROUP BY key1;
"%0 =| Get t1_pkne| Project (#0)| Distinct group=(#0)"

> EXPLAIN SELECT key2 FROM t1_pkne GROUP BY key2;
"%0 =| Get t1_pkne| Project (#1)| Distinct group=(#0)"

> EXPLAIN SELECT COUNT(DISTINCT key1) FROM t1_pkne;
"%0 = Let l0 =| Get t1_pkne| Project (#0)| Reduce group=()| | agg count(distinct #0)%1 =| Get %0 (l0)| Project ()| Negate%2 =| Constant ()%3 =| Union %1 %2| Map 0%4 =| Union %0 %3"

# Make sure that primary key information is inherited from the source

> CREATE VIEW v1_pkne AS SELECT * FROM t1_pkne;

> EXPLAIN SELECT DISTINCT key1, key2 FROM v1_pkne;
"%0 =| Get t1_pkne| Project (#0, #1)"

> CREATE MATERIALIZED VIEW v2_pkne AS SELECT * FROM t1_pkne;

> EXPLAIN SELECT DISTINCT key1, key2 FROM v2_pkne;
"%0 =| Get v2_pkne| Project (#0, #1)"

# Make sure that having a DISTINCT or GROUP BY confers PK semantics on upstream views

> CREATE MATERIALIZED VIEW distinct_view_pkne AS SELECT DISTINCT nokey FROM t1_pkne;

> EXPLAIN SELECT DISTINCT nokey FROM distinct_view_pkne
"%0 =| Get distinct_view_pkne"

> CREATE MATERIALIZED VIEW group_by_view_pkne AS SELECT nokey || 'a' AS f1 , nokey || 'b' AS f2 FROM t1_pkne GROUP BY nokey || 'a', nokey || 'b';

> EXPLAIN SELECT DISTINCT f1, f2 FROM group_by_view_pkne;
"%0 =| Get group_by_view_pkne"

# Redundant table is eliminated from an inner join using PK information

> EXPLAIN SELECT a1.* FROM t1_pkne AS a1, t1_pkne AS a2 WHERE a1.key1 = a2.key1 AND a1.key2 = a2.key2;
"%0 =| Get t1_pkne"

> EXPLAIN SELECT a1.* FROM v1_pkne AS a1, v1_pkne AS a2 WHERE a1.key1 = a2.key1 AND a1.key2 = a2.key2;
"%0 =| Get t1_pkne"

> EXPLAIN SELECT a1.* FROM v2_pkne AS a1, v2_pkne AS a2 WHERE a1.key1 = a2.key1 AND a1.key2 = a2.key2;
"%0 =| Get v2_pkne"
