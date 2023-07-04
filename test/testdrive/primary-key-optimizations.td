# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_table_keys = true
ALTER SYSTEM SET enable_primary_key_not_enforced = true

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

$ kafka-ingest format=avro topic=t1 key-format=avro key-schema=${keyschema-2keys} schema=${schema}

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE SOURCE t1
  FROM KAFKA CONNECTION kafka_conn (TOPIC
  'testdrive-t1-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;

> CREATE DEFAULT INDEX ON t1

# Optimization is possible - no distinct is mentioned in the plan

> EXPLAIN SELECT DISTINCT key1, key2 FROM t1;
"Explained Query (fast path):  Project (#0, #1)    ReadExistingIndex t1_primary_idxUsed Indexes:  - t1_primary_idx"

> EXPLAIN SELECT DISTINCT key2, key1 FROM t1;
"Explained Query (fast path):  Project (#1, #0)    ReadExistingIndex t1_primary_idxUsed Indexes:  - t1_primary_idx"

> EXPLAIN SELECT DISTINCT key2, key1, key2 FROM t1;
"Explained Query (fast path):  Project (#1, #0, #1)    ReadExistingIndex t1_primary_idxUsed Indexes:  - t1_primary_idx"

> EXPLAIN SELECT key2, key1 FROM t1 GROUP BY key1, key2;
"Explained Query (fast path):  Project (#1, #0)    ReadExistingIndex t1_primary_idxUsed Indexes:  - t1_primary_idx"

> EXPLAIN SELECT key2, key1 FROM t1 GROUP BY key1, key2, key2 || 'a';
"Explained Query (fast path):  Project (#1, #0)    ReadExistingIndex t1_primary_idxUsed Indexes:  - t1_primary_idx"

> EXPLAIN SELECT DISTINCT key1, key2, nokey FROM t1;
"Explained Query (fast path):  ReadExistingIndex t1_primary_idxUsed Indexes:  - t1_primary_idx"

> EXPLAIN SELECT key1, key2, nokey FROM t1 GROUP BY key1, key2, nokey;
"Explained Query (fast path):  ReadExistingIndex t1_primary_idxUsed Indexes:  - t1_primary_idx"

> EXPLAIN SELECT key1, key2 FROM t1 GROUP BY key1, key2 HAVING key1 = 'a';
"Explained Query (fast path):  Project (#3, #1)    Filter (#0 = \"a\")      Map (\"a\")        ReadExistingIndex t1_primary_idxUsed Indexes:  - t1_primary_idx"

# Optimization not possible - explicit distinct is present in planFor certain types of tests the 

> EXPLAIN SELECT DISTINCT key1 FROM t1;
"Explained Query:  Distinct group_by=[#0]    Project (#0)      Get t1Used Indexes:  - t1_primary_idx"

> EXPLAIN SELECT DISTINCT key2 FROM t1;
"Explained Query:  Distinct group_by=[#0]    Project (#1)      Get t1Used Indexes:  - t1_primary_idx"

> EXPLAIN SELECT DISTINCT key1, upper(key2) FROM t1;
"Explained Query:  Distinct group_by=[#0, upper(#1)]    Project (#0, #1)      Get t1Used Indexes:  - t1_primary_idx"

> EXPLAIN SELECT DISTINCT key1, key2 || 'a' FROM t1;
"Explained Query:  Distinct group_by=[#0, (#1 || \"a\")]    Project (#0, #1)      Get t1Used Indexes:  - t1_primary_idx"

> EXPLAIN SELECT key1 FROM t1 GROUP BY key1;
"Explained Query:  Distinct group_by=[#0]    Project (#0)      Get t1Used Indexes:  - t1_primary_idx"

> EXPLAIN SELECT key2 FROM t1 GROUP BY key2;
"Explained Query:  Distinct group_by=[#0]    Project (#1)      Get t1Used Indexes:  - t1_primary_idx"

> EXPLAIN SELECT COUNT(DISTINCT key1) FROM t1;
"Explained Query:  Return    Union      Get l0      Map (0)        Union          Negate            Project ()              Get l0          Constant            - ()  With    cte l0 =      Reduce aggregates=[count(distinct #0)]        Project (#0)          Get t1Used Indexes:  - t1_primary_idx"
# Make sure that primary key information is inherited from the source

> CREATE VIEW v1 AS SELECT * FROM t1;

> EXPLAIN SELECT DISTINCT key1, key2 FROM v1;
"Explained Query (fast path):  Project (#0, #1)    ReadExistingIndex t1_primary_idxUsed Indexes:  - t1_primary_idx"

> CREATE VIEW v2 AS SELECT * FROM t1;
> CREATE DEFAULT INDEX ON v2;

> EXPLAIN SELECT DISTINCT key1, key2 FROM v2;
"Explained Query (fast path):  Project (#0, #1)    ReadExistingIndex v2_primary_idxUsed Indexes:  - v2_primary_idx"

# Make sure that having a DISTINCT or GROUP BY confers PK semantics on upstream views

> CREATE VIEW distinct_view AS SELECT DISTINCT nokey FROM t1;
> CREATE DEFAULT INDEX ON distinct_view;

> EXPLAIN SELECT DISTINCT nokey FROM distinct_view
"Explained Query (fast path):  ReadExistingIndex distinct_view_primary_idxUsed Indexes:  - distinct_view_primary_idx"

> CREATE VIEW group_by_view AS SELECT nokey || 'a' AS f1 , nokey || 'b' AS f2 FROM t1 GROUP BY nokey || 'a', nokey || 'b';
> CREATE DEFAULT INDEX ON group_by_view;

> EXPLAIN SELECT DISTINCT f1, f2 FROM group_by_view;
"Explained Query (fast path):  ReadExistingIndex group_by_view_primary_idxUsed Indexes:  - group_by_view_primary_idx"

# Redundant table is eliminated from an inner join using PK information

> EXPLAIN SELECT a1.* FROM t1 AS a1, t1 AS a2 WHERE a1.key1 = a2.key1 AND a1.key2 = a2.key2;
"Explained Query (fast path):  ReadExistingIndex t1_primary_idxUsed Indexes:  - t1_primary_idx"

> EXPLAIN SELECT a1.* FROM v1 AS a1, v1 AS a2 WHERE a1.key1 = a2.key1 AND a1.key2 = a2.key2;
"Explained Query (fast path):  ReadExistingIndex t1_primary_idxUsed Indexes:  - t1_primary_idx"

> EXPLAIN SELECT a1.* FROM v2 AS a1, v2 AS a2 WHERE a1.key1 = a2.key1 AND a1.key2 = a2.key2;
"Explained Query (fast path):  ReadExistingIndex v2_primary_idxUsed Indexes:  - v2_primary_idx"

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

$ kafka-ingest format=avro topic=t1-pkne schema=${schema}

> CREATE SOURCE t1_pkne (PRIMARY KEY (key1, key2) NOT ENFORCED)
  FROM KAFKA CONNECTION kafka_conn (TOPIC
  'testdrive-t1-pkne-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE;

> CREATE DEFAULT INDEX ON t1_pkne

# Optimization is possible - no distinct is mentioned in the plan

> EXPLAIN SELECT DISTINCT key1, key2 FROM t1_pkne;
"Explained Query (fast path):  Project (#0, #1)    ReadExistingIndex t1_pkne_primary_idxUsed Indexes:  - t1_pkne_primary_idx"

> EXPLAIN SELECT DISTINCT key2, key1 FROM t1_pkne;
"Explained Query (fast path):  Project (#1, #0)    ReadExistingIndex t1_pkne_primary_idxUsed Indexes:  - t1_pkne_primary_idx"

> EXPLAIN SELECT DISTINCT key2, key1, key2 FROM t1_pkne;
"Explained Query (fast path):  Project (#1, #0, #1)    ReadExistingIndex t1_pkne_primary_idxUsed Indexes:  - t1_pkne_primary_idx"

> EXPLAIN SELECT key2, key1 FROM t1_pkne GROUP BY key1, key2;
"Explained Query (fast path):  Project (#1, #0)    ReadExistingIndex t1_pkne_primary_idxUsed Indexes:  - t1_pkne_primary_idx"

> EXPLAIN SELECT key2, key1 FROM t1_pkne GROUP BY key1, key2, key2 || 'a';
"Explained Query (fast path):  Project (#1, #0)    ReadExistingIndex t1_pkne_primary_idxUsed Indexes:  - t1_pkne_primary_idx"

> EXPLAIN SELECT DISTINCT key1, key2, nokey FROM t1_pkne;
"Explained Query (fast path):  ReadExistingIndex t1_pkne_primary_idxUsed Indexes:  - t1_pkne_primary_idx"

> EXPLAIN SELECT key1, key2, nokey FROM t1_pkne GROUP BY key1, key2, nokey;
"Explained Query (fast path):  ReadExistingIndex t1_pkne_primary_idxUsed Indexes:  - t1_pkne_primary_idx"

> EXPLAIN SELECT key1, key2 FROM t1_pkne GROUP BY key1, key2 HAVING key1 = 'a';
"Explained Query (fast path):  Project (#3, #1)    Filter (#0 = \"a\")      Map (\"a\")        ReadExistingIndex t1_pkne_primary_idxUsed Indexes:  - t1_pkne_primary_idx"

# Optimization not possible - explicit distinct is present in plan

> EXPLAIN SELECT DISTINCT key1 FROM t1_pkne;
"Explained Query:  Distinct group_by=[#0] monotonic    Project (#0)      Get t1_pkneUsed Indexes:  - t1_pkne_primary_idx"

> EXPLAIN SELECT DISTINCT key2 FROM t1_pkne;
"Explained Query:  Distinct group_by=[#0] monotonic    Project (#1)      Get t1_pkneUsed Indexes:  - t1_pkne_primary_idx"

> EXPLAIN SELECT DISTINCT key1, upper(key2) FROM t1_pkne;
"Explained Query:  Distinct group_by=[#0, upper(#1)] monotonic    Project (#0, #1)      Get t1_pkneUsed Indexes:  - t1_pkne_primary_idx"

> EXPLAIN SELECT DISTINCT key1, key2 || 'a' FROM t1_pkne;
"Explained Query:  Distinct group_by=[#0, (#1 || \"a\")] monotonic    Project (#0, #1)      Get t1_pkneUsed Indexes:  - t1_pkne_primary_idx"

> EXPLAIN SELECT key1 FROM t1_pkne GROUP BY key1;
"Explained Query:  Distinct group_by=[#0] monotonic    Project (#0)      Get t1_pkneUsed Indexes:  - t1_pkne_primary_idx"

> EXPLAIN SELECT key2 FROM t1_pkne GROUP BY key2;
"Explained Query:  Distinct group_by=[#0] monotonic    Project (#1)      Get t1_pkneUsed Indexes:  - t1_pkne_primary_idx"

> EXPLAIN SELECT COUNT(DISTINCT key1) FROM t1_pkne;
"Explained Query:  Return    Union      Get l0      Map (0)        Union          Negate            Project ()              Get l0          Constant            - ()  With    cte l0 =      Reduce aggregates=[count(distinct #0)] monotonic        Project (#0)          Get t1_pkneUsed Indexes:  - t1_pkne_primary_idx"

# Make sure that primary key information is inherited from the source

> CREATE VIEW v1_pkne AS SELECT * FROM t1_pkne;

> EXPLAIN SELECT DISTINCT key1, key2 FROM v1_pkne;
"Explained Query (fast path):  Project (#0, #1)    ReadExistingIndex t1_pkne_primary_idxUsed Indexes:  - t1_pkne_primary_idx"

> CREATE VIEW v2_pkne AS SELECT * FROM t1_pkne;
> CREATE DEFAULT INDEX ON v2_pkne;

> EXPLAIN SELECT DISTINCT key1, key2 FROM v2_pkne;
"Explained Query (fast path):  Project (#0, #1)    ReadExistingIndex v2_pkne_primary_idxUsed Indexes:  - v2_pkne_primary_idx"

# Make sure that having a DISTINCT or GROUP BY confers PK semantics on upstream views

> CREATE VIEW distinct_view_pkne AS SELECT DISTINCT nokey FROM t1_pkne;
> CREATE DEFAULT INDEX ON distinct_view_pkne;

> EXPLAIN SELECT DISTINCT nokey FROM distinct_view_pkne
"Explained Query (fast path):  ReadExistingIndex distinct_view_pkne_primary_idxUsed Indexes:  - distinct_view_pkne_primary_idx"

> CREATE VIEW group_by_view_pkne AS SELECT nokey || 'a' AS f1 , nokey || 'b' AS f2 FROM t1_pkne GROUP BY nokey || 'a', nokey || 'b';
> CREATE DEFAULT INDEX ON group_by_view_pkne;

> EXPLAIN SELECT DISTINCT f1, f2 FROM group_by_view_pkne;
"Explained Query (fast path):  ReadExistingIndex group_by_view_pkne_primary_idxUsed Indexes:  - group_by_view_pkne_primary_idx"

# Redundant table is eliminated from an inner join using PK information

> EXPLAIN SELECT a1.* FROM t1_pkne AS a1, t1_pkne AS a2 WHERE a1.key1 = a2.key1 AND a1.key2 = a2.key2;
"Explained Query (fast path):  ReadExistingIndex t1_pkne_primary_idxUsed Indexes:  - t1_pkne_primary_idx"

> EXPLAIN SELECT a1.* FROM v1_pkne AS a1, v1_pkne AS a2 WHERE a1.key1 = a2.key1 AND a1.key2 = a2.key2;
"Explained Query (fast path):  ReadExistingIndex t1_pkne_primary_idxUsed Indexes:  - t1_pkne_primary_idx"

> EXPLAIN SELECT a1.* FROM v2_pkne AS a1, v2_pkne AS a2 WHERE a1.key1 = a2.key1 AND a1.key2 = a2.key2;
"Explained Query (fast path):  ReadExistingIndex v2_pkne_primary_idxUsed Indexes:  - v2_pkne_primary_idx"
