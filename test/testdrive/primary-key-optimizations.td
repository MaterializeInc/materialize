# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET unsafe_enable_table_keys = true
ALTER SYSTEM SET enable_primary_key_not_enforced = true

#
# Test that the knowledge that a given set of fields forms a primary key is used by the optimizer to remove grouping and distinct operations
#

# Remove references to internal table identifiers and "materialize.public" strings

$ set-regex match=(\s\(u\d+\)|materialize\.public\.) replacement=

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
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> BEGIN
> CREATE SOURCE t1
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC
  'testdrive-t1-${testdrive.seed}');

> CREATE TABLE t1_tbl FROM SOURCE t1 (REFERENCE "testdrive-t1-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;
> COMMIT

> CREATE DEFAULT INDEX ON t1_tbl

# Optimization is possible - no distinct is mentioned in the plan

? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT DISTINCT key1, key2 FROM t1_tbl;
Explained Query (fast path):
  Project (#0, #1)
    ReadIndex on=t1_tbl t1_tbl_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_tbl_primary_idx (*** full scan ***)

Target cluster: quickstart



? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT DISTINCT key2, key1 FROM t1_tbl;
Explained Query (fast path):
  Project (#1, #0)
    ReadIndex on=t1_tbl t1_tbl_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_tbl_primary_idx (*** full scan ***)

Target cluster: quickstart

? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT DISTINCT key2, key1, key2 FROM t1_tbl;
Explained Query (fast path):
  Project (#1, #0, #1)
    ReadIndex on=t1_tbl t1_tbl_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_tbl_primary_idx (*** full scan ***)

Target cluster: quickstart

? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT key2, key1 FROM t1_tbl GROUP BY key1, key2;
Explained Query (fast path):
  Project (#1, #0)
    ReadIndex on=t1_tbl t1_tbl_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_tbl_primary_idx (*** full scan ***)

Target cluster: quickstart

? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT key2, key1 FROM t1_tbl GROUP BY key1, key2, key2 || 'a';
Explained Query (fast path):
  Project (#1, #0)
    ReadIndex on=t1_tbl t1_tbl_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_tbl_primary_idx (*** full scan ***)

Target cluster: quickstart

? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT DISTINCT key1, key2, nokey FROM t1_tbl;
Explained Query (fast path):
  ReadIndex on=t1_tbl t1_tbl_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_tbl_primary_idx (*** full scan ***)

Target cluster: quickstart

? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT key1, key2, nokey FROM t1_tbl GROUP BY key1, key2, nokey;
Explained Query (fast path):
  ReadIndex on=t1_tbl t1_tbl_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_tbl_primary_idx (*** full scan ***)

Target cluster: quickstart

? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT key1, key2 FROM t1_tbl GROUP BY key1, key2 HAVING key1 = 'a';
Explained Query (fast path):
  Project (#0, #1)
    Filter (#0 = "a")
      ReadIndex on=t1_tbl t1_tbl_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_tbl_primary_idx (*** full scan ***)

Target cluster: quickstart

# Optimization not possible - explicit distinct is present in planFor certain types of tests the 

? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT DISTINCT key1 FROM t1_tbl;
Explained Query:
  Distinct project=[#0]
    Project (#0)
      ReadIndex on=t1_tbl t1_tbl_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_tbl_primary_idx (*** full scan ***)

Target cluster: quickstart

? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT DISTINCT key2 FROM t1_tbl;
Explained Query:
  Distinct project=[#0]
    Project (#1)
      ReadIndex on=t1_tbl t1_tbl_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_tbl_primary_idx (*** full scan ***)

Target cluster: quickstart

? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT DISTINCT key1, upper(key2) FROM t1_tbl;
Explained Query:
  Distinct project=[#0, upper(#1{key2})]
    Project (#0, #1)
      ReadIndex on=t1_tbl t1_tbl_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_tbl_primary_idx (*** full scan ***)

Target cluster: quickstart

? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT DISTINCT key1, key2 || 'a' FROM t1_tbl;
Explained Query:
  Distinct project=[#0, (#1{key2} || "a")]
    Project (#0, #1)
      ReadIndex on=t1_tbl t1_tbl_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_tbl_primary_idx (*** full scan ***)

Target cluster: quickstart

? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT key1 FROM t1_tbl GROUP BY key1;
Explained Query:
  Distinct project=[#0]
    Project (#0)
      ReadIndex on=t1_tbl t1_tbl_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_tbl_primary_idx (*** full scan ***)

Target cluster: quickstart

? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT key2 FROM t1_tbl GROUP BY key2;
Explained Query:
  Distinct project=[#0]
    Project (#1)
      ReadIndex on=t1_tbl t1_tbl_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_tbl_primary_idx (*** full scan ***)

Target cluster: quickstart

? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT COUNT(DISTINCT key1) FROM t1_tbl;
Explained Query:
  With
    cte l0 =
      Reduce aggregates=[count(distinct #0{key1})]
        Project (#0)
          ReadIndex on=t1_tbl t1_tbl_primary_idx=[*** full scan ***]
  Return
    Union
      Get l0
      Map (0)
        Union
          Negate
            Project ()
              Get l0
          Constant
            - ()

Used Indexes:
  - t1_tbl_primary_idx (*** full scan ***)

Target cluster: quickstart

# Make sure that primary key information is inherited from the source

> CREATE VIEW v1 AS SELECT * FROM t1_tbl;

? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT DISTINCT key1, key2 FROM v1;
Explained Query (fast path):
  Project (#0, #1)
    ReadIndex on=t1_tbl t1_tbl_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_tbl_primary_idx (*** full scan ***)

Target cluster: quickstart

> CREATE VIEW v2 AS SELECT * FROM t1_tbl;
> CREATE DEFAULT INDEX ON v2;

? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT DISTINCT key1, key2 FROM v2;
Explained Query (fast path):
  Project (#0, #1)
    ReadIndex on=v2 v2_primary_idx=[*** full scan ***]

Used Indexes:
  - v2_primary_idx (*** full scan ***)

Target cluster: quickstart

# Make sure that having a DISTINCT or GROUP BY confers PK semantics on upstream views

> CREATE VIEW distinct_view AS SELECT DISTINCT nokey FROM t1_tbl;
> CREATE DEFAULT INDEX ON distinct_view;

? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT DISTINCT nokey FROM distinct_view
Explained Query (fast path):
  ReadIndex on=distinct_view distinct_view_primary_idx=[*** full scan ***]

Used Indexes:
  - distinct_view_primary_idx (*** full scan ***)

Target cluster: quickstart

> CREATE VIEW group_by_view AS SELECT nokey || 'a' AS f1 , nokey || 'b' AS f2 FROM t1_tbl GROUP BY nokey || 'a', nokey || 'b';
> CREATE DEFAULT INDEX ON group_by_view;

? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT DISTINCT f1, f2 FROM group_by_view;
Explained Query (fast path):
  ReadIndex on=group_by_view group_by_view_primary_idx=[*** full scan ***]

Used Indexes:
  - group_by_view_primary_idx (*** full scan ***)

Target cluster: quickstart

# Redundant table is eliminated from an inner join using PK information

? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT a1.* FROM t1_tbl AS a1, t1_tbl AS a2 WHERE a1.key1 = a2.key1 AND a1.key2 = a2.key2;
Explained Query (fast path):
  ReadIndex on=t1_tbl t1_tbl_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_tbl_primary_idx (*** full scan ***)

Target cluster: quickstart

? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT a1.* FROM v1 AS a1, v1 AS a2 WHERE a1.key1 = a2.key1 AND a1.key2 = a2.key2;
Explained Query (fast path):
  ReadIndex on=t1_tbl t1_tbl_primary_idx=[*** full scan ***]

Used Indexes:
  - t1_tbl_primary_idx (*** full scan ***)

Target cluster: quickstart

? EXPLAIN OPTIMIZED PLAN WITH(no notices) AS VERBOSE TEXT FOR SELECT a1.* FROM v2 AS a1, v2 AS a2 WHERE a1.key1 = a2.key1 AND a1.key2 = a2.key2;
Explained Query (fast path):
  ReadIndex on=v2 v2_primary_idx=[*** full scan ***]

Used Indexes:
  - v2_primary_idx (*** full scan ***)

Target cluster: quickstart

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
