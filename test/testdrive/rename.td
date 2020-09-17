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
      {"name": "b", "type": "string"}
    ]
  }

$ avro-ocf-write path=data.ocf schema=${writer-schema} codec=null
{"a": 1, "b": "dog"}

# Create library of objects and verify names
> CREATE MATERIALIZED SOURCE mz_data
  FROM AVRO OCF '${testdrive.temp-dir}/data.ocf'

> CREATE SINK sink1 FROM mz_data
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'snk1'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> CREATE MATERIALIZED VIEW mz_view AS
    SELECT * FROM mz_data

> SHOW INDEXES FROM mz_view
Source_or_view                Key_name                              Column_name  Expression  Null   Seq_in_index
----------------------------------------------------------------------------------------------------------------
materialize.public.mz_view  materialize.public.mz_view_primary_idx  a            <null>      false             1
materialize.public.mz_view  materialize.public.mz_view_primary_idx  b            <null>      false             2
materialize.public.mz_view  materialize.public.mz_view_primary_idx  mz_obj_no    <null>      false             3

> CREATE VIEW dependent_view AS
    SELECT * FROM mz_view;

> CREATE VIEW byzantine_view AS
    SELECT mz_data.a, mz_view.b FROM mz_data JOIN mz_view ON mz_data.a = mz_view.a;

> CREATE VIEW oppositional_view AS
    SELECT * FROM mz_view WHERE b = '
    an adversarial string
    "materialize"."public"."mz_data"
    ';

> SELECT name FROM mz_catalog_names WHERE name LIKE '%materialize.public.mz_data%';
name
-----
materialize.public.mz_data
materialize.public.mz_data_primary_idx

> ALTER SOURCE mz_data RENAME TO renamed_mz_data;

> SELECT name FROM mz_catalog_names WHERE name LIKE '%materialize.public.mz_data%';
name
-----
materialize.public.mz_data_primary_idx

> SELECT name FROM mz_catalog_names WHERE name LIKE '%materialize.public.renamed_mz_data%';
name
-----
materialize.public.renamed_mz_data

> SELECT name FROM mz_catalog_names WHERE name LIKE '%materialize.public.mz_view%';
name
-----
materialize.public.mz_view
materialize.public.mz_view_primary_idx

> ALTER VIEW mz_view RENAME TO renamed_mz_view;

> SELECT name FROM mz_catalog_names WHERE name LIKE '%materialize.public.mz_view%';
name
-----
materialize.public.mz_view_primary_idx

> SELECT name FROM mz_catalog_names WHERE name LIKE '%materialize.public.renamed_mz_view%';
name
-----
materialize.public.renamed_mz_view

> SELECT name FROM mz_catalog_names WHERE name LIKE '%materialize.public.mz_view_primary_idx%';
name
-----
materialize.public.mz_view_primary_idx

> ALTER INDEX mz_view_primary_idx RENAME TO renamed_index;

> SELECT name FROM mz_catalog_names WHERE name LIKE '%materialize.public.mz_view_primary_idx%';
name
-----

> SELECT name FROM mz_catalog_names WHERE name LIKE '%materialize.public.renamed_index%';
name
-----
materialize.public.renamed_index

> SELECT name FROM mz_catalog_names WHERE name LIKE '%materialize.public.sink1%';
name
-----
materialize.public.sink1

> ALTER SINK sink1 RENAME TO renamed_sink

> SELECT name FROM mz_catalog_names WHERE name LIKE '%materialize.public.sink1%';
name
-----

> SELECT name FROM mz_catalog_names WHERE name LIKE '%materialize.public.renamed_sink%';
name
-----
materialize.public.renamed_sink

# Source was successfully renamed
> SHOW SOURCES;
SOURCES
-------
renamed_mz_data

# Sink was successfully renamed
> SHOW SINKS
SINKS
-----
renamed_sink

# View was successfully renamed
> SHOW VIEWS
VIEWS
-----
byzantine_view
dependent_view
oppositional_view
renamed_mz_view

# Item's own `CREATE VIEW` statement updated
> SHOW CREATE VIEW renamed_mz_view
View                                "Create View"
---------------------------------------------------------------------------------------------------------------------------------------------------
materialize.public.renamed_mz_view  "CREATE VIEW \"materialize\".\"public\".\"renamed_mz_view\" AS SELECT * FROM \"materialize\".\"public\".\"renamed_mz_data\""

# Item's indexes are properly re-attributed
> SHOW INDEXES FROM renamed_mz_view
Source_or_view                      Key_name                          Column_name Expression Null  Seq_in_index
---------------------------------------------------------------------------------------------------------------
 materialize.public.renamed_mz_view materialize.public.renamed_index  a           <null>     false            1
 materialize.public.renamed_mz_view materialize.public.renamed_index  b           <null>     false            2
 materialize.public.renamed_mz_view materialize.public.renamed_index  mz_obj_no   <null>     false            3

> SHOW CREATE INDEX renamed_index
Index               "Create Index"
---------------------------------------------------------------------------------------------------------------------
materialize.public.renamed_index "CREATE INDEX \"renamed_index\" ON \"materialize\".\"public\".\"renamed_mz_view\" (\"a\", \"b\", \"mz_obj_no\")"

# Simple dependencies are renamed
> SHOW CREATE VIEW dependent_view
View                                "Create View"
------------------------------------------------------------------------------------------------------------------------------------------------
materialize.public.dependent_view   "CREATE VIEW \"materialize\".\"public\".\"dependent_view\" AS SELECT * FROM \"materialize\".\"public\".\"renamed_mz_view\""

> SHOW CREATE SINK renamed_sink
Sink                            "Create Sink"
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
materialize.public.renamed_sink "CREATE SINK \"materialize\".\"public\".\"renamed_sink\" FROM \"materialize\".\"public\".\"renamed_mz_data\" INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'snk1' FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}' WITH SNAPSHOT"

# Simple dependencies with both fully qualified and unqualified item references are renamed
> SHOW CREATE VIEW byzantine_view
View                              "Create View"
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
materialize.public.byzantine_view "CREATE VIEW \"materialize\".\"public\".\"byzantine_view\" AS SELECT \"renamed_mz_data\".\"a\", \"renamed_mz_view\".\"b\" FROM \"materialize\".\"public\".\"renamed_mz_data\" JOIN \"materialize\".\"public\".\"renamed_mz_view\" ON \"renamed_mz_data\".\"a\" = \"renamed_mz_view\".\"a\""

# Strings containing old item name are not modified
> SHOW CREATE VIEW oppositional_view
View                                 "Create View"
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
materialize.public.oppositional_view "CREATE VIEW \"materialize\".\"public\".\"oppositional_view\" AS SELECT * FROM \"materialize\".\"public\".\"renamed_mz_view\" WHERE \"b\" = '   an adversarial string   \"materialize\".\"public\".\"mz_data\"   '"

# 🔬 Name collisions

> ALTER VIEW renamed_mz_view RENAME TO t1
> CREATE VIEW a AS SELECT 1 AS a
> CREATE VIEW v0 AS SELECT 2 AS b
> CREATE VIEW t2 AS
  SELECT * FROM a
  JOIN v0
  ON a.a = v0.b
  JOIN t1
  ON a.a = t1.a;

# ❌ Identifiers used in dependent items
! ALTER VIEW t1 RENAME TO b;
renaming conflict
! ALTER VIEW t1 RENAME TO materialize;
renaming conflict
! ALTER VIEW t1 RENAME TO public;
renaming conflict

# ❌ Identifiers used in own definition
# `materialize.public.a` contains an unqualified reference to `materialize.public.t1.a`.
! ALTER VIEW a RENAME TO anything
renaming conflict
! ALTER VIEW v0 RENAME TO b
renaming conflict

# ❌ Name used by another item in schema's catalog
! ALTER VIEW t1 RENAME TO a
a is already taken by item in schema
! ALTER VIEW t1 RENAME TO dependent_view;
dependent_view is already taken by item in schema

# 🔬 Aliases

> CREATE VIEW table_alias_check AS
  SELECT t1.a AS b
  FROM (
    SELECT a
    FROM t1
  ) AS t1;

# ❌ View name used as alias
! ALTER VIEW t1 RENAME TO anything
renaming conflict

# 🔬 Unresolvable without scope analysis
# These are example queries whose ambiguity cannot be resolved through
# simple ident analysis; i.e. you need either real or strictly inferred scope
# information to disambiguate references to `db0.scm0.z`.

> CREATE DATABASE IF NOT EXISTS db0;
> CREATE SCHEMA IF NOT EXISTS db0.scm0;
> CREATE VIEW db0.scm0.z AS SELECT 1 AS a;

> CREATE SCHEMA IF NOT EXISTS materialize.db0;
> CREATE VIEW materialize.db0.scm0 AS SELECT 1 AS z;

> CREATE VIEW final_boss_1 AS
  SELECT db0.scm0.z, db0.scm0.z.a
  FROM db0.scm0.z
  JOIN db0.scm0
  ON db0.scm0.z = db0.scm0.z.a;

# ❌ Item name used as column
! ALTER VIEW db0.scm0.z RENAME TO anything
renaming conflict
# ❌ Item name used as schema
! ALTER VIEW db0.scm0 RENAME TO anything
renaming conflict

> CREATE SCHEMA IF NOT EXISTS materialize.scm0;
> CREATE VIEW materialize.scm0.z AS SELECT 1 AS a;
> CREATE VIEW materialize.public.scm0 AS SELECT 1 AS z;

> CREATE VIEW final_boss_2 AS
  SELECT scm0.z, scm0.z.a
  FROM scm0.z
  JOIN scm0
  ON scm0.z = scm0.z.a;

# ❌ Item name used as column
! ALTER VIEW scm0.z RENAME TO anything
renaming conflict
# ❌ Item name used as schema
! ALTER VIEW scm0 RENAME TO anything
renaming conflict

# 🔬 DB::DB

# 🔬🔬 DB::DB - DB-level qualification
> CREATE DATABASE IF NOT EXISTS db1
> CREATE DATABASE IF NOT EXISTS db2

> CREATE SCHEMA IF NOT EXISTS db1.scm1
> CREATE SCHEMA IF NOT EXISTS db2.scm1

> CREATE VIEW db1.scm1.v AS SELECT 1 as a;
> CREATE VIEW db2.scm1.v AS SELECT 2 as b;

# References to `v` are ambiguous because not all references are sufficiently
# qualified. This won't work until we have database-level qualification.
> CREATE VIEW db1_db2_scm1_min_qual_invalid AS
    SELECT * FROM
    (
        SELECT v.a FROM db1.scm1.v
    ) AS l
    JOIN (
        SELECT v.b FROM db2.scm1.v
    ) AS r
    ON l.a = r.b;

# ❌ Insufficient qualification
! ALTER VIEW db1.scm1.v rename to anything;
renaming conflict
! ALTER VIEW db2.scm1.v rename to anything;
renaming conflict

> DROP VIEW db1_db2_scm1_min_qual_invalid

# References to `v` are ambiguous because not all references are sufficiently
# qualified.
> CREATE VIEW db1_db2_scm1_mix_qual_invalid AS
    SELECT * FROM
    (
        SELECT db1.scm1.v.a FROM db1.scm1.v
    ) AS l
    JOIN (
        SELECT scm1.v.b FROM db2.scm1.v
    ) AS r
    ON l.a = r.b;

# ❌ Insufficient qualification
! ALTER VIEW db1.scm1.v rename to anything;
renaming conflict
! ALTER VIEW db2.scm1.v rename to anything;
renaming conflict

> DROP VIEW db1_db2_scm1_mix_qual_invalid

# Fully qualifying references allows renaming
> CREATE VIEW db1_db2_scm1_valid_qual AS
    SELECT * FROM
    (
        SELECT db1.scm1.v.a FROM db1.scm1.v
    ) AS l
    JOIN (
        SELECT db2.scm1.v.b FROM db2.scm1.v
    ) AS r
    ON l.a = r.b;

# ✅ Sufficient qualification + new idents
> ALTER VIEW db1.scm1.v RENAME TO v0
> ALTER VIEW db1.scm1.v0 RENAME TO v1
> ALTER VIEW db2.scm1.v RENAME TO v2

> SHOW CREATE VIEW db1_db2_scm1_valid_qual
View                                       "Create View"
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
materialize.public.db1_db2_scm1_valid_qual "CREATE VIEW \"materialize\".\"public\".\"db1_db2_scm1_valid_qual\" AS SELECT * FROM (SELECT \"db1\".\"scm1\".\"v1\".\"a\" FROM \"db1\".\"scm1\".\"v1\") AS \"l\" JOIN (SELECT \"db2\".\"scm1\".\"v2\".\"b\" FROM \"db2\".\"scm1\".\"v2\") AS \"r\" ON \"l\".\"a\" = \"r\".\"b\""

> CREATE VIEW "materialize"."public"."db1_db2_scm1_valid_qual_test" AS SELECT * FROM (SELECT "db1"."scm1"."v1"."a" FROM "db1"."scm1"."v1") AS "l" JOIN (SELECT "db2"."scm1"."v2"."b" FROM "db2"."scm1"."v2") AS "r" ON "l"."a" = "r"."b"

# 🔬🔬 DB::DB - No qualification necessary
# Evaluate ability to handle two database-qualified references.
> CREATE SCHEMA db2.scm2;
> CREATE VIEW db2.scm2.v2 AS
    SELECT 1 AS z;

> CREATE VIEW db_db_qual_diff_s_v AS
    SELECT db2.scm2.v2.z
    FROM db2.scm2.v2
    JOIN db1.scm1.v1
    ON db2.scm2.v2.z = db1.scm1.v1.a

# ❌ Identifiers used in dependent items
! ALTER VIEW db2.scm2.v2 RENAME TO db2;
renaming conflict
! ALTER VIEW db2.scm2.v2 RENAME TO scm2;
renaming conflict
! ALTER VIEW db2.scm2.v2 RENAME TO z;
renaming conflict
! ALTER VIEW db2.scm2.v2 RENAME TO scm2;
renaming conflict
! ALTER VIEW db2.scm2.v2 RENAME TO a;
renaming conflict

# ✅ New idents
> ALTER VIEW db2.scm2.v2 RENAME TO v3;
> ALTER VIEW db1.scm1.v1 RENAME TO v4;

> SHOW CREATE VIEW db_db_qual_diff_s_v
View                          "Create View"
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
materialize.public.db_db_qual_diff_s_v "CREATE VIEW \"materialize\".\"public\".\"db_db_qual_diff_s_v\" AS SELECT \"db2\".\"scm2\".\"v3\".\"z\" FROM \"db2\".\"scm2\".\"v3\" JOIN \"db1\".\"scm1\".\"v4\" ON \"db2\".\"scm2\".\"v3\".\"z\" = \"db1\".\"scm1\".\"v4\".\"a\""

> CREATE VIEW "materialize"."public"."db_db_qual_diff_s_v_test" AS SELECT "db2"."scm2"."v3"."z" FROM "db2"."scm2"."v3" JOIN "db1"."scm1"."v4" ON "db2"."scm2"."v3"."z" = "db1"."scm1"."v4"."a"

# 🔬 DB::Schema
# Evaluate ability to handle one database-qualified reference and one
# schema-qualified reference.

> CREATE SCHEMA IF NOT EXISTS scm3

> CREATE VIEW scm3.v2 AS
    SELECT 1 AS z;

> CREATE VIEW db_scm_qual AS
    SELECT scm3.v2.z, db1.scm1.v4.a
    FROM scm3.v2
    JOIN db1.scm1.v4
    ON scm3.v2.z = db1.scm1.v4.a;

> ALTER VIEW scm3.v2 RENAME TO v3;
> ALTER VIEW db1.scm1.v4 RENAME TO v5;

> SHOW CREATE VIEW db_scm_qual
View                           "Create View"
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
materialize.public.db_scm_qual "CREATE VIEW \"materialize\".\"public\".\"db_scm_qual\" AS SELECT \"scm3\".\"v3\".\"z\", \"db1\".\"scm1\".\"v5\".\"a\" FROM \"materialize\".\"scm3\".\"v3\" JOIN \"db1\".\"scm1\".\"v5\" ON \"scm3\".\"v3\".\"z\" = \"db1\".\"scm1\".\"v5\".\"a\""

> CREATE VIEW "materialize"."public"."db_scm_qual_test" AS SELECT "scm3"."v3"."z", "db1"."scm1"."v5"."a" FROM "materialize"."scm3"."v3" JOIN "db1"."scm1"."v5" ON "scm3"."v3"."z" = "db1"."scm1"."v5"."a"

# 🔬 DB::View
# Evaluate ability to handle one database-qualified reference and one
# implicitly-qualified reference.

> CREATE VIEW v1 AS
    SELECT 1 AS z;

> CREATE VIEW db_v_qual AS
    SELECT v1.z, db1.scm1.v5.a
    FROM v1
    JOIN db1.scm1.v5
    ON v1.z = db1.scm1.v5.a;

# ❌ Identifiers used in dependent items
! ALTER VIEW v1 RENAME TO z;
renaming conflict
! ALTER VIEW v1 RENAME TO db1;
renaming conflict
! ALTER VIEW v1 RENAME TO scm1;
renaming conflict
! ALTER VIEW v1 RENAME TO v5;
renaming conflict
! ALTER VIEW db1.scm1.v5 RENAME TO v1;
renaming conflict

# ✅ New idents
> ALTER VIEW v1 RENAME TO v2;
> ALTER VIEW db1.scm1.v5 RENAME TO v6;

> SHOW CREATE VIEW db_v_qual
View                         "Create View"
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
materialize.public.db_v_qual "CREATE VIEW \"materialize\".\"public\".\"db_v_qual\" AS SELECT \"v2\".\"z\", \"db1\".\"scm1\".\"v6\".\"a\" FROM \"materialize\".\"public\".\"v2\" JOIN \"db1\".\"scm1\".\"v6\" ON \"v2\".\"z\" = \"db1\".\"scm1\".\"v6\".\"a\""

> CREATE VIEW "materialize"."public"."db_v_qual_test" AS SELECT "v2"."z", "db1"."scm1"."v6"."a" FROM "materialize"."public"."v2" JOIN "db1"."scm1"."v6" ON "v2"."z" = "db1"."scm1"."v6"."a"

# 🔬 Schema::Schema

# 🔬🔬 Require schema-level qualification
> CREATE DATABASE IF NOT EXISTS db1
> CREATE SCHEMA IF NOT EXISTS db1.scm1
> CREATE SCHEMA IF NOT EXISTS db1.scm2

> CREATE VIEW db1.scm1.v AS SELECT 1 as a;
> CREATE VIEW db1.scm2.v AS SELECT 2 as b;

# References to `v` are ambiguous because not all references are sufficiently
# qualified. This won't work until we have schema-level qualification.
> CREATE VIEW db1_scm1_scm2_min_qual_invalid AS
    SELECT * FROM
    (
        SELECT v.a FROM db1.scm1.v
    ) AS l
    JOIN (
        SELECT v.b FROM db1.scm2.v
    ) AS r
    ON l.a = r.b;

# ❌ Insufficient qualification
! ALTER VIEW db1.scm1.v rename to anything;
renaming conflict
! ALTER VIEW db1.scm2.v rename to anything;
renaming conflict

> DROP VIEW db1_scm1_scm2_min_qual_invalid

# References to `v` are ambiguous because not all references are sufficiently
# qualified.
> CREATE VIEW db1_scm1_scm2_mix_qual_invalid AS
    SELECT * FROM
    (
        SELECT db1.scm1.v.a FROM db1.scm1.v
    ) AS l
    JOIN (
        SELECT v.b FROM db1.scm2.v
    ) AS r
    ON l.a = r.b;

# ❌ Insufficient qualification
! ALTER VIEW db1.scm1.v rename to anything;
renaming conflict
! ALTER VIEW db1.scm2.v rename to anything;
renaming conflict

> DROP VIEW db1_scm1_scm2_mix_qual_invalid

# Qualifying to the point of differentiation allows renaming
> CREATE VIEW db1_scm1_scm2_valid_qual AS
    SELECT * FROM
    (
        SELECT scm1.v.a FROM db1.scm1.v
    ) AS l
    JOIN (
        SELECT scm2.v.b FROM db1.scm2.v
    ) AS r
    ON l.a = r.b;

# ✅ Sufficient qualification + new idents
> ALTER VIEW db1.scm1.v RENAME TO v3
> ALTER VIEW db1.scm1.v3 RENAME TO v4
> ALTER VIEW db1.scm2.v RENAME TO v5

> SHOW CREATE VIEW db1_scm1_scm2_valid_qual
View                                       "Create View"
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
materialize.public.db1_scm1_scm2_valid_qual "CREATE VIEW \"materialize\".\"public\".\"db1_scm1_scm2_valid_qual\" AS SELECT * FROM (SELECT \"scm1\".\"v4\".\"a\" FROM \"db1\".\"scm1\".\"v4\") AS \"l\" JOIN (SELECT \"scm2\".\"v5\".\"b\" FROM \"db1\".\"scm2\".\"v5\") AS \"r\" ON \"l\".\"a\" = \"r\".\"b\""

> CREATE VIEW "materialize"."public"."db1_scm1_scm2_valid_qual_test" AS SELECT * FROM (SELECT "scm1"."v4"."a" FROM "db1"."scm1"."v4") AS "l" JOIN (SELECT "scm2"."v5"."b" FROM "db1"."scm2"."v5") AS "r" ON "l"."a" = "r"."b"

# 🔬🔬 No qualification necessary
> CREATE SCHEMA IF NOT EXISTS scm4;
> CREATE SCHEMA IF NOT EXISTS scm5;

> CREATE VIEW scm4.v1 AS
    SELECT 1 AS z;

> CREATE VIEW scm5.v2 AS
    SELECT 1 AS a;

> CREATE VIEW scm_scm_qual AS
    SELECT scm4.v1.z, scm5.v2.a
    FROM scm4.v1
    JOIN scm5.v2
    ON scm4.v1.z = scm5.v2.a;

# ❌ Identifiers used in dependent items
! ALTER VIEW scm4.v1 RENAME TO z;
renaming conflict
! ALTER VIEW scm4.v1 RENAME TO scm5;
renaming conflict
! ALTER VIEW scm4.v1 RENAME TO v2;
renaming conflict
! ALTER VIEW scm4.v1 RENAME TO a;
renaming conflict
! ALTER VIEW scm5.v2 RENAME TO a;
renaming conflict
! ALTER VIEW scm5.v2 RENAME TO v1;
renaming conflict

# ✅ New idents
> ALTER VIEW scm4.v1 RENAME TO v3;
> ALTER VIEW scm5.v2 RENAME TO v4;

> SHOW CREATE VIEW scm_scm_qual
View                           "Create View"
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
materialize.public.scm_scm_qual "CREATE VIEW \"materialize\".\"public\".\"scm_scm_qual\" AS SELECT \"scm4\".\"v3\".\"z\", \"scm5\".\"v4\".\"a\" FROM \"materialize\".\"scm4\".\"v3\" JOIN \"materialize\".\"scm5\".\"v4\" ON \"scm4\".\"v3\".\"z\" = \"scm5\".\"v4\".\"a\""

> CREATE VIEW "materialize"."public"."scm_scm_qual_test" AS SELECT "scm4"."v3"."z", "scm5"."v4"."a" FROM "materialize"."scm4"."v3" JOIN "materialize"."scm5"."v4" ON "scm4"."v3"."z" = "scm5"."v4"."a"

# 🔬 Schema::View
# Evaluate ability to handle one schema-qualified reference and one
# implicitly-qualified reference.

> CREATE VIEW v1 AS
    SELECT 1 AS z;

> CREATE VIEW scm_v_qual AS
    SELECT v1.z, scm5.v4.a
    FROM v1
    JOIN scm5.v4
    ON v1.z = scm5.v4.a;

# ❌ Identifiers used in dependent items
! ALTER VIEW v1 RENAME TO z;
renaming conflict
! ALTER VIEW v1 RENAME TO scm5;
renaming conflict
! ALTER VIEW v1 RENAME TO v4;
renaming conflict

# ✅ New idents
> ALTER VIEW v1 RENAME TO v3;
> ALTER VIEW scm5.v4 RENAME TO v6;

> SHOW CREATE VIEW scm_v_qual
View                          "Create View"
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
materialize.public.scm_v_qual "CREATE VIEW \"materialize\".\"public\".\"scm_v_qual\" AS SELECT \"v3\".\"z\", \"scm5\".\"v6\".\"a\" FROM \"materialize\".\"public\".\"v3\" JOIN \"materialize\".\"scm5\".\"v6\" ON \"v3\".\"z\" = \"scm5\".\"v6\".\"a\""

> CREATE VIEW "materialize"."public"."scm_v_qual_test" AS SELECT "v3"."z", "scm5"."v6"."a" FROM "materialize"."public"."v3" JOIN "materialize"."scm5"."v6" ON "v3"."z" = "scm5"."v6"."a"

# 🔬 View::View
# Evaluate ability to handle two implicitly-qualified references.

> CREATE VIEW v4 AS
  SELECT 1 AS a

> CREATE VIEW v_v_qual AS
  SELECT v3.z, v4.a
  FROM v3
  JOIN v4
  ON v3.z = v4.a

# ❌ Identifiers used in dependent items
! ALTER VIEW v4 RENAME TO z;
renaming conflict

# ✅ New idents
> ALTER VIEW v4 RENAME TO v6;
> ALTER VIEW v3 RENAME TO v5;

> SHOW CREATE VIEW v_v_qual
View                          "Create View"
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
materialize.public.v_v_qual "CREATE VIEW \"materialize\".\"public\".\"v_v_qual\" AS SELECT \"v5\".\"z\", \"v6\".\"a\" FROM \"materialize\".\"public\".\"v5\" JOIN \"materialize\".\"public\".\"v6\" ON \"v5\".\"z\" = \"v6\".\"a\""

> CREATE VIEW "materialize"."public"."v_v_qual_test" AS SELECT "v5"."z", "v6"."a" FROM "materialize"."public"."v5" JOIN "materialize"."public"."v6" ON "v5"."z" = "v6"."a"

# 🔬 Qualified wildcard

> CREATE VIEW qualified_wildcard AS
  SELECT v5.*
  FROM v5

# ✅ New idents
> ALTER VIEW v5 RENAME TO v7

> SHOW CREATE VIEW qualified_wildcard
View                                  "Create View"
---------------------------------------------------------------------------------------------------------------------------------------------------------------
materialize.public.qualified_wildcard "CREATE VIEW \"materialize\".\"public\".\"qualified_wildcard\" AS SELECT \"v7\".* FROM \"materialize\".\"public\".\"v7\""

> CREATE VIEW "materialize"."public"."qualified_wildcard_test" AS SELECT "v7".* FROM "materialize"."public"."v7"

# 🔬 WHERE...IN

> CREATE VIEW where_in_subquery AS
  SELECT b FROM t1 WHERE a IN (SELECT a FROM scm5.v6);

> CREATE VIEW where_in_literal AS
  SELECT b FROM t1 WHERE b IN ('v8')

# ❌ Identifiers used in dependent items
! ALTER VIEW where_in_subquery RENAME TO scm5
renaming conflict

# ✅ New idents
> ALTER VIEW where_in_literal RENAME TO v8
> ALTER VIEW v8 RENAME TO v9

> SHOW CREATE VIEW v9
View                   "Create View"
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
materialize.public.v9 "CREATE VIEW \"materialize\".\"public\".\"v9\" AS SELECT \"b\" FROM \"materialize\".\"public\".\"t1\" WHERE \"b\" IN ('v8')"

> CREATE VIEW "materialize"."public"."where_in_literal_test" AS SELECT "b" FROM "materialize"."public"."t1" WHERE "b" IN ('v8')

# 🔬🔬🔬 Spaces

> CREATE VIEW space AS
  SELECT 1 as "has space";

> CREATE VIEW "already has space" AS
  SELECT 1 as my_space;

> CREATE VIEW space_dependent AS
  SELECT * FROM space
  JOIN "already has space"
  ON "already has space".my_space = space."has space";

# ❌ Identifiers used in own definition
! ALTER VIEW space RENAME TO "has space"
renaming conflict
! ALTER VIEW "already has space" RENAME TO my_space
renaming conflict

# ✅ New idents
> ALTER VIEW space RENAME TO "now has space"
> ALTER VIEW "already has space" RENAME TO "still has space"

> SHOW CREATE VIEW space_dependent
View                               "Create View"
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
materialize.public.space_dependent "CREATE VIEW \"materialize\".\"public\".\"space_dependent\" AS SELECT * FROM \"materialize\".\"public\".\"now has space\" JOIN \"materialize\".\"public\".\"still has space\" ON \"still has space\".\"my_space\" = \"now has space\".\"has space\""

> CREATE VIEW "materialize"."public"."space_dependent_test" AS SELECT * FROM "materialize"."public"."now has space" JOIN "materialize"."public"."still has space" ON "still has space"."my_space" = "now has space"."has space"

# 🔬 Keyword

> CREATE VIEW natural AS
  SELECT * FROM t1 NATURAL JOIN t1;

# ✅ Keywords are never trapped in a rename
> ALTER VIEW natural RENAME TO unnatural

> SHOW CREATE VIEW unnatural
View                         "Create View"
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
materialize.public.unnatural "CREATE VIEW \"materialize\".\"public\".\"unnatural\" AS SELECT * FROM \"materialize\".\"public\".\"t1\" NATURAL JOIN \"materialize\".\"public\".\"t1\""

> CREATE VIEW "materialize"."public"."unnatural_test" AS SELECT * FROM "materialize"."public"."t1" NATURAL JOIN "materialize"."public"."t1"

# 🔬 Function names

> CREATE VIEW func AS
  SELECT count(1)

> CREATE VIEW no_func AS
  SELECT 1 AS a;

> CREATE VIEW func_dependency AS
  SELECT * FROM no_func
  JOIN (
    SELECT * FROM no_func
  ) AS x
  ON no_func.a = x.a;

# ❌ Identifiers used in dependent items
! ALTER VIEW func RENAME TO count
renaming conflict

# ✅ Non-colliding function name
> ALTER VIEW no_func RENAME TO count

> SHOW CREATE VIEW func_dependency
View                                "Create View"
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
materialize.public.func_dependency "CREATE VIEW \"materialize\".\"public\".\"func_dependency\" AS SELECT * FROM \"materialize\".\"public\".\"count\" JOIN (SELECT * FROM \"materialize\".\"public\".\"count\") AS \"x\" ON \"count\".\"a\" = \"x\".\"a\""

> CREATE VIEW "materialize"."public"."func_dependency_test" AS SELECT * FROM count JOIN (SELECT * FROM count) AS x ON count.a = x.a

> SHOW CREATE VIEW count
View                     "Create View"
---------------------------------------------------------------------------------------
materialize.public.count "CREATE VIEW \"materialize\".\"public\".\"count\" AS SELECT 1 AS \"a\""

> DROP VIEW count CASCADE;

> CREATE VIEW count AS SELECT 1

> CREATE TABLE j (b int)

> SHOW TABLES
TABLES
----
j

> ALTER TABLE j RENAME TO renamed_j

> SHOW TABLES
TABLES
----
renamed_j

> SELECT tables FROM mz_tables WHERE global_id like 'u%';
tables
----
renamed_j
