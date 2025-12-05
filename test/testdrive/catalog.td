# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default replicas=1
$ set-arg-default single-replica-cluster=quickstart

# Testdrive accounts for multi-replica runs by creating additional clusters,
# which affect the output of this test––instead just don't run this test.

$ skip-if
SELECT ${arg.replicas} > 1;

$ set-sql-timeout duration=1s

# Test \d <object> command
> CREATE TABLE tbl (a int, b text)

> CREATE INDEX tbl_ind ON tbl (b)

> CREATE INDEX tbl_lower_ind ON tbl (lower(b), a)

$ psql-execute command="\d tbl"
\                Table "public.tbl"
 Column |  Type   | Collation | Nullable | Default
--------+---------+-----------+----------+---------
 a      | integer |           |          | NULL
 b      | text    |           |          | NULL
Indexes:
    "tbl_ind" arrangement (b)
    "tbl_lower_ind" arrangement (pg_catalog.lower(b),a)

> DROP TABLE tbl CASCADE

# What schemas do we have by default?
> SHOW SCHEMAS FROM materialize
public ""
information_schema ""
mz_catalog ""
mz_catalog_unstable ""
mz_unsafe ""
mz_internal ""
mz_introspection ""
pg_catalog ""

> SHOW SCHEMAS
name  comment
------------------
information_schema ""
public ""
mz_catalog ""
mz_catalog_unstable ""
mz_unsafe ""
mz_internal ""
mz_introspection ""
pg_catalog ""

$ psql-execute command=\dn
\         List of schemas
        Name         |   Owner
---------------------+-----------
 mz_catalog          | mz_system
 mz_catalog_unstable | mz_system
 mz_internal         | mz_system
 mz_introspection    | mz_system
 mz_unsafe           | mz_system
 public              | mz_system


$ psql-execute command="\dn mz_catalog"
\    List of schemas
    Name    |   Owner
------------+-----------
 mz_catalog | mz_system


$ psql-execute command="\dn mz_*"
\         List of schemas
        Name         |   Owner
---------------------+-----------
 mz_catalog          | mz_system
 mz_catalog_unstable | mz_system
 mz_internal         | mz_system
 mz_introspection    | mz_system
 mz_unsafe           | mz_system

# What objects do we have by default?
> SHOW OBJECTS
name    type    comment
-----------------------

# Creating a schema should be reflected in the output of SHOW SCHEMAS.
> CREATE SCHEMA s
> SHOW SCHEMAS
name            comment
-----------------------
public ""
s ""
information_schema ""
mz_catalog ""
mz_catalog_unstable ""
mz_internal ""
mz_introspection ""
mz_unsafe ""
pg_catalog ""


# Creating a schema with a name that already exists should fail.
! CREATE SCHEMA s
contains:schema 's' already exists

# Dropping a schema with a view should only succeed with CASCADE.
> CREATE VIEW s.v AS SELECT 1
! DROP SCHEMA s
contains:schema 'materialize.s' cannot be dropped without CASCADE while it contains objects
> DROP SCHEMA s CASCADE

# Dropping a schema with no objects should succeed without CASCADE.
> CREATE SCHEMA s
> CREATE VIEW s.v AS SELECT 1
> DROP VIEW s.v
> DROP SCHEMA s

# What databases do we have by default?
> SHOW DATABASES
name  comment
------------
materialize ""

> SELECT name FROM mz_databases
name
-----------
materialize

# Creating a database should be reflected in the output of SHOW DATABASES.
> CREATE DATABASE d
> SHOW DATABASES
name  comment
-----------
d ""
materialize ""

# ...and also in `\l`
$ psql-execute command="\l"
\                                                 List of databases
    Name     |    Owner    | Encoding | Locale Provider | Collate | Ctype | Locale | ICU Rules | Access privileges
-------------+-------------+----------+-----------------+---------+-------+--------+-----------+-------------------
 d           | materialize | UTF8     | libc            | C       | C     |        |           |
 materialize | mz_system   | UTF8     | libc            | C       | C     |        |           |



> SELECT name FROM mz_databases
name
-----------
materialize
d

# The same catalog information should be accessible with any amount of
# database or schema qualification.
> SELECT count(*) FROM materialize.mz_catalog.mz_databases
2
> SELECT count(*) FROM d.mz_catalog.mz_databases
2

# SHOW DATABASES should filter its output according to the provided LIKE or
# WHERE clause.
> SHOW DATABASES LIKE 'foo'
> SHOW DATABASES LIKE 'd'
d ""
> SHOW DATABASES LIKE 'mat%'
materialize ""
> SHOW DATABASES WHERE (SELECT name = name)
materialize ""
d ""
> SHOW DATABASES WHERE (name = (SELECT min(name) FROM mz_databases))
d ""
! SHOW DATABASES WHERE 7
contains:WHERE clause must have type boolean, not type integer

# Creating a database with a name that already exists should fail.
! CREATE DATABASE d
contains:database 'd' already exists

# The new database should have a default public schema.
> SHOW SCHEMAS FROM d
name                comment
---------------------------
public ""
information_schema ""
mz_catalog ""
mz_catalog_unstable ""
mz_internal ""
mz_introspection ""
mz_unsafe ""
pg_catalog ""

# New schemas in the database should appear in the output of SHOW SCHEMAS FROM.
> CREATE SCHEMA d.s
> SHOW SCHEMAS FROM d
name    comment
-------------------
public ""
s ""
information_schema ""
mz_catalog ""
mz_catalog_unstable ""
mz_internal ""
mz_introspection ""
mz_unsafe ""
pg_catalog ""

# SHOW SCHEMAS should filter its output based on the provided LIKE or WHERE
# clause.
> SHOW SCHEMAS LIKE 'pub%'
public ""
> SHOW SCHEMAS LIKE 'private'
> SHOW SCHEMAS WHERE name = 'public'
public ""

# New views in the database should work.
> CREATE VIEW d.public.v1 AS SELECT 1
> SHOW VIEWS FROM d.public
name    comment
---------------
v1      ""

# Setting the session database should update name resolution appropriately.
> SET DATABASE = d
> SHOW DATABASE
d
> SHOW SCHEMAS
public ""
s ""
information_schema ""
mz_catalog ""
mz_catalog_unstable ""
mz_internal ""
mz_introspection ""
mz_unsafe ""
pg_catalog ""
> CREATE VIEW v2 AS SELECT 2
> SHOW VIEWS
v1 ""
v2 ""
> SELECT * FROM v1 UNION ALL SELECT * FROM v2
1
2

> SHOW OBJECTS
name    type    comment
-----------------------
v1      view    ""
v2      view    ""

# Test minimizing name qualification

> CREATE TYPE int_list AS list (ELEMENT TYPE = int4)

> SELECT pg_typeof('{1}'::d.public.int_list)
int_list

> CREATE SCHEMA other
> CREATE TYPE other.int_list AS list (ELEMENT TYPE = int4)
> SELECT pg_typeof('{1}'::d.other.int_list)
other.int_list

> CREATE DATABASE foo
> CREATE SCHEMA foo.other
> CREATE TYPE foo.other.int_list AS LIST (ELEMENT TYPE = int4)
> SELECT pg_typeof('{1}'::foo.other.int_list)
foo.other.int_list

> CREATE TYPE bool AS LIST (ELEMENT TYPE = int4)
! SELECT '{1}'::bool
contains:invalid input syntax for type boolean: "{1}"

> SELECT pg_typeof('{1}'::public.bool);
public.bool

> SELECT pg_typeof('{1}'::d.public.bool);
public.bool

> DROP DATABASE foo

! DROP OBJECT v1
contains:Expected one of TABLE or VIEW or MATERIALIZED or SOURCE or SINK or INDEX or TYPE or ROLE or USER or CLUSTER or SECRET or CONNECTION or DATABASE or SCHEMA or FUNCTION or CONTINUAL or NETWORK, found identifier

> SHOW OBJECTS
name        type        comment
-------------------------------
bool        type        ""
int_list    type        ""
v1          view        ""
v2          view        ""

# Create one of every mz_object type
$ set schema={
    "name": "row",
    "type": "record",
    "fields": [
      {"name": "a", "type": "long"}
    ]
  }
$ kafka-create-topic topic=data
$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"a": 1}

> CREATE TABLE tbl (a int, b text);
> CREATE SECRET pass_secret AS 'pass';

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE MATERIALIZED VIEW mv AS SELECT (1);

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> BEGIN
> CREATE SOURCE source_data
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}');

> CREATE TABLE source_data_tbl FROM SOURCE source_data (REFERENCE "testdrive-data-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn;
> COMMIT

> CREATE SINK snk
  IN CLUSTER ${arg.single-replica-cluster}
  FROM source_data_tbl
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-catalog-sink-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;

> SHOW OBJECTS
name        type              comment
-------------------------------------
bool        type              ""
csr_conn    connection        ""
int_list    type              ""
v1          view              ""
v2          view              ""
tbl         table             ""
pass_secret secret            ""
kafka_conn  connection        ""
mv          materialized-view ""
source_data source            ""
source_data_tbl table         ""
snk         sink              ""

> SELECT DISTINCT(TYPE) FROM mz_objects
type
----
table
source
view
materialized-view
sink
index
connection
type
function
secret

> SELECT * FROM (SHOW OBJECTS) ORDER BY name DESC
bool        type        ""
csr_conn    connection  ""
int_list    type        ""
kafka_conn  connection  ""
mv          materialized-view  ""
pass_secret secret      ""
snk         sink        ""
source_data source      ""
source_data_tbl table   ""
tbl         table       ""
v1          view        ""
v2          view        ""

> SELECT create_sql FROM (SHOW CREATE TABLE tbl)
"CREATE TABLE d.public.tbl (a pg_catalog.int4, b pg_catalog.text);"

! SHOW COLUMNS FROM pass_secret
contains:d.public.pass_secret is a secret and so does not have columns
! SHOW COLUMNS FROM kafka_conn
contains:d.public.kafka_conn is a connection and so does not have columns

# DROP DATABASE does not support both RESTRICT and CASCADE.
! DROP DATABASE d RESTRICT CASCADE
contains:Cannot specify both RESTRICT and CASCADE in DROP
! DROP DATABASE d CASCADE RESTRICT
contains:Cannot specify both CASCADE and RESTRICT in DROP
! DROP DATABASE d CASCADE CASCADE
contains:Expected end of statement, found CASCADE

! DROP DATABASE d RESTRICT
contains:database 'd' cannot be dropped with RESTRICT while it contains schemas

# DROP DATABASE should succeed even when there are objects in the database.
> DROP DATABASE d
# SHOW DATABASES should work, even if the current database has been dropped.
> SHOW DATABASES
name        comment
-------------------
materialize ""

> SELECT name FROM mz_databases
name
-----------
materialize

# The session database should remain set to the dropped database, but future
# queries that depend on the session database should fail with sensible error
# messages.
> SHOW DATABASE
d
! SELECT * FROM v
contains:unknown catalog item 'v'

# But queries that do not depend on the session database should work fine.
> CREATE VIEW materialize.public.v AS SELECT 1
> CREATE DATABASE d

# Dropping the public schema is okay, but dropping the catalog schemas is not.
> DROP SCHEMA public

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET enable_rbac_checks TO false

! DROP SCHEMA mz_catalog
contains:cannot drop schema mz_catalog because it is required by the database system
! DROP SCHEMA pg_catalog
contains:cannot drop schema pg_catalog because it is required by the database system

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET enable_rbac_checks TO true

# Schema names that start with "mz_" or "pg_" are reserved for future use by the
# system.
! CREATE SCHEMA mz_foo
contains:unacceptable schema name 'mz_foo'
! CREATE SCHEMA pg_bar
contains:unacceptable schema name 'pg_bar'

# The search path is configurable.
> SHOW search_path
"public"
> SET search_path = foo
> SET SCHEMA foo
> SET search_path to public

# Creating views in non-existent databases should fail.
! CREATE VIEW noexist.ignored AS SELECT 1
contains:unknown schema 'noexist'
! CREATE VIEW materialize.noexist.ignored AS SELECT 1
contains:unknown schema 'noexist'
! CREATE VIEW noexist.ignored.ignored AS SELECT 1
contains:unknown database 'noexist'

# As should showing views.
! SHOW VIEWS FROM noexist
contains:unknown schema 'noexist'
! SHOW VIEWS FROM noexist_db.noexist_schema
contains:unknown database 'noexist_db'

# Dropping database with cross-schema dependencies is ok.
> CREATE DATABASE d1;
> CREATE SCHEMA d1.s1;
> CREATE VIEW d1.s1.t as select 1;
> CREATE VIEW d1.public.tt as select * from d1.s1.t;
> DROP DATABASE d1;

# Dropping database with cross-database dependencies is ok and drops the
# dependent views.
> CREATE DATABASE d1;
> CREATE VIEW d1.public.t as select 1;
> CREATE DATABASE d2;
> CREATE VIEW d2.public.t AS SELECT * FROM d1.public.t;
> DROP DATABASE d1;
> SHOW DATABASES
name        comment
-------------------
d           ""
d2          ""
materialize ""
> SELECT name FROM mz_databases
name
-----------
materialize
d
d2

> SHOW VIEWS FROM d2.public;
name    comment
---------------

# Check default sources, tables, and views in mz_catalog.

> SHOW SOURCES FROM mz_catalog
name                                         type    cluster  comment
---------------------------------------------------------------------
mz_cluster_replica_frontiers                 source  <null>   ""

> SHOW TABLES FROM mz_catalog
name                           comment
--------------------------------------
mz_array_types                 ""
mz_audit_events                ""
mz_aws_privatelink_connections ""
mz_base_types                  ""
mz_clusters                    ""
mz_cluster_replicas            ""
mz_cluster_replica_sizes       ""
mz_columns                     ""
mz_connections                 ""
mz_databases                   ""
mz_default_privileges          ""
mz_egress_ips                  ""
mz_functions                   ""
mz_iceberg_sinks               ""
mz_index_columns               ""
mz_indexes                     ""
mz_kafka_connections           ""
mz_kafka_sinks                 ""
mz_kafka_sources               ""
mz_list_types                  ""
mz_map_types                   ""
mz_materialized_views          ""
mz_operators                   ""
mz_pseudo_types                ""
mz_role_auth                   ""
mz_roles                       ""
mz_role_members                ""
mz_role_parameters             ""
mz_schemas                     ""
mz_secrets                     ""
mz_sinks                       ""
mz_sources                     ""
mz_ssh_tunnel_connections      ""
mz_system_privileges           ""
mz_tables                      ""
mz_types                       ""
mz_views                       ""

> SHOW VIEWS FROM mz_catalog
name                       comment
-----------------------------------
mz_objects                 ""
mz_relations               ""
mz_recent_storage_usage    ""
mz_storage_usage           ""
mz_timezone_abbreviations  ""
mz_timezone_names          ""

# Check default sources, tables, and views in mz_catalog_unstable.

> SHOW SOURCES FROM mz_catalog_unstable

> SHOW TABLES FROM mz_catalog_unstable

> SHOW VIEWS FROM mz_catalog_unstable

# Check default sources, tables, and views in mz_internal.

> SHOW SOURCES FROM mz_internal
name                                         type    cluster    comment
-----------------------------------------------------------------------
mz_aws_privatelink_connection_status_history source  <null>     ""
mz_cluster_replica_metrics_history           source  <null>     ""
mz_cluster_replica_status_history            source  <null>     ""
mz_compute_dependencies                      source  <null>     ""
mz_compute_error_counts_raw_unified          source  <null>     ""
mz_compute_hydration_times                   source  <null>     ""
mz_compute_operator_hydration_statuses       source  <null>     ""
mz_frontiers                                 source  <null>     ""
mz_materialized_view_refreshes               source  <null>     ""
mz_prepared_statement_history                source  <null>     ""
mz_session_history                           source  <null>     ""
mz_sink_statistics_raw                       source  <null>     ""
mz_sink_status_history                       source  <null>     ""
mz_source_statistics_raw                     source  <null>     ""
mz_source_status_history                     source  <null>     ""
mz_sql_text                                  source  <null>     ""
mz_statement_execution_history               source  <null>     ""
mz_statement_lifecycle_history               source  <null>     ""
mz_storage_shards                            source  <null>     ""
mz_wallclock_global_lag_histogram_raw        source  <null>     ""
mz_wallclock_lag_history                     source  <null>     ""

> SHOW TABLES FROM mz_internal
name                                     comment
------------------------------------------------
mz_aggregates                            ""
mz_aws_connections                       ""
mz_cluster_schedules                     ""
mz_cluster_workload_classes              ""
mz_comments                              ""
mz_continual_tasks                       ""
mz_history_retention_strategies          ""
mz_internal_cluster_replicas             ""
mz_pending_cluster_replicas              ""
mz_kafka_source_tables                   ""
mz_license_keys                          ""
mz_materialized_view_refresh_strategies  ""
mz_mysql_source_tables                   ""
mz_network_policies                      ""
mz_network_policy_rules                  ""
mz_object_dependencies                   ""
mz_object_global_ids                     ""
mz_optimizer_notices                     ""
mz_postgres_sources                      ""
mz_postgres_source_tables                ""
mz_replacements                          ""
mz_sessions                              ""
mz_source_references                     ""
mz_sql_server_source_tables              ""
mz_storage_usage_by_shard                ""
mz_subscriptions                         ""
mz_type_pg_metadata                      ""
mz_webhook_sources                       ""

> SHOW VIEWS FROM mz_internal
name                                     comment
------------------------------------------------
mz_activity_log_thinned                  ""
mz_cluster_deployment_lineage            ""
mz_cluster_replica_history               ""
mz_cluster_replica_metrics               ""
mz_cluster_replica_name_history          ""
mz_cluster_replica_statuses              ""
mz_cluster_replica_utilization           ""
mz_cluster_replica_utilization_history   ""
mz_compute_hydration_statuses            ""
mz_console_cluster_utilization_overview  ""
mz_show_continual_tasks                  ""
mz_global_frontiers                      ""
mz_hydration_statuses                    ""
mz_index_advice                          ""
mz_materialization_dependencies          ""
mz_materialization_lag                   ""
mz_notices                               ""
mz_notices_redacted                      ""
mz_object_fully_qualified_names          ""
mz_object_history                        ""
mz_object_lifetimes                      ""
mz_object_oid_alias                      ""
mz_object_transitive_dependencies        ""
mz_objects_id_namespace_types            ""
mz_recent_activity_log                   ""
mz_recent_activity_log_thinned           ""
mz_recent_activity_log_redacted          ""
mz_recent_sql_text                       ""
mz_recent_sql_text_redacted              ""
mz_show_all_my_privileges                ""
mz_show_all_objects                      ""
mz_show_all_privileges                   ""
mz_show_cluster_privileges               ""
mz_show_cluster_replicas                 ""
mz_show_clusters                         ""
mz_show_columns                          ""
mz_show_connections                      ""
mz_show_database_privileges              ""
mz_show_databases                        ""
mz_show_default_privileges               ""
mz_show_indexes                          ""
mz_show_materialized_views               ""
mz_show_my_cluster_privileges            ""
mz_show_my_database_privileges           ""
mz_show_my_default_privileges            ""
mz_show_my_object_privileges             ""
mz_show_my_role_members                  ""
mz_show_my_schema_privileges             ""
mz_show_my_system_privileges             ""
mz_show_network_policies                 ""
mz_show_object_privileges                ""
mz_show_role_members                     ""
mz_show_roles                            ""
mz_show_schema_privileges                ""
mz_show_schemas                          ""
mz_show_secrets                          ""
mz_show_sinks                            ""
mz_show_sources                          ""
mz_show_system_privileges                ""
mz_show_tables                           ""
mz_show_types                            ""
mz_show_views                            ""
mz_sink_statistics                       ""
mz_sink_statuses                         ""
mz_source_statistics                     ""
mz_source_statistics_with_history        ""
mz_source_statuses                       ""
mz_sql_text_redacted                     ""
mz_aws_privatelink_connection_statuses   ""
mz_statement_execution_history_redacted  ""
mz_wallclock_global_lag                  ""
mz_wallclock_global_lag_histogram        ""
mz_wallclock_global_lag_history          ""
mz_wallclock_global_lag_recent_history   ""
pg_class_all_databases                   ""
pg_type_all_databases                    ""
pg_namespace_all_databases               ""
pg_description_all_databases             ""
pg_attrdef_all_databases                 ""
pg_attribute_all_databases               ""

# Check default sources, tables, and views in mz_introspection.

> SHOW SOURCES FROM mz_introspection
mz_active_peeks_per_worker                   log   <null>    ""
mz_arrangement_batcher_allocations_raw       log   <null>    ""
mz_arrangement_batcher_capacity_raw          log   <null>    ""
mz_arrangement_batcher_records_raw           log   <null>    ""
mz_arrangement_batcher_size_raw              log   <null>    ""
mz_arrangement_batches_raw                   log   <null>    ""
mz_arrangement_heap_allocations_raw          log   <null>    ""
mz_arrangement_heap_capacity_raw             log   <null>    ""
mz_arrangement_heap_size_raw                 log   <null>    ""
mz_arrangement_records_raw                   log   <null>    ""
mz_arrangement_sharing_raw                   log   <null>    ""
mz_compute_dataflow_global_ids_per_worker    log   <null>    ""
mz_compute_error_counts_raw                  log   <null>    ""
mz_compute_exports_per_worker                log   <null>    ""
mz_compute_frontiers_per_worker              log   <null>    ""
mz_compute_hydration_times_per_worker        log   <null>    ""
mz_compute_import_frontiers_per_worker       log   <null>    ""
mz_compute_lir_mapping_per_worker            log   <null>    ""
mz_compute_operator_durations_histogram_raw  log   <null>    ""
mz_compute_operator_hydration_statuses_per_worker log <null> ""
mz_dataflow_addresses_per_worker             log   <null>    ""
mz_dataflow_channels_per_worker              log   <null>    ""
mz_dataflow_operator_reachability_raw        log   <null>    ""
mz_dataflow_operators_per_worker             log   <null>    ""
mz_dataflow_shutdown_durations_histogram_raw log   <null>    ""
mz_message_counts_received_raw               log   <null>    ""
mz_message_counts_sent_raw                   log   <null>    ""
mz_message_batch_counts_received_raw         log   <null>    ""
mz_message_batch_counts_sent_raw             log   <null>    ""
mz_peek_durations_histogram_raw              log   <null>    ""
mz_scheduling_elapsed_raw                    log   <null>    ""
mz_scheduling_parks_histogram_raw            log   <null>    ""

> SHOW TABLES FROM mz_introspection

> SHOW VIEWS FROM mz_introspection
mz_active_peeks ""
mz_arrangement_sharing ""
mz_arrangement_sharing_per_worker ""
mz_arrangement_sizes ""
mz_arrangement_sizes_per_worker ""
mz_compute_error_counts ""
mz_compute_error_counts_per_worker ""
mz_compute_exports ""
mz_compute_frontiers ""
mz_compute_import_frontiers ""
mz_compute_operator_durations_histogram ""
mz_compute_operator_durations_histogram_per_worker ""
mz_dataflow_addresses ""
mz_dataflow_arrangement_sizes ""
mz_dataflow_channel_operators ""
mz_dataflow_channel_operators_per_worker ""
mz_dataflow_channels ""
mz_dataflow_global_ids ""
mz_dataflow_operator_dataflows ""
mz_dataflow_operator_dataflows_per_worker ""
mz_dataflow_operator_parents ""
mz_dataflow_operator_parents_per_worker ""
mz_dataflow_operator_reachability ""
mz_dataflow_operator_reachability_per_worker ""
mz_dataflow_operators ""
mz_dataflow_shutdown_durations_histogram ""
mz_dataflow_shutdown_durations_histogram_per_worker ""
mz_dataflows ""
mz_dataflows_per_worker ""
mz_expected_group_size_advice ""
mz_lir_mapping ""
mz_mappable_objects ""
mz_message_counts ""
mz_message_counts_per_worker ""
mz_peek_durations_histogram ""
mz_peek_durations_histogram_per_worker ""
mz_records_per_dataflow ""
mz_records_per_dataflow_operator ""
mz_records_per_dataflow_operator_per_worker ""
mz_records_per_dataflow_per_worker ""
mz_scheduling_elapsed ""
mz_scheduling_elapsed_per_worker ""
mz_scheduling_parks_histogram ""
mz_scheduling_parks_histogram_per_worker ""

> SET database = materialize

> CREATE SCHEMA tester

> SHOW TABLES FROM tester

> CREATE TABLE tester.test_table (a int)

> SHOW TABLES FROM tester
test_table ""

# `SHOW TABLES` and `mz_tables` should agree.
> SELECT COUNT(*) FROM mz_tables WHERE id LIKE 's%'
65

# There is one entry in mz_indexes for each field_number/expression of the index.
> SELECT COUNT(id) FROM mz_indexes WHERE id LIKE 's%'
263

# Create a second schema with the same table name as above
> CREATE SCHEMA tester2

> CREATE TABLE tester2.test_table (a int)

$ psql-execute command="\dt tester.*"
\             List of relations
 Schema |    Name    | Type  |    Owner
--------+------------+-------+-------------
 tester | test_table | table | materialize

$ psql-execute command="\dt tester.test_table"
\             List of relations
 Schema |    Name    | Type  |    Owner
--------+------------+-------+-------------
 tester | test_table | table | materialize

$ psql-execute command="\dt *.test_table"
\             List of relations
 Schema  |    Name    | Type  |    Owner
---------+------------+-------+-------------
 tester  | test_table | table | materialize
 tester2 | test_table | table | materialize

> CREATE TYPE type1 AS LIST (ELEMENT TYPE = text)

> SHOW TYPES
type1  ""

$ psql-execute command="\dT"
\              List of data types
   Schema   |       Name        | Description
------------+-------------------+-------------
 mz_catalog | anycompatiblelist |
 mz_catalog | anycompatiblemap  |
 mz_catalog | list              |
 mz_catalog | map               |
 mz_catalog | mz_aclitem        |
 mz_catalog | mz_timestamp      |
 mz_catalog | uint2             |
 mz_catalog | uint4             |
 mz_catalog | uint8             |
 public     | type1             |
