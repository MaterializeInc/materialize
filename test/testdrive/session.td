# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Cloudtest is still running with RBAC off, while the mzcompose-based tests
# have it enabled, so enable it explicitly for this test so that we have
# consistent output in "SHOW ALL".
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_rbac_checks = true;

$ set-regex match="cluster1|^quickstart$|v\d+\.\d+\.\d+(-[a-z0-9]+)?(\.\d+)? \([a-f0-9]{9}\)" replacement=<VARIES>

> SHOW ALL
allowed_cluster_replica_sizes            ""                      "The allowed sizes when creating a new cluster replica (Materialize)."
application_name                         ""                      "Sets the application name to be reported in statistics and logs (PostgreSQL)."
auto_route_catalog_queries               on                      "Whether to force queries that depend only on system tables, to run on the mz_catalog_server cluster (Materialize)."
client_encoding                          UTF8                    "Sets the client's character set encoding (PostgreSQL)."
client_min_messages                      notice                  "Sets the message levels that are sent to the client (PostgreSQL)."
cluster                                  <VARIES>                "Sets the current cluster (Materialize)."
cluster_replica                          ""                      "Sets a target cluster replica for SELECT queries (Materialize)."
current_object_missing_warnings          on                      "Whether to emit warnings when the current database, schema, or cluster is missing (Materialize)."
database                                 materialize             "Sets the current database (CockroachDB)."
DateStyle                                "ISO, MDY"              "Sets the display format for date and time values (PostgreSQL)."
default_cluster_replication_factor       1                       "Default cluster replication factor (Materialize)."
emit_introspection_query_notice          on                      "Whether to print a notice when querying per-replica introspection sources."
emit_plan_insights_notice                off                     "Boolean flag indicating whether to send a NOTICE with JSON-formatted plan insights before executing a SELECT statement (Materialize)."
emit_timestamp_notice                    off                     "Boolean flag indicating whether to send a NOTICE with timestamp explanations of queries (Materialize)."
emit_trace_id_notice                     off                     "Boolean flag indicating whether to send a NOTICE specifying the trace id when available (Materialize)."
enable_rbac_checks                       on                      "User facing global boolean flag indicating whether to apply RBAC checks before executing statements (Materialize)."
enable_reduce_reduction                  on                      "split complex reductions in to simpler ones and a join (Materialize)."
enable_session_rbac_checks               off                     "User facing session boolean flag indicating whether to apply RBAC checks before executing statements (Materialize)."
extra_float_digits                       3                       "Adjusts the number of digits displayed for floating-point values (PostgreSQL)."
failpoints                               <omitted>               "Allows failpoints to be dynamically activated."
idle_in_transaction_session_timeout      "2 min"                 "Sets the maximum allowed duration that a session can sit idle in a transaction before being terminated. If this value is specified without units, it is taken as milliseconds. A value of zero disables the timeout (PostgreSQL)."
integer_datetimes                        on                      "Reports whether the server uses 64-bit-integer dates and times (PostgreSQL)."
IntervalStyle                            postgres                "Sets the display format for interval values (PostgreSQL)."
is_superuser                             off                     "Reports whether the current session is a superuser (PostgreSQL)."
max_aws_privatelink_connections          0                       "The maximum number of AWS PrivateLink connections in the region, across all schemas (Materialize)."
max_clusters                             25                      "The maximum number of clusters in the region (Materialize)."
max_connections                          5000                    "The maximum number of concurrent connections (PostgreSQL)."
max_continual_tasks                      100                     "The maximum number of continual tasks in the region, across all schemas (Materialize)."
max_copy_from_size                       1073741824              "The maximum size in bytes we buffer for COPY FROM statements (Materialize)."
max_credit_consumption_rate              1024                    "The maximum rate of credit consumption in a region. Credits are consumed based on the size of cluster replicas in use (Materialize)."
max_databases                            1000                    "The maximum number of databases in the region (Materialize)."
max_identifier_length                    255                     "The maximum length of object identifiers in bytes (PostgreSQL)."
max_kafka_connections                    1000                    "The maximum number of Kafka connections in the region, across all schemas (Materialize)."
max_materialized_views                   500                     "The maximum number of materialized views in the region, across all schemas (Materialize)."
max_mysql_connections                    1000                    "The maximum number of MySQL connections in the region, across all schemas (Materialize)."
max_network_policies                     25                      "The maximum number of network policies in the region."
max_objects_per_schema                   1000                    "The maximum number of objects in a schema (Materialize)."
max_postgres_connections                 1000                    "The maximum number of PostgreSQL connections in the region, across all schemas (Materialize)."
max_query_result_size                    "1GB"                   "The maximum size in bytes for a single query's result (Materialize)."
max_replicas_per_cluster                 5                       "The maximum number of replicas of a single cluster (Materialize)."
max_result_size                          "1GB"                   "The maximum size in bytes for an internal query result (Materialize)."
max_roles                                1000                    "The maximum number of roles in the region (Materialize)."
max_rules_per_network_policy             25                      "The maximum number of rules per network policies."
max_schemas_per_database                 1000                    "The maximum number of schemas in a database (Materialize)."
max_secrets                              100                     "The maximum number of secrets in the region, across all schemas (Materialize)."
max_sinks                                1000                    "The maximum number of sinks in the region, across all schemas (Materialize)."
max_sources                              200                     "The maximum number of sources in the region, across all schemas (Materialize)."
max_sql_server_connections               1000                    "The maximum number of SQL Server connections in the region, across all schemas (Materialize)."
max_tables                               200                     "The maximum number of tables in the region, across all schemas (Materialize)."
network_policy                           default                 "Sets the fallback network policy applied to all users without an explicit policy."
optimizer_e2e_latency_warning_threshold  "500 ms"                "Sets the duration that a query can take to compile; queries that take longer will trigger a warning. If this value is specified without units, it is taken as milliseconds. A value of zero disables the timeout (Materialize)."
mz_version                               <VARIES>                "Shows the Materialize server version (Materialize)."
scram_iterations                         600000                  "Iterations to use when hashing passwords. Higher iterations are more secure, but take longer to validated. Please consider the security risks before reducing this below the default value."
real_time_recency                        off                     "Feature flag indicating whether real time recency is enabled (Materialize)."
real_time_recency_timeout                "10 s"                  "Sets the maximum allowed duration of SELECTs that actively use real-time recency, i.e. reach out to an external system to determine their most recencly exposed data (Materialize)."
search_path                              public                  "Sets the schema search order for names that are not schema-qualified (PostgreSQL)."
server_version                           9.5.0                   "Shows the PostgreSQL compatible server version (PostgreSQL)."
server_version_num                       90500                   "Shows the PostgreSQL compatible server version as an integer (PostgreSQL)."
sql_safe_updates                         off                     "Prohibits SQL statements that may be overly destructive (CockroachDB)."
standard_conforming_strings              on                      "Causes '...' strings to treat backslashes literally (PostgreSQL)."
statement_logging_default_sample_rate    0.01                    "The default value of `statement_logging_sample_rate` for new sessions (Materialize)."
statement_logging_max_sample_rate        0.01                    "The maximum rate at which statements may be logged. If this value is less than that of `statement_logging_sample_rate`, the latter is ignored (Materialize)."
statement_logging_sample_rate            0.01                    "User-facing session variable indicating how many statement executions should be logged, subject to constraint by the system variable `statement_logging_max_sample_rate` (Materialize)."
statement_timeout                        "1 min"                  "Sets the maximum allowed duration of INSERT...SELECT, UPDATE, and DELETE operations. If this value is specified without units, it is taken as milliseconds."
superuser_reserved_connections           3                       "The number of connections that are reserved for superusers (PostgreSQL)."
TimeZone                                 UTC                     "Sets the time zone for displaying and interpreting time stamps (PostgreSQL)."
transaction_isolation                    "strict serializable"   "Sets the current transaction's isolation level (PostgreSQL)."
unsafe_new_transaction_wall_time         ""                      "Sets the wall time for all new explicit or implicit transactions to control the value of `now()`. If not set, uses the system's clock."
welcome_message                          on                      "Whether to send a notice with a welcome message after a successful connection (Materialize)."
enable_consolidate_after_union_negate    on                      "consolidation after Unions that have a Negated input (Materialize)."
force_source_table_syntax                off                     "Force use of new source model (CREATE TABLE .. FROM SOURCE) and migrate existing sources"

> SET application_name = 'foo'

> SHOW client_encoding
UTF8

> SHOW cLiENt_EnCoDInG
UTF8

! SET client_encoding = UTF9
contains:invalid value for parameter "client_encoding": "utf9"

# if its utf8 we let it through
> SET NAMES 'UTF8';

# match the behavior of postgres as specified here: https://www.postgresql.org/docs/9.1/sql-set.html
! SET NAMES = "something";
contains:unrecognized configuration parameter "names"

# standard_conforming_strings is constrained to true
> SET standard_conforming_strings = ON;

> SET standard_conforming_strings = true;

> SET standard_conforming_strings TO TRUE;

! SET standard_conforming_strings = OFF;
contains:parameter "standard_conforming_strings" can only be set to "on"

! SET standard_conforming_strings = typo;
contains:parameter "standard_conforming_strings" requires a "boolean" value

> SET sql_safe_updates = on
> SHOW sql_safe_updates
on
> SET sql_safe_updates = 'on'
> SET sql_safe_updates = 't'
> SET sql_safe_updates = 'true'
> SET sql_safe_updates = true
> SET sql_safe_updates = 'off'
> SET sql_safe_updates = 'f'
> SET sql_safe_updates = 'false'
> SET sql_safe_updates = false
> SHOW sql_safe_updates
off

# `database` is tested elsewhere.

> SET extra_float_digits = 1

> SET DateStyle = 'ISO'
> SET DateStyle = 'MDY'
> SET DateStyle = 'ISO,MDY'
! SET DateStyle = 'ooga booga'
contains:parameter "DateStyle" can only be set to "ISO, MDY"

# `search_path` is tested elsewhere.

! SET server_version = "9.6.0"
contains:parameter "server_version" cannot be changed

! SET server_version_num = "90600"
contains:parameter "server_version_num" cannot be changed

! SET TimeZone = 'nope'
contains:invalid value for parameter "TimeZone": "nope"

# The `transaction_isolation` variable has dedicated syntax as mandated by the
# SQL standard.
> SHOW TRANSACTION ISOLATION LEVEL
"strict serializable"

! SET transaction_isolation = 'read draft'
contains:invalid value for parameter "transaction_isolation": "read draft"

! SET integer_datetimes = false
contains:parameter "integer_datetimes" can only be set to "on"

> SET client_min_messages TO ERROR
> SHOW client_min_messages
error

> SET client_min_messages = warning
> SHOW client_min_messages
warning

> SET client_min_messages = 'Notice'
> SHOW client_min_messages
notice

> SET client_min_messages = LoG
> SHOW client_min_messages
log

> SET client_min_messages to 'debug4'
> SHOW client_min_messages
debug4

# postgres behavior corner cases
> SET client_min_messages to 'debug'
> SHOW client_min_messages
debug2

> SET client_min_messages to 'info'
> SHOW client_min_messages
info

! SET client_min_messages to invalid
contains:invalid value for parameter "client_min_messages": "invalid"

> SHOW intervalstyle
postgres

! SET intervalstyle = 'postgres-legacy'
contains:invalid value for parameter "IntervalStyle": "postgres-legacy"

> SET intervalstyle = 'postgres';

# Test some other parameter value codepaths, like `Option<String>` here
> SET cluster_replica = ''

> SET cluster_replica = 'r1'

! SET cluster_replica = 1, 2
contains:parameter "cluster_replica" cannot have value "1","2": expects a single value

! SET statement_logging_sample_rate = 1.1
contains:parameter "statement_logging_sample_rate" cannot have value "1.1": only supports values in range 0.0..=1.0

> SET max_query_result_size = '120 MB';
> SHOW max_query_result_size
"120MB"

> SET max_query_result_size = 1048576 -- 1MB
> SHOW max_query_result_size
"1MB"

> SET max_query_result_size = 10240 -- 10KB
> SHOW max_query_result_size
"10kB"
