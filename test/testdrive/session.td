# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-regex match="cluster1|default|v\d+\.\d+\.\d+(-[a-z0-9]+)? \([a-f0-9]{9}\)" replacement=<VARIES>

> SHOW ALL
allowed_cluster_replica_sizes           ""                     "The allowed sizes when creating a new cluster replica (Materialize)."
application_name                        ""                     "Sets the application name to be reported in statistics and logs (PostgreSQL)."
auto_route_introspection_queries        on                     "Whether to force queries that depend only on system tables, to run on the mz_introspection cluster (Materialize)."
client_encoding                         UTF8                   "Sets the client's character set encoding (PostgreSQL)."
client_min_messages                     notice                 "Sets the message levels that are sent to the client (PostgreSQL)."
cluster                                 <VARIES>               "Sets the current cluster (Materialize)."
cluster_replica                         ""                     "Sets a target cluster replica for SELECT queries (Materialize)."
database                                materialize            "Sets the current database (CockroachDB)."
DateStyle                               "ISO, MDY"             "Sets the display format for date and time values (PostgreSQL)."
enable_rbac_checks                      off                    "User facing global boolean flag indicating whether to apply RBAC checks before executing statements (Materialize)."
enable_session_rbac_checks              off                    "User facing session boolean flag indicating whether to apply RBAC checks before executing statements (Materialize)."
emit_timestamp_notice                   off                    "Boolean flag indicating whether to send a NOTICE specifying query timestamps (Materialize)."
emit_trace_id_notice                    off                    "Boolean flag indicating whether to send a NOTICE specifying the trace id when available (Materialize)."
extra_float_digits                      3                      "Adjusts the number of digits displayed for floating-point values (PostgreSQL)."
failpoints                              ""                     "Allows failpoints to be dynamically activated."
idle_in_transaction_session_timeout     "2 min"                "Sets the maximum allowed duration that a session can sit idle in a transaction before being terminated. If this value is specified without units, it is taken as milliseconds. A value of zero disables the timeout (PostgreSQL)."
integer_datetimes                       on                     "Reports whether the server uses 64-bit-integer dates and times (PostgreSQL)."
IntervalStyle                           postgres               "Sets the display format for interval values (PostgreSQL)."
is_superuser                            off                    "Reports whether the current session is a superuser (PostgreSQL)."
max_aws_privatelink_connections         0                      "The maximum number of AWS PrivateLink connections in the region, across all schemas (Materialize)."
max_clusters                            10                     "The maximum number of clusters in the region (Materialize)."
max_databases                           1000                   "The maximum number of databases in the region (Materialize)."
max_materialized_views                  100                    "The maximum number of materialized views in the region, across all schemas (Materialize)."
max_objects_per_schema                  1000                   "The maximum number of objects in a schema (Materialize)."
max_replicas_per_cluster                5                      "The maximum number of replicas of a single cluster (Materialize)."
max_result_size                         1073741824             "The maximum size in bytes for a single query's result (Materialize)."
max_roles                               1000                   "The maximum number of roles in the region (Materialize)."
max_schemas_per_database                1000                   "The maximum number of schemas in a database (Materialize)."
max_secrets                             100                    "The maximum number of secrets in the region, across all schemas (Materialize)."
max_sinks                               25                     "The maximum number of sinks in the region, across all schemas (Materialize)."
max_sources                             25                     "The maximum number of sources in the region, across all schemas (Materialize)."
max_tables                              25                     "The maximum number of tables in the region, across all schemas (Materialize)."
mz_version                              <VARIES>               "Shows the Materialize server version (Materialize)."
search_path                             "public"               "Sets the schema search order for names that are not schema-qualified (PostgreSQL)."
server_version                          9.5.0                  "Shows the PostgreSQL compatible server version (PostgreSQL)."
server_version_num                      90500                  "Shows the PostgreSQL compatible server version as an integer (PostgreSQL)."
sql_safe_updates                        off                    "Prohibits SQL statements that may be overly destructive (CockroachDB)."
standard_conforming_strings             on                     "Causes '...' strings to treat backslashes literally (PostgreSQL)."
statement_timeout                       "10 s"                 "Sets the maximum allowed duration of INSERT...SELECT, UPDATE, and DELETE operations. If this value is specified without units, it is taken as milliseconds."
TimeZone                                UTC                    "Sets the time zone for displaying and interpreting time stamps (PostgreSQL)."
transaction_isolation                   "strict serializable"  "Sets the current transaction's isolation level (PostgreSQL)."

> SET application_name = 'foo'

> SHOW client_encoding
UTF8

> SHOW cLiENt_EnCoDInG
UTF8

! SET client_encoding = UTF9
contains:parameter "client_encoding" can only be set to "UTF8"

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
contains:parameter "integer_datetimes" cannot be changed

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
contains:parameter "IntervalStyle" can only be set to "postgres"

> SET intervalstyle = 'postgres';
