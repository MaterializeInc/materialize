# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=1s
$ set-max-tries max-tries=5

$ set-arg-default single-replica-cluster=quickstart

$ set-regex match=cluster1|quickstart replacement=<VARIABLE_OUTPUT>

$ set writer-schema={
    "name": "row",
    "type": "record",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "int"}
    ]
  }

$ kafka-create-topic topic=data
$ kafka-ingest topic=data format=avro schema=${writer-schema}
{"a": 1, "b": 1}

# Sources do not have indexes automatically created
> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE data
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${writer-schema}'

> SET CLUSTER TO mz_catalog_server
> SHOW INDEXES ON data

> SHOW INDEXES ON data
name    on  cluster  key  comment
--------------------------------------------------------------------------

> SET CLUSTER TO quickstart

# Sources can have default indexes added
> CREATE DEFAULT INDEX ON data

> SET CLUSTER TO mz_catalog_server
> SHOW INDEXES ON data
name                on      cluster             key    comment
-------------------------------------------------------------------------------------------
data_primary_idx    data    <VARIABLE_OUTPUT>   {a,b}  ""

> SET CLUSTER TO quickstart

> SELECT index_position FROM mz_index_columns WHERE index_id LIKE '%u%'
index_position
--------------
1
2

> SELECT position, name FROM mz_columns where id LIKE '%u%';
position         name
----------------------
1                a
1                partition
2                b
2                offset

# Views do not have indexes automatically created
> CREATE VIEW data_view as SELECT * from data

> SET CLUSTER TO mz_catalog_server
> SHOW INDEXES ON data_view
name    on  cluster  key  comment
--------------------------------------------------------------------------

> SET CLUSTER TO quickstart

# Views can have default indexes added
> CREATE DEFAULT INDEX ON data_view

> SET CLUSTER TO mz_catalog_server
> SHOW INDEXES ON data_view
name                    on          cluster             key     comment
---------------------------------------------------------------------------------------------------
data_view_primary_idx   data_view   <VARIABLE_OUTPUT>   {a,b}   ""
> SET CLUSTER TO quickstart

# Materialized views do not have indexes automatically created
> CREATE MATERIALIZED VIEW matv AS
  SELECT b, sum(a) FROM data GROUP BY b

> SET CLUSTER TO mz_catalog_server
> SHOW INDEXES ON matv
name    on  cluster  key  comment
--------------------------------------------------------------------------

> SET CLUSTER TO quickstart

# Materialized views can have default indexes added
> CREATE DEFAULT INDEX ON matv

> SET CLUSTER TO mz_catalog_server
> SHOW INDEXES ON matv
name                on      cluster             key    comment
--------------------------------------------------------------------------------------------
matv_primary_idx    matv    <VARIABLE_OUTPUT>   {b}    ""

> SET CLUSTER TO quickstart

# IF NOT EXISTS prevents adding multiple default indexes
> CREATE DEFAULT INDEX IF NOT EXISTS ON data_view

> SET CLUSTER TO mz_catalog_server
> SHOW INDEXES ON data_view
name                    on          cluster             key    comment
-------------------------------------------------------------------------------------------------
data_view_primary_idx   data_view   <VARIABLE_OUTPUT>   {a,b}  ""

> SET CLUSTER TO quickstart

# Additional default indexes have the same structure as the first
> CREATE DEFAULT INDEX ON matv

> SET CLUSTER TO mz_catalog_server
> SHOW INDEXES ON matv
name                on      cluster             key    comment
------------------------------------------------------------------------------------------------
matv_primary_idx    matv    <VARIABLE_OUTPUT>   {b}    ""
matv_primary_idx1   matv    <VARIABLE_OUTPUT>   {b}    ""

> SET CLUSTER TO quickstart

# Default indexes can be named
> CREATE DEFAULT INDEX named_idx ON data_view

> SET CLUSTER TO mz_catalog_server
> SHOW INDEXES ON data_view
name                    on          cluster             key      comment
-----------------------------------------------------------------------------------------------
data_view_primary_idx   data_view   <VARIABLE_OUTPUT>   {a,b}    ""
named_idx               data_view   <VARIABLE_OUTPUT>   {a,b}    ""

> SET CLUSTER TO quickstart

> DROP INDEX data_view_primary_idx
> DROP INDEX named_idx

# Indexes with specified columns can be automatically named
> CREATE INDEX ON data_view(a)

> SET CLUSTER TO mz_catalog_server
> SHOW INDEXES ON data_view
name            on          cluster             key    comment
-------------------------------------------------------------------------------------------
data_view_a_idx data_view   <VARIABLE_OUTPUT>   {a}    ""

> SET CLUSTER TO quickstart

> DROP INDEX data_view_a_idx

# Automatically named indexes rename expression columns to "expr" and join all columns with underscores.
> CREATE INDEX ON data_view(b, a)
> CREATE INDEX ON data_view(b - a, a)

> SET CLUSTER TO mz_catalog_server
> SHOW INDEXES ON data_view
name                    on          cluster             key          comment
-----------------------------------------------------------------------------------------------
data_view_b_a_idx       data_view   <VARIABLE_OUTPUT>   {b,a}        ""
data_view_expr_a_idx    data_view   <VARIABLE_OUTPUT>   "{b - a,a}"  ""
> SET CLUSTER TO quickstart

> DROP INDEX data_view_b_a_idx
> DROP INDEX data_view_expr_a_idx

# Indexes can be both explicitly named and explicitly structured
> CREATE INDEX named_idx ON data_view (b - a, a)

> SET CLUSTER TO mz_catalog_server
> SHOW INDEXES ON data_view
name        on          cluster             key          comment
---------------------------------------------------------------------------------------------
named_idx   data_view   <VARIABLE_OUTPUT>   "{b - a,a}"  ""

> SET CLUSTER TO quickstart

> DROP INDEX named_idx

# Default indexes only check for names, not structures
> CREATE INDEX data_view_primary_idx ON data_view (b - a, a)
> CREATE DEFAULT INDEX IF NOT EXISTS ON data_view

> SET CLUSTER TO mz_catalog_server
> SHOW INDEXES ON data_view
name                    on          cluster             key          comment
------------------------------------------------------------------------------------------------------
data_view_primary_idx   data_view   <VARIABLE_OUTPUT>   "{b - a,a}"  ""

> SET CLUSTER TO quickstart

> SHOW CREATE INDEX data_view_primary_idx
name                                     create_sql
--------------------------------------------------------------------------------------------------------------------------------------
materialize.public.data_view_primary_idx "CREATE INDEX \"data_view_primary_idx\" IN CLUSTER \"<VARIABLE_OUTPUT>\" ON \"materialize\".\"public\".\"data_view\" (\"b\" - \"a\", \"a\")"

> CREATE TABLE foo (
    a int NOT NULL,
    b decimal(13, 1),
    z text
  )
> CREATE DEFAULT INDEX ON foo
> CREATE INDEX ON foo (a + b)
> CREATE INDEX ON foo (substr(z, 3))
> SET CLUSTER TO mz_catalog_server
> SHOW INDEXES ON foo
foo_primary_idx foo <VARIABLE_OUTPUT>   {a,b,z}                      ""
foo_expr_idx    foo <VARIABLE_OUTPUT>   "{a + b}"                    ""
foo_expr_idx1   foo <VARIABLE_OUTPUT>   "{pg_catalog.substr(z, 3)}"  ""
> SHOW INDEXES ON foo WHERE name = 'noexist'
> SHOW INDEXES ON foo WHERE name = 'foo_expr_idx'
foo_expr_idx    foo <VARIABLE_OUTPUT>   "{a + b}"    ""

> SHOW INDEXES ON foo LIKE 'foo_primary%'
foo_primary_idx foo <VARIABLE_OUTPUT>   {a,b,z}    ""

! SHOW INDEXES ON nonexistent
contains:unknown catalog item 'nonexistent'

! SHOW INDEXES ON foo_primary_idx
contains:cannot show indexes on materialize.public.foo_primary_idx because it is a index

> SET CLUSTER TO quickstart

> CREATE CLUSTER clstr REPLICAS (r1 (SIZE '1'))
> CREATE DEFAULT INDEX IN CLUSTER clstr ON foo;
> SET CLUSTER TO mz_catalog_server
> SHOW INDEXES IN CLUSTER clstr WHERE on = 'foo'
foo_primary_idx1    foo clstr   {a,b,z}    ""

> SHOW INDEXES FROM public WHERE name = 'foo_primary_idx1'
foo_primary_idx1    foo clstr   {a,b,z}    ""

> SET CLUSTER TO quickstart
> DROP TABLE foo CASCADE
> DROP SOURCE data CASCADE

> SET CLUSTER TO mz_catalog_server

! SHOW INDEXES FROM public ON foo
contains:Cannot specify both FROM and ON

! SHOW INDEXES FROM nonexistent
contains:unknown schema 'nonexistent'

> SET CLUSTER TO quickstart

> CREATE TABLE bar ();
> CREATE INDEX bar_ind ON bar ();

> SET CLUSTER TO mz_catalog_server
> SHOW INDEXES
bar_ind bar <VARIABLE_OUTPUT> {}  ""
> SET CLUSTER TO quickstart

> DROP TABLE bar CASCADE
> CREATE SCHEMA foo
> CREATE TABLE foo.bar (a INT)
> CREATE INDEX bar_ind ON foo.bar (a)

> SET CLUSTER TO mz_catalog_server
> SHOW INDEXES ON foo.bar
bar_ind bar <VARIABLE_OUTPUT> {a}  ""
> SET CLUSTER TO quickstart

> DROP CLUSTER clstr CASCADE;

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET enable_rbac_checks TO true

>[version<=13100] SHOW INDEXES IN CLUSTER mz_catalog_server
mz_active_peeks_per_worker_s2_primary_idx                   mz_active_peeks_per_worker                   mz_catalog_server    {id,worker_id}                              ""
mz_arrangement_batches_raw_s2_primary_idx                   mz_arrangement_batches_raw                   mz_catalog_server    {operator_id,worker_id}                     ""
mz_arrangement_records_raw_s2_primary_idx                   mz_arrangement_records_raw                   mz_catalog_server    {operator_id,worker_id}                     ""
mz_arrangement_sharing_raw_s2_primary_idx                   mz_arrangement_sharing_raw                   mz_catalog_server    {operator_id,worker_id}                     ""
mz_arrangement_heap_capacity_raw_s2_primary_idx             mz_arrangement_heap_capacity_raw             mz_catalog_server    {operator_id,worker_id}                     ""
mz_arrangement_heap_allocations_raw_s2_primary_idx          mz_arrangement_heap_allocations_raw          mz_catalog_server    {operator_id,worker_id}                     ""
mz_arrangement_heap_size_raw_s2_primary_idx                 mz_arrangement_heap_size_raw                 mz_catalog_server    {operator_id,worker_id}                     ""
mz_arrangement_batcher_allocations_raw_s2_primary_idx       mz_arrangement_batcher_allocations_raw       mz_catalog_server    {operator_id,worker_id}                     ""
mz_arrangement_batcher_capacity_raw_s2_primary_idx          mz_arrangement_batcher_capacity_raw          mz_catalog_server    {operator_id,worker_id}                     ""
mz_arrangement_batcher_records_raw_s2_primary_idx           mz_arrangement_batcher_records_raw           mz_catalog_server    {operator_id,worker_id}                     ""
mz_arrangement_batcher_size_raw_s2_primary_idx              mz_arrangement_batcher_size_raw              mz_catalog_server    {operator_id,worker_id}                     ""
mz_cluster_deployment_lineage_ind                           mz_cluster_deployment_lineage                mz_catalog_server    {cluster_id}                                ""
mz_cluster_replica_history_ind                              mz_cluster_replica_history                   mz_catalog_server    {dropped_at}                                ""
mz_cluster_replica_name_history_ind                         mz_cluster_replica_name_history              mz_catalog_server    {id}                                        ""
mz_cluster_replica_metrics_ind                              mz_cluster_replica_metrics                   mz_catalog_server    {replica_id}                                ""
mz_cluster_replica_metrics_history_ind                      mz_cluster_replica_metrics_history           mz_catalog_server    {replica_id}                                ""
mz_cluster_replica_sizes_ind                                mz_cluster_replica_sizes                     mz_catalog_server    {size}                                      ""
mz_cluster_replica_statuses_ind                             mz_cluster_replica_statuses                  mz_catalog_server    {replica_id}                                ""
mz_cluster_replica_status_history_ind                       mz_cluster_replica_status_history            mz_catalog_server    {replica_id}                                ""
mz_cluster_replicas_ind                                     mz_cluster_replicas                          mz_catalog_server    {id}                                        ""
mz_clusters_ind                                             mz_clusters                                  mz_catalog_server    {id}                                        ""
mz_columns_ind                                              mz_columns                                   mz_catalog_server    {name}                                      ""
mz_comments_ind                                             mz_comments                                  mz_catalog_server    {id}                                        ""
mz_compute_dependencies_ind                                 mz_compute_dependencies                      mz_catalog_server    {dependency_id}                             ""
mz_compute_dataflow_global_ids_per_worker_s2_primary_idx   mz_compute_dataflow_global_ids_per_worker     mz_catalog_server    {id,worker_id}                             ""
mz_compute_error_counts_raw_s2_primary_idx                  mz_compute_error_counts_raw                  mz_catalog_server    {export_id,worker_id}                       ""
mz_compute_exports_per_worker_s2_primary_idx                mz_compute_exports_per_worker                mz_catalog_server    {export_id,worker_id}                       ""
mz_compute_frontiers_per_worker_s2_primary_idx              mz_compute_frontiers_per_worker              mz_catalog_server    {export_id,worker_id}                       ""
mz_compute_hydration_times_per_worker_s2_primary_idx        mz_compute_hydration_times_per_worker        mz_catalog_server    {export_id,worker_id}                       ""
mz_compute_import_frontiers_per_worker_s2_primary_idx       mz_compute_import_frontiers_per_worker       mz_catalog_server    {export_id,import_id,worker_id}             ""
mz_compute_lir_mapping_per_worker_s2_primary_idx            mz_compute_lir_mapping_per_worker            mz_catalog_server    {global_id,lir_id,worker_id}                ""
mz_compute_operator_durations_histogram_raw_s2_primary_idx  mz_compute_operator_durations_histogram_raw  mz_catalog_server    {id,worker_id,duration_ns}                  ""
mz_connections_ind                                          mz_connections                               mz_catalog_server    {schema_id}                                 ""
mz_console_cluster_utilization_overview_ind                 mz_console_cluster_utilization_overview      mz_catalog_server    {cluster_id}                                ""
mz_continual_tasks_ind                                      mz_continual_tasks                           mz_catalog_server    {id}                                        ""
mz_databases_ind                                            mz_databases                                 mz_catalog_server    {name}                                      ""
mz_dataflow_addresses_per_worker_s2_primary_idx             mz_dataflow_addresses_per_worker             mz_catalog_server    {id,worker_id}                              ""
mz_dataflow_channels_per_worker_s2_primary_idx              mz_dataflow_channels_per_worker              mz_catalog_server    {id,worker_id}                              ""
mz_dataflow_operator_reachability_raw_s2_primary_idx        mz_dataflow_operator_reachability_raw        mz_catalog_server    {address,port,worker_id,update_type,time}   ""
mz_dataflow_operators_per_worker_s2_primary_idx             mz_dataflow_operators_per_worker             mz_catalog_server    {id,worker_id}                              ""
mz_dataflow_shutdown_durations_histogram_raw_s2_primary_idx mz_dataflow_shutdown_durations_histogram_raw mz_catalog_server    {worker_id,duration_ns}                     ""
mz_frontiers_ind                                            mz_frontiers                                 mz_catalog_server    {object_id}                                 ""
mz_indexes_ind                                              mz_indexes                                   mz_catalog_server    {id}                                        ""
mz_kafka_sources_ind                                        mz_kafka_sources                             mz_catalog_server    {id}                                        ""
mz_materialized_views_ind                                   mz_materialized_views                        mz_catalog_server    {id}                                        ""
mz_message_batch_counts_received_raw_s2_primary_idx         mz_message_batch_counts_received_raw         mz_catalog_server    {channel_id,from_worker_id,to_worker_id}    ""
mz_message_batch_counts_sent_raw_s2_primary_idx             mz_message_batch_counts_sent_raw             mz_catalog_server    {channel_id,from_worker_id,to_worker_id}    ""
mz_message_counts_received_raw_s2_primary_idx               mz_message_counts_received_raw               mz_catalog_server    {channel_id,from_worker_id,to_worker_id}    ""
mz_message_counts_sent_raw_s2_primary_idx                   mz_message_counts_sent_raw                   mz_catalog_server    {channel_id,from_worker_id,to_worker_id}    ""
mz_object_dependencies_ind                                  mz_object_dependencies                       mz_catalog_server    {object_id}                                 ""
mz_object_history_ind                                       mz_object_history                            mz_catalog_server    {id}                                        ""
mz_object_lifetimes_ind                                     mz_object_lifetimes                          mz_catalog_server    {id}                                        ""
mz_object_transitive_dependencies_ind                       mz_object_transitive_dependencies            mz_catalog_server    {object_id}                                 ""
mz_objects_ind                                              mz_objects                                   mz_catalog_server    {schema_id}                                 ""
mz_notices_ind                                              mz_notices                                   mz_catalog_server    {id}                                        ""
mz_peek_durations_histogram_raw_s2_primary_idx              mz_peek_durations_histogram_raw              mz_catalog_server    {worker_id,type,duration_ns}                ""
mz_recent_activity_log_thinned_ind                          mz_recent_activity_log_thinned               mz_catalog_server    {sql_hash}                                  ""
mz_recent_storage_usage_ind                                 mz_recent_storage_usage                      mz_catalog_server    {object_id}                                 ""
mz_roles_ind                                                mz_roles                                     mz_catalog_server    {id}                                        ""
mz_scheduling_elapsed_raw_s2_primary_idx                    mz_scheduling_elapsed_raw                    mz_catalog_server    {id,worker_id}                              ""
mz_scheduling_parks_histogram_raw_s2_primary_idx            mz_scheduling_parks_histogram_raw            mz_catalog_server    {worker_id,slept_for_ns,requested_ns}       ""
mz_schemas_ind                                              mz_schemas                                   mz_catalog_server    {database_id}                               ""
mz_secrets_ind                                              mz_secrets                                   mz_catalog_server    {name}                                      ""
mz_show_all_objects_ind                                     mz_show_all_objects                          mz_catalog_server    {schema_id}                                 ""
mz_show_cluster_replicas_ind                                mz_show_cluster_replicas                     mz_catalog_server    {cluster}                                   ""
mz_show_clusters_ind                                        mz_show_clusters                             mz_catalog_server    {name}                                      ""
mz_show_columns_ind                                         mz_show_columns                              mz_catalog_server    {id}                                        ""
mz_show_connections_ind                                     mz_show_connections                          mz_catalog_server    {schema_id}                                 ""
mz_show_databases_ind                                       mz_show_databases                            mz_catalog_server    {name}                                      ""
mz_show_indexes_ind                                         mz_show_indexes                              mz_catalog_server    {schema_id}                                 ""
mz_show_materialized_views_ind                              mz_show_materialized_views                   mz_catalog_server    {schema_id}                                 ""
mz_show_roles_ind                                           mz_show_roles                                mz_catalog_server    {name}                                      ""
mz_show_schemas_ind                                         mz_show_schemas                              mz_catalog_server    {database_id}                               ""
mz_show_secrets_ind                                         mz_show_secrets                              mz_catalog_server    {schema_id}                                 ""
mz_show_sinks_ind                                           mz_show_sinks                                mz_catalog_server    {schema_id}                                 ""
mz_show_sources_ind                                         mz_show_sources                              mz_catalog_server    {schema_id}                                 ""
mz_show_tables_ind                                          mz_show_tables                               mz_catalog_server    {schema_id}                                 ""
mz_show_types_ind                                           mz_show_types                                mz_catalog_server    {schema_id}                                 ""
mz_show_views_ind                                           mz_show_views                                mz_catalog_server    {schema_id}                                 ""
mz_sink_statistics_ind                                      mz_sink_statistics                           mz_catalog_server    {id}                                        ""
mz_sink_status_history_ind                                  mz_sink_status_history                       mz_catalog_server    {sink_id}                                   ""
mz_source_statistics_with_history_ind                       mz_source_statistics_with_history            mz_catalog_server    {id}                                        ""
mz_sink_statuses_ind                                        mz_sink_statuses                             mz_catalog_server    {id}                                        ""
mz_sinks_ind                                                mz_sinks                                     mz_catalog_server    {id}                                        ""
mz_source_statistics_ind                                    mz_source_statistics                         mz_catalog_server    {id}                                        ""
mz_source_status_history_ind                                mz_source_status_history                     mz_catalog_server    {source_id}                                 ""
mz_source_statuses_ind                                      mz_source_statuses                           mz_catalog_server    {id}                                        ""
mz_sources_ind                                              mz_sources                                   mz_catalog_server    {id}                                        ""
mz_tables_ind                                               mz_tables                                    mz_catalog_server    {schema_id}                                 ""
mz_types_ind                                                mz_types                                     mz_catalog_server    {schema_id}                                 ""
mz_recent_sql_text_ind                                      mz_recent_sql_text                           mz_catalog_server    {sql_hash}                                  ""
mz_views_ind                                                mz_views                                     mz_catalog_server    {schema_id}                                 ""
mz_wallclock_global_lag_recent_history_ind                  mz_wallclock_global_lag_recent_history       mz_catalog_server    {object_id}                                 ""
mz_webhook_sources_ind                                      mz_webhook_sources                           mz_catalog_server    {id}                                        ""
pg_attrdef_all_databases_ind                                pg_attrdef_all_databases                     mz_catalog_server    {oid,adrelid,adnum,adbin,adsrc}             ""
pg_attribute_all_databases_ind                              pg_attribute_all_databases                   mz_catalog_server    {attrelid,attname,atttypid,attlen,attnum,atttypmod,attnotnull,atthasdef,attidentity,attgenerated,attisdropped,attcollation,database_name,pg_type_database_name}    ""
pg_class_all_databases_ind                                  pg_class_all_databases                       mz_catalog_server    {relname}                                   ""
pg_description_all_databases_ind                            pg_description_all_databases                 mz_catalog_server    {objoid,classoid,objsubid,description,oid_database_name,class_database_name}    ""
pg_namespace_all_databases_ind                              pg_namespace_all_databases                   mz_catalog_server    {nspname}                                   ""
pg_type_all_databases_ind                                   pg_type_all_databases                        mz_catalog_server    {oid}                                       ""

>[version>13100] SHOW INDEXES IN CLUSTER mz_catalog_server
mz_active_peeks_per_worker_s2_primary_idx                   mz_active_peeks_per_worker                   mz_catalog_server    {id,worker_id}                              ""
mz_arrangement_batches_raw_s2_primary_idx                   mz_arrangement_batches_raw                   mz_catalog_server    {operator_id,worker_id}                     ""
mz_arrangement_records_raw_s2_primary_idx                   mz_arrangement_records_raw                   mz_catalog_server    {operator_id,worker_id}                     ""
mz_arrangement_sharing_raw_s2_primary_idx                   mz_arrangement_sharing_raw                   mz_catalog_server    {operator_id,worker_id}                     ""
mz_arrangement_heap_capacity_raw_s2_primary_idx             mz_arrangement_heap_capacity_raw             mz_catalog_server    {operator_id,worker_id}                     ""
mz_arrangement_heap_allocations_raw_s2_primary_idx          mz_arrangement_heap_allocations_raw          mz_catalog_server    {operator_id,worker_id}                     ""
mz_arrangement_heap_size_raw_s2_primary_idx                 mz_arrangement_heap_size_raw                 mz_catalog_server    {operator_id,worker_id}                     ""
mz_arrangement_batcher_allocations_raw_s2_primary_idx       mz_arrangement_batcher_allocations_raw       mz_catalog_server    {operator_id,worker_id}                     ""
mz_arrangement_batcher_capacity_raw_s2_primary_idx          mz_arrangement_batcher_capacity_raw          mz_catalog_server    {operator_id,worker_id}                     ""
mz_arrangement_batcher_records_raw_s2_primary_idx           mz_arrangement_batcher_records_raw           mz_catalog_server    {operator_id,worker_id}                     ""
mz_arrangement_batcher_size_raw_s2_primary_idx              mz_arrangement_batcher_size_raw              mz_catalog_server    {operator_id,worker_id}                     ""
mz_cluster_deployment_lineage_ind                           mz_cluster_deployment_lineage                mz_catalog_server    {cluster_id}                                ""
mz_cluster_replica_history_ind                              mz_cluster_replica_history                   mz_catalog_server    {dropped_at}                                ""
mz_cluster_replica_name_history_ind                         mz_cluster_replica_name_history              mz_catalog_server    {id}                                        ""
mz_cluster_replica_metrics_ind                              mz_cluster_replica_metrics                   mz_catalog_server    {replica_id}                                ""
mz_cluster_replica_metrics_history_ind                      mz_cluster_replica_metrics_history           mz_catalog_server    {replica_id}                                ""
mz_cluster_replica_sizes_ind                                mz_cluster_replica_sizes                     mz_catalog_server    {size}                                      ""
mz_cluster_replica_statuses_ind                             mz_cluster_replica_statuses                  mz_catalog_server    {replica_id}                                ""
mz_cluster_replica_status_history_ind                       mz_cluster_replica_status_history            mz_catalog_server    {replica_id}                                ""
mz_cluster_replicas_ind                                     mz_cluster_replicas                          mz_catalog_server    {id}                                        ""
mz_clusters_ind                                             mz_clusters                                  mz_catalog_server    {id}                                        ""
mz_columns_ind                                              mz_columns                                   mz_catalog_server    {name}                                      ""
mz_comments_ind                                             mz_comments                                  mz_catalog_server    {id}                                        ""
mz_compute_dependencies_ind                                 mz_compute_dependencies                      mz_catalog_server    {dependency_id}                             ""
mz_compute_dataflow_global_ids_per_worker_s2_primary_idx   mz_compute_dataflow_global_ids_per_worker     mz_catalog_server    {id,worker_id}                             ""
mz_compute_error_counts_raw_s2_primary_idx                  mz_compute_error_counts_raw                  mz_catalog_server    {export_id,worker_id}                       ""
mz_compute_exports_per_worker_s2_primary_idx                mz_compute_exports_per_worker                mz_catalog_server    {export_id,worker_id}                       ""
mz_compute_frontiers_per_worker_s2_primary_idx              mz_compute_frontiers_per_worker              mz_catalog_server    {export_id,worker_id}                       ""
mz_compute_hydration_times_per_worker_s2_primary_idx        mz_compute_hydration_times_per_worker        mz_catalog_server    {export_id,worker_id}                       ""
mz_compute_import_frontiers_per_worker_s2_primary_idx       mz_compute_import_frontiers_per_worker       mz_catalog_server    {export_id,import_id,worker_id}             ""
mz_compute_lir_mapping_per_worker_s2_primary_idx            mz_compute_lir_mapping_per_worker            mz_catalog_server    {global_id,lir_id,worker_id}                ""
mz_compute_operator_durations_histogram_raw_s2_primary_idx  mz_compute_operator_durations_histogram_raw  mz_catalog_server    {id,worker_id,duration_ns}                  ""
mz_connections_ind                                          mz_connections                               mz_catalog_server    {schema_id}                                 ""
mz_console_cluster_utilization_overview_ind                 mz_console_cluster_utilization_overview      mz_catalog_server    {cluster_id}                                ""
mz_continual_tasks_ind                                      mz_continual_tasks                           mz_catalog_server    {id}                                        ""
mz_databases_ind                                            mz_databases                                 mz_catalog_server    {name}                                      ""
mz_dataflow_addresses_per_worker_s2_primary_idx             mz_dataflow_addresses_per_worker             mz_catalog_server    {id,worker_id}                              ""
mz_dataflow_channels_per_worker_s2_primary_idx              mz_dataflow_channels_per_worker              mz_catalog_server    {id,worker_id}                              ""
mz_dataflow_operator_reachability_raw_s2_primary_idx        mz_dataflow_operator_reachability_raw        mz_catalog_server    {id,worker_id,source,port,update_type,time} ""
mz_dataflow_operators_per_worker_s2_primary_idx             mz_dataflow_operators_per_worker             mz_catalog_server    {id,worker_id}                              ""
mz_dataflow_shutdown_durations_histogram_raw_s2_primary_idx mz_dataflow_shutdown_durations_histogram_raw mz_catalog_server    {worker_id,duration_ns}                     ""
mz_frontiers_ind                                            mz_frontiers                                 mz_catalog_server    {object_id}                                 ""
mz_indexes_ind                                              mz_indexes                                   mz_catalog_server    {id}                                        ""
mz_kafka_sources_ind                                        mz_kafka_sources                             mz_catalog_server    {id}                                        ""
mz_materialized_views_ind                                   mz_materialized_views                        mz_catalog_server    {id}                                        ""
mz_message_batch_counts_received_raw_s2_primary_idx         mz_message_batch_counts_received_raw         mz_catalog_server    {channel_id,from_worker_id,to_worker_id}    ""
mz_message_batch_counts_sent_raw_s2_primary_idx             mz_message_batch_counts_sent_raw             mz_catalog_server    {channel_id,from_worker_id,to_worker_id}    ""
mz_message_counts_received_raw_s2_primary_idx               mz_message_counts_received_raw               mz_catalog_server    {channel_id,from_worker_id,to_worker_id}    ""
mz_message_counts_sent_raw_s2_primary_idx                   mz_message_counts_sent_raw                   mz_catalog_server    {channel_id,from_worker_id,to_worker_id}    ""
mz_object_dependencies_ind                                  mz_object_dependencies                       mz_catalog_server    {object_id}                                 ""
mz_object_history_ind                                       mz_object_history                            mz_catalog_server    {id}                                        ""
mz_object_lifetimes_ind                                     mz_object_lifetimes                          mz_catalog_server    {id}                                        ""
mz_object_transitive_dependencies_ind                       mz_object_transitive_dependencies            mz_catalog_server    {object_id}                                 ""
mz_objects_ind                                              mz_objects                                   mz_catalog_server    {schema_id}                                 ""
mz_notices_ind                                              mz_notices                                   mz_catalog_server    {id}                                        ""
mz_peek_durations_histogram_raw_s2_primary_idx              mz_peek_durations_histogram_raw              mz_catalog_server    {worker_id,type,duration_ns}                ""
mz_recent_activity_log_thinned_ind                          mz_recent_activity_log_thinned               mz_catalog_server    {sql_hash}                                  ""
mz_recent_storage_usage_ind                                 mz_recent_storage_usage                      mz_catalog_server    {object_id}                                 ""
mz_roles_ind                                                mz_roles                                     mz_catalog_server    {id}                                        ""
mz_scheduling_elapsed_raw_s2_primary_idx                    mz_scheduling_elapsed_raw                    mz_catalog_server    {id,worker_id}                              ""
mz_scheduling_parks_histogram_raw_s2_primary_idx            mz_scheduling_parks_histogram_raw            mz_catalog_server    {worker_id,slept_for_ns,requested_ns}       ""
mz_schemas_ind                                              mz_schemas                                   mz_catalog_server    {database_id}                               ""
mz_secrets_ind                                              mz_secrets                                   mz_catalog_server    {name}                                      ""
mz_show_all_objects_ind                                     mz_show_all_objects                          mz_catalog_server    {schema_id}                                 ""
mz_show_cluster_replicas_ind                                mz_show_cluster_replicas                     mz_catalog_server    {cluster}                                   ""
mz_show_clusters_ind                                        mz_show_clusters                             mz_catalog_server    {name}                                      ""
mz_show_columns_ind                                         mz_show_columns                              mz_catalog_server    {id}                                        ""
mz_show_connections_ind                                     mz_show_connections                          mz_catalog_server    {schema_id}                                 ""
mz_show_databases_ind                                       mz_show_databases                            mz_catalog_server    {name}                                      ""
mz_show_indexes_ind                                         mz_show_indexes                              mz_catalog_server    {schema_id}                                 ""
mz_show_materialized_views_ind                              mz_show_materialized_views                   mz_catalog_server    {schema_id}                                 ""
mz_show_roles_ind                                           mz_show_roles                                mz_catalog_server    {name}                                      ""
mz_show_schemas_ind                                         mz_show_schemas                              mz_catalog_server    {database_id}                               ""
mz_show_secrets_ind                                         mz_show_secrets                              mz_catalog_server    {schema_id}                                 ""
mz_show_sinks_ind                                           mz_show_sinks                                mz_catalog_server    {schema_id}                                 ""
mz_show_sources_ind                                         mz_show_sources                              mz_catalog_server    {schema_id}                                 ""
mz_show_tables_ind                                          mz_show_tables                               mz_catalog_server    {schema_id}                                 ""
mz_show_types_ind                                           mz_show_types                                mz_catalog_server    {schema_id}                                 ""
mz_show_views_ind                                           mz_show_views                                mz_catalog_server    {schema_id}                                 ""
mz_sink_statistics_ind                                      mz_sink_statistics                           mz_catalog_server    {id}                                        ""
mz_sink_status_history_ind                                  mz_sink_status_history                       mz_catalog_server    {sink_id}                                   ""
mz_source_statistics_with_history_ind                       mz_source_statistics_with_history            mz_catalog_server    {id}                                        ""
mz_sink_statuses_ind                                        mz_sink_statuses                             mz_catalog_server    {id}                                        ""
mz_sinks_ind                                                mz_sinks                                     mz_catalog_server    {id}                                        ""
mz_source_statistics_ind                                    mz_source_statistics                         mz_catalog_server    {id}                                        ""
mz_source_status_history_ind                                mz_source_status_history                     mz_catalog_server    {source_id}                                 ""
mz_source_statuses_ind                                      mz_source_statuses                           mz_catalog_server    {id}                                        ""
mz_sources_ind                                              mz_sources                                   mz_catalog_server    {id}                                        ""
mz_tables_ind                                               mz_tables                                    mz_catalog_server    {schema_id}                                 ""
mz_types_ind                                                mz_types                                     mz_catalog_server    {schema_id}                                 ""
mz_recent_sql_text_ind                                      mz_recent_sql_text                           mz_catalog_server    {sql_hash}                                  ""
mz_views_ind                                                mz_views                                     mz_catalog_server    {schema_id}                                 ""
mz_wallclock_global_lag_recent_history_ind                  mz_wallclock_global_lag_recent_history       mz_catalog_server    {object_id}                                 ""
mz_webhook_sources_ind                                      mz_webhook_sources                           mz_catalog_server    {id}                                        ""
pg_attrdef_all_databases_ind                                pg_attrdef_all_databases                     mz_catalog_server    {oid,adrelid,adnum,adbin,adsrc}             ""
pg_attribute_all_databases_ind                              pg_attribute_all_databases                   mz_catalog_server    {attrelid,attname,atttypid,attlen,attnum,atttypmod,attnotnull,atthasdef,attidentity,attgenerated,attisdropped,attcollation,database_name,pg_type_database_name}    ""
pg_class_all_databases_ind                                  pg_class_all_databases                       mz_catalog_server    {relname}                                   ""
pg_description_all_databases_ind                            pg_description_all_databases                 mz_catalog_server    {objoid,classoid,objsubid,description,oid_database_name,class_database_name}    ""
pg_namespace_all_databases_ind                              pg_namespace_all_databases                   mz_catalog_server    {nspname}                                   ""
pg_type_all_databases_ind                                   pg_type_all_databases                        mz_catalog_server    {oid}                                       ""
