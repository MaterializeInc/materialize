// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Dumps catalog information to files.

use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use mz_tls_util::make_tls;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio_postgres::{Client as PgClient, Config as PgConfig};
use tokio_util::io::StreamReader;

use k8s_openapi::api::core::v1::Service;
use kube::{Api, Client};
use mz_ore::retry::{self, RetryResult};
use mz_ore::task::{self, JoinHandle};
use tracing::{error, info};

use crate::utils::format_base_path;
use crate::{Args, Context};

#[derive(Debug, Clone)]
pub struct SqlPortForwardingInfo {
    pub namespace: String,
    pub service_name: String,
    pub target_port: i32,
    pub local_port: i32,
}

pub async fn get_sql_port_forwarding_info(
    client: &Client,
    args: &Args,
) -> Result<SqlPortForwardingInfo, anyhow::Error> {
    for namespace in &args.k8s_namespaces {
        let services: Api<Service> = Api::namespaced(client.clone(), namespace);

        // If override for target service and port is provided, verify that the
        // service exists and has the port.
        let maybe_service_and_port_override = match (&args.sql_target_service, args.sql_target_port)
        {
            (Some(service), Some(port)) => Some((service, port)),
            _ => None,
        };

        if let Some((service_override, port_override)) = maybe_service_and_port_override {
            let service = services.get(service_override).await?;
            if let Some(spec) = service.spec {
                let contains_port = spec
                    .ports
                    .unwrap_or_default()
                    .iter()
                    .any(|port_info| port_info.port == port_override);

                if contains_port {
                    return Ok(SqlPortForwardingInfo {
                        namespace: namespace.clone(),
                        service_name: service_override.clone(),
                        target_port: port_override,
                        local_port: args.sql_local_port.unwrap_or(port_override),
                    });
                }
            }

            return Err(anyhow::anyhow!(
                "Service {} with port {} not found",
                service_override,
                port_override
            ));
        }
        let services = services.list(&Default::default()).await?;
        // Check if any service contains a port with name "internal-sql"
        let maybe_port_info = services
            .iter()
            .filter_map(|service| {
                let spec = service.spec.as_ref()?;
                let service_name = service.metadata.name.as_ref()?;
                Some((spec, service_name))
            })
            .flat_map(|(spec, service_name)| {
                spec.ports
                    .iter()
                    .flatten()
                    .map(move |port| (port, service_name))
            })
            .find_map(|(port_info, service_name)| {
                if let Some(port_name) = &port_info.name {
                    if port_name.to_lowercase().contains("internal")
                        && port_name.to_lowercase().contains("sql")
                    {
                        return Some(SqlPortForwardingInfo {
                            namespace: namespace.clone(),
                            service_name: service_name.to_owned(),
                            target_port: port_info.port,
                            local_port: args.sql_local_port.unwrap_or(port_info.port),
                        });
                    }
                }

                None
            });

        if let Some(port_info) = maybe_port_info {
            return Ok(port_info);
        }
    }

    Err(anyhow::anyhow!("No SQL port forwarding info found"))
}

/// Spawns a port forwarding process for the given k8s service.
/// The process will retry if the port-forwarding fails and
/// will terminate once the port forwarding reaches the max number of retries.
/// We retry since kubectl port-forward is flaky.
pub fn spawn_sql_port_forwarding_process(
    port_forwarding_info: &SqlPortForwardingInfo,
    args: &Args,
) -> JoinHandle<()> {
    let port_forwarding_info = port_forwarding_info.clone();

    let k8s_context = args.k8s_context.clone();
    let local_address = args.sql_local_address.clone();

    task::spawn(|| "port-forwarding", async move {
        if let Err(err) = retry::Retry::default()
            .max_duration(Duration::from_secs(60))
            .retry_async(|retry_state| {
                let k8s_context = k8s_context.clone();
                let namespace = port_forwarding_info.namespace.clone();
                let service_name = port_forwarding_info.service_name.clone();
                let local_address = local_address.clone();
                let local_port = port_forwarding_info.local_port;
                let target_port = port_forwarding_info.target_port;
                let local_address_or_default =
                    local_address.clone().unwrap_or("localhost".to_string());

                info!(
                    "Spawning port forwarding process for {} from ports {}:{} -> {}",
                    service_name, local_address_or_default, local_port, target_port
                );

                async move {
                    let port_arg_str = format!("{}:{}", &local_port, &target_port);
                    let service_name_arg_str = format!("services/{}", &service_name);
                    let mut args = vec![
                        "port-forward",
                        &service_name_arg_str,
                        &port_arg_str,
                        "-n",
                        &namespace,
                    ];

                    if let Some(k8s_context) = &k8s_context {
                        args.extend(["--context", k8s_context]);
                    }

                    if let Some(local_address) = &local_address {
                        args.extend(["--address", local_address]);
                    }

                    match tokio::process::Command::new("kubectl")
                        .args(args)
                        // Silence stdout/stderr
                        .stdout(std::process::Stdio::null())
                        .stderr(std::process::Stdio::null())
                        .kill_on_drop(true)
                        .output()
                        .await
                    {
                        Ok(output) => {
                            if !output.status.success() {
                                let retry_err_msg = format!(
                                    "Failed to port-forward{}: {}",
                                    retry_state.next_backoff.map_or_else(
                                        || "".to_string(),
                                        |d| format!(", retrying in {:?}", d)
                                    ),
                                    String::from_utf8_lossy(&output.stderr)
                                );
                                error!("{}", retry_err_msg);

                                return RetryResult::RetryableErr(anyhow::anyhow!(retry_err_msg));
                            }
                        }
                        Err(err) => {
                            return RetryResult::RetryableErr(anyhow::anyhow!(
                                "Failed to port-forward: {}",
                                err
                            ));
                        }
                    }
                    // The kubectl subprocess's future will only resolve on error, thus the
                    // code here is unreachable. We return RetryResult::Ok to satisfy
                    // the type checker.
                    RetryResult::Ok(())
                }
            })
            .await
        {
            error!("{}", err);
        }
    })
}

#[derive(Debug, Clone)]
pub enum RelationCategory {
    /// For relations that belong in the `mz_introspection` schema.
    /// These relations require a replica name to be specified.
    Introspection,
    /// For relations that are retained metric objects that we'd also like to get the SUBSCRIBE output for.
    Retained,
    /// Other relations that we want to do a SELECT * FROM on.
    Basic,
}

#[derive(Debug, Clone)]
pub struct Relation {
    pub name: &'static str,
    pub category: RelationCategory,
}

const RELATIONS: &[Relation] = &[
    // Basic object information
    Relation {
        name: "mz_audit_events",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_databases",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_schemas",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_tables",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_sources",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_sinks",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_views",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_materialized_views",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_secrets",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_connections",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_roles",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_subscriptions",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_object_fully_qualified_names",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_sessions",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_object_history",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_object_lifetimes",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_object_dependencies",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_object_transitive_dependencies",
        category: RelationCategory::Basic,
    },
    // Compute
    Relation {
        name: "mz_clusters",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_indexes",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_cluster_replicas",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_cluster_replica_sizes",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_cluster_replica_statuses",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_cluster_replica_metrics_history",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_compute_hydration_times",
        category: RelationCategory::Retained,
    },
    Relation {
        name: "mz_materialization_dependencies",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_cluster_replica_status_history",
        category: RelationCategory::Basic,
    },
    // Freshness
    Relation {
        name: "mz_wallclock_global_lag_recent_history",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_global_frontiers",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_cluster_replica_frontiers",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_materialization_lag",
        category: RelationCategory::Basic,
    },
    // Sources/sinks
    Relation {
        name: "mz_source_statistics_with_history",
        category: RelationCategory::Retained,
    },
    Relation {
        name: "mz_sink_statistics",
        category: RelationCategory::Retained,
    },
    Relation {
        name: "mz_source_statuses",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_sink_statuses",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_source_status_history",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_sink_status_history",
        category: RelationCategory::Basic,
    },
    // Refresh every information
    Relation {
        name: "mz_materialized_view_refresh_strategies",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_cluster_schedules",
        category: RelationCategory::Basic,
    },
    // Persist
    Relation {
        name: "mz_recent_storage_usage",
        category: RelationCategory::Basic,
    },
    // Introspection relations
    Relation {
        name: "mz_arrangement_sharing_per_worker",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_arrangement_sharing",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_arrangement_sizes_per_worker",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_dataflow_channels",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_dataflow_operators",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_dataflow_global_ids",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_dataflow_operator_dataflows_per_worker",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_dataflow_operator_dataflows",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_dataflow_operator_parents_per_worker",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_dataflow_operator_parents",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_compute_exports",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_dataflow_arrangement_sizes",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_expected_group_size_advice",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_compute_frontiers",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_dataflow_channel_operators_per_worker",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_dataflow_channel_operators",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_compute_import_frontiers",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_message_counts_per_worker",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_message_counts",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_active_peeks",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_compute_operator_durations_histogram_per_worker",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_compute_operator_durations_histogram",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_records_per_dataflow_operator_per_worker",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_records_per_dataflow_operator",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_records_per_dataflow_per_worker",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_records_per_dataflow",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_peek_durations_histogram_per_worker",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_peek_durations_histogram",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_dataflow_shutdown_durations_histogram_per_worker",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_dataflow_shutdown_durations_histogram",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_scheduling_elapsed_per_worker",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_scheduling_elapsed",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_scheduling_parks_histogram_per_worker",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_scheduling_parks_histogram",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_compute_lir_mapping_per_worker",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_lir_mapping",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_compute_error_counts",
        category: RelationCategory::Introspection,
    },
    Relation {
        name: "mz_compute_error_counts_per_worker",
        category: RelationCategory::Introspection,
    },
    // Relations that are redundant with some of the above, but
    // are used by the Console.
    Relation {
        name: "mz_cluster_replica_metrics_history",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_webhook_sources",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_cluster_replica_history",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_source_statistics",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_cluster_deployment_lineage",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_show_indexes",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_relations",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_frontiers",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_console_cluster_utilization_overview",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_columns",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_kafka_sources",
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_kafka_sinks",
        category: RelationCategory::Basic,
    },
];

const PG_CONNECTION_TIMEOUT: Duration = Duration::from_secs(60);
// Timeout for a query. We use 6 minutes since it's a good 
// sign that the operation won't work.
const PG_QUERY_TIMEOUT: Duration = Duration::from_secs(60 * 6);

#[derive(Debug, Clone)]
pub struct ClusterReplica {
    pub cluster_name: String,
    pub replica_name: String,
}

pub struct SystemCatalogDumper<'n> {
    context: &'n Context,
    pg_client: Arc<Mutex<PgClient>>,
    cluster_replicas: Vec<ClusterReplica>,
    _cluster_replicas: Vec<ClusterReplica>,
    _pg_conn_handle: JoinHandle<Result<(), tokio_postgres::Error>>,
}

impl<'n> SystemCatalogDumper<'n> {
    pub async fn new(context: &'n Context, connection_string: &str) -> Result<Self, anyhow::Error> {
        let (pg_client, pg_conn) = retry::Retry::default()
            .max_duration(PG_CONNECTION_TIMEOUT)
            .retry_async_canceling(|_| async move {
                let pg_config = &mut PgConfig::from_str(connection_string)?;
                pg_config.connect_timeout(PG_CONNECTION_TIMEOUT);
                let tls = make_tls(pg_config)?;
                pg_config.connect(tls).await.map_err(|e| anyhow::anyhow!(e))
            })
            .await?;

        info!("Connected to PostgreSQL server at {}...", connection_string);

        let handle = task::spawn(|| "postgres-connection", pg_conn);

        // Set search path to system catalog tables
        pg_client
            .execute(
                "SET search_path = mz_internal, mz_catalog, mz_introspection",
                &[],
            )
            .await?;

        // We need to get all cluster replicas to dump introspection relations.
        let cluster_replicas = match pg_client
            .query("SELECT c.name as cluster_name, cr.name as replica_name FROM mz_clusters AS c JOIN mz_cluster_replicas AS cr ON c.id = cr.cluster_id;", &[])
            .await
            {
                Ok(rows) => rows.into_iter()
                            .map(|row| ClusterReplica {
                                cluster_name: row.get::<_, String>("cluster_name"),
                                replica_name: row.get::<_, String>("replica_name"),
                            })
                            .collect::<Vec<_>>(),
            Err(e) => {
                error!("Failed to get replica names: {}", e);
                vec![]
            }
        };

        Ok(Self {
            context,
            pg_client: Arc::new(Mutex::new(pg_client)),
            cluster_replicas,
            _pg_conn_handle: handle,
        })
    }

    pub fn dump_relation(
        &self,
        relation: &Relation,
        cluster_replica: Option<&ClusterReplica>,
    ) -> JoinHandle<()> {
        let start_time = self.context.start_time.clone();
        let pg_client = Arc::clone(&self.pg_client);
        let cluster_replica = cluster_replica.cloned();
        let relation_name = relation.name.to_string();
        let relation_category = relation.category.clone();

        task::spawn(|| "dump-relation", async move {
            if let Err(err) = retry::Retry::default()
                .max_duration(PG_QUERY_TIMEOUT)
                .initial_backoff(Duration::from_secs(2))
                .retry_async_canceling(|_| {
                    let start_time = start_time.clone();
                    let pg_client = Arc::clone(&pg_client);
                    let relation_name = relation_name.clone();
                    let cluster_replica = cluster_replica.clone();
                    let relation_category = relation_category.clone();


                    async move {
                        match async {
                            // TODO (debug_tool3): Use a transaction for the entire dump instead of per query.
                            let mut pg_client_lock = pg_client.lock().await;
                            let transaction = pg_client_lock.transaction().await?;
                            
                            // Some queries (i.e. mz_introspection relations) require the cluster and replica to be set.
                            if let Some(cluster_replica) = &cluster_replica {
                                transaction
                                    .execute(
                                        &format!(
                                            "SET LOCAL CLUSTER = '{}'",
                                            cluster_replica.cluster_name
                                        ),
                                        &[],
                                    )
                                    .await?;
                                transaction
                                    .execute(
                                        &format!(
                                            "SET LOCAL CLUSTER_REPLICA = '{}'",
                                            cluster_replica.replica_name
                                        ),
                                        &[],
                                    )
                                    .await?;
                            }

                        
                            // We query the column names to write the header row of the CSV file.
                            // TODO (SangJunBak): Use `WITH (HEADER TRUE)` once database-issues#2846 is implemented.
                            let column_names = transaction
                            .query(&format!("SHOW COLUMNS FROM {}", &relation_name), &[])
                            .await?
                            .into_iter()
                            .map(|row| row.get::<_, String>("name"))
                            .collect::<Vec<_>>();

                            let copy_to_csv = |file_path_name: PathBuf, relation_category: RelationCategory| {
                                let relation_name = relation_name.clone();
                                let mut column_names = column_names.clone();

                                if let RelationCategory::Introspection = relation_category {
                                    column_names.splice(0..0, vec!["mz_timestamp".to_string(), "mz_diff".to_string()]);
                                }

                                async move {
                                    let mut file = tokio::fs::File::create(&file_path_name).await?;

                                    file.write_all((column_names.join(",") + "\n").as_bytes())
                                        .await?;

                                    // Stream data rows to CSV
                                    let copy_query = format!(
                                        "COPY (SELECT * FROM {}) TO STDOUT WITH (FORMAT CSV)",
                                        relation_name
                                    );
                                    
                                    let copy_stream = transaction.copy_out(&copy_query).await?
                                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
                                    let copy_stream = std::pin::pin!(copy_stream);
                                    let mut reader = StreamReader::new(copy_stream);
                                    tokio::io::copy(&mut reader, &mut file).await?;

                                    info!(
                                        "Copied {} to {}",
                                        relation_name,
                                        file_path_name.display()
                                    );
                                    Ok::<(), anyhow::Error>(())
                                }
                            };

                            match relation_category {
                                RelationCategory::Basic => {
                                    let file_path = format_file_path(start_time, None);
                                    let file_path_name = file_path
                                        .join(relation_name.as_str())
                                        .with_extension("csv");
                                    tokio::fs::create_dir_all(&file_path).await?;

                                    copy_to_csv(file_path_name, relation_category).await?;
                                }
                                RelationCategory::Introspection => {
                                    let file_path =
                                        format_file_path(start_time, cluster_replica.as_ref());
                                    tokio::fs::create_dir_all(&file_path).await?;

                                    let file_path_name = file_path
                                        .join(relation_name.as_str())
                                        .with_extension("csv");

                                    copy_to_csv(file_path_name, relation_category).await?;
                                }
                                RelationCategory::Retained => {
                                    let file_path = format_file_path(start_time, None);
                                    let file_path_name = file_path
                                        .join(relation_name.as_str())
                                        .with_extension("csv");
                                    tokio::fs::create_dir_all(&file_path).await?;
                                    copy_to_csv(file_path_name, relation_category).await?;
                                    // TODO (debug_tool1): Dump the `FETCH ALL SUBSCRIBE` output too
                                }
                            }
                            Ok::<(), anyhow::Error>(())
                        }
                        .await
                        {
                            Ok(()) => Ok(()),
                            Err(err) => {
                                error!(
                                    "{}: {}. Retrying...",
                                    format_catalog_dump_error_message(
                                        &relation_name,
                                        cluster_replica.as_ref()
                                    ),
                                    err
                                );
                                Err(err)
                            }
                        }
                    }
                })
                .await
            {
                error!(
                    "{}: {}. Consider increasing the size of your mz_catalog_server cluster \
                    https://materialize.com/docs/self-managed/v25.1/installation/troubleshooting/#troubleshooting-console-unresponsiveness",
                    format_catalog_dump_error_message(&relation_name, cluster_replica.as_ref()),
                    err,
                    
                );
            }
        })
    }

    pub fn dump_all_relations(&self) -> Vec<JoinHandle<()>> {
        RELATIONS
            .iter()
            .map(|relation| match relation.category {
                RelationCategory::Introspection => self
                    .cluster_replicas
                    .iter()
                    .map(|cluster_replica| self.dump_relation(relation, Some(cluster_replica)))
                    .collect(),
                _ => vec![self.dump_relation(relation, None)],
            })
            .flatten()
            .collect()
    }
}

fn format_catalog_dump_error_message(
    relation_name: &String,
    cluster_replica: Option<&ClusterReplica>,
) -> String {
    format!(
        "Failed to dump relation {}{}",
        relation_name,
        cluster_replica.map_or_else(
            || "".to_string(),
            |replica| format!(" for {}.{}", replica.cluster_name, replica.replica_name)
        )
    )
}

fn format_file_path(date_time: DateTime<Utc>, cluster_replica: Option<&ClusterReplica>) -> PathBuf {
    let path = format_base_path(date_time).join("system-catalog");
    if let Some(cluster_replica) = cluster_replica {
        path.join(cluster_replica.cluster_name.as_str())
            .join(cluster_replica.replica_name.as_str())
    } else {
        path
    }
}

pub fn create_postgres_connection_string(
    host_address: Option<&str>,
    host_port: Option<i32>,
    target_port: Option<i32>,
) -> String {
    let host_address = host_address.unwrap_or("localhost");
    let host_port = host_port.unwrap_or(6877);
    let user = match target_port {
        // We assume that if the target port is 6877, we are connecting to the
        // internal SQL port.
        Some(6877) => "mz_system",
        _ => "materialize",
    };

    format!(
        "postgres://{}:materialize@{}:{}",
        user, host_address, host_port
    )
}
