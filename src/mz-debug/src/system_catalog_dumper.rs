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
//! We run queries in serial rather than parallel because all queries in the pgwire
//! connection are run in serial anyways. Running the queries in serial also makes
//! cleaning up / aborting queries much easier.

use anyhow::{Context as _, Result};
use csv_async::AsyncSerializer;
use futures::TryStreamExt;
use mz_tls_util::make_tls;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio_postgres::{
    Client as PgClient, Config as PgConfig, Connection, SimpleQueryMessage, Socket, Transaction,
};
use tokio_util::io::StreamReader;

use mz_ore::collections::HashMap;
use mz_ore::retry::{self};
use mz_ore::task::{self, JoinHandle};
use postgres_openssl::{MakeTlsConnector, TlsStream};
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub enum RelationCategory {
    /// For relations that belong in the `mz_introspection` schema.
    /// These relations require a replica name to be specified.
    Introspection,
    /// For relations that are retained metric objects that we'd like to get the SUBSCRIBE output for.
    Retained,
    /// Other relations that we want to do a SELECT * FROM on.
    Basic,
}

#[derive(Debug, Clone)]
pub struct Relation {
    pub name: &'static str,
    pub category: RelationCategory,
}

static SYSTEM_CATALOG_DUMP_DIR: &str = "system_catalog";
/// This list is used to determine which relations to dump.
/// The relations are grouped and delimited by their category (i.e. Basic object information)
static RELATIONS: &[Relation] = &[
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
        category: RelationCategory::Basic,
    },
    Relation {
        name: "mz_source_statistics_with_history",
        category: RelationCategory::Retained,
    },
    Relation {
        name: "mz_sink_statistics",
        category: RelationCategory::Basic,
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

static PG_CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);
/// Timeout for a query.
// TODO (debug_tool3): Make this configurable.
static PG_QUERY_TIMEOUT: Duration = Duration::from_secs(20);

/// The maximum number of errors we tolerate for a cluster replica.
/// If a cluster replica has more than this many errors, we skip it.
static MAX_CLUSTER_REPLICA_ERROR_COUNT: usize = 3;

static SET_SEARCH_PATH_QUERY: &str = "SET search_path = mz_internal, mz_catalog, mz_introspection";
static SELECT_CLUSTER_REPLICAS_QUERY: &str = "SELECT c.name as cluster_name, cr.name as replica_name FROM mz_clusters AS c JOIN mz_cluster_replicas AS cr ON c.id = cr.cluster_id;";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClusterReplica {
    pub cluster_name: String,
    pub replica_name: String,
}

impl Default for ClusterReplica {
    fn default() -> Self {
        Self {
            cluster_name: "mz_catalog_server".to_string(),
            replica_name: "r1".to_string(),
        }
    }
}

impl fmt::Display for ClusterReplica {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.cluster_name, self.replica_name)
    }
}

pub struct SystemCatalogDumper {
    base_path: PathBuf,
    pg_client: Arc<Mutex<PgClient>>,
    pg_tls: MakeTlsConnector,
    cluster_replicas: Vec<ClusterReplica>,
    _pg_conn_handle: JoinHandle<Result<(), tokio_postgres::Error>>,
}

pub async fn create_postgres_connection(
    connection_string: &str,
) -> Result<
    (
        PgClient,
        Connection<Socket, TlsStream<Socket>>,
        MakeTlsConnector,
    ),
    anyhow::Error,
> {
    let mut pg_config = PgConfig::from_str(connection_string)?;
    pg_config.connect_timeout(PG_CONNECTION_TIMEOUT);
    let tls = make_tls(&pg_config)?;
    info!(
        "Connecting to PostgreSQL server at {}...",
        connection_string
    );
    let (pg_client, pg_conn) = retry::Retry::default()
        .max_duration(PG_CONNECTION_TIMEOUT)
        .retry_async_canceling(|_| {
            let pg_config = pg_config.clone();
            let tls = tls.clone();
            async move { pg_config.connect(tls).await.map_err(|e| anyhow::anyhow!(e)) }
        })
        .await?;

    Ok((pg_client, pg_conn, tls))
}

pub async fn write_copy_stream(
    transaction: &Transaction<'_>,
    copy_query: &str,
    file: &mut tokio::fs::File,
    relation_name: &str,
) -> Result<(), anyhow::Error> {
    let copy_stream = transaction
        .copy_out(copy_query)
        .await
        .context(format!("Failed to COPY TO for {}", relation_name))?
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
    let copy_stream = std::pin::pin!(copy_stream);
    let mut reader = StreamReader::new(copy_stream);
    tokio::io::copy(&mut reader, file).await?;
    // Ensure the file is flushed to disk.
    file.sync_all().await?;

    Ok::<(), anyhow::Error>(())
}

pub async fn copy_relation_to_csv(
    transaction: &Transaction<'_>,
    file_path_name: PathBuf,
    column_names: &Vec<String>,
    relation: &Relation,
) -> Result<(), anyhow::Error> {
    let mut file = tokio::fs::File::create(&file_path_name).await?;

    match relation.category {
        RelationCategory::Retained => {
            let mut writer = AsyncSerializer::from_writer(file);
            writer.serialize(column_names).await?;

            transaction
                .execute(
                    &format!("DECLARE c CURSOR FOR SUBSCRIBE TO {}", relation.name),
                    &[],
                )
                .await
                .context("Failed to declare cursor")?;

            // We need to use simple_query, otherwise tokio-postgres will run an introspection SELECT query to figure out the types since it'll
            // try to prepare the query. This causes an error since SUBSCRIBEs and SELECT queries are not allowed to be executed in the same transaction.
            // Thus we use simple_query to avoid the introspection query.
            let rows = transaction
                // We use a timeout of '1' to receive the snapshot of the current state. A timeout of '0' will return no results.
                // We also don't care if we get more than just the snapshot.
                .simple_query("FETCH ALL FROM c WITH (TIMEOUT '1')")
                .await
                .context("Failed to fetch all from cursor")?;

            for row in rows {
                if let SimpleQueryMessage::Row(row) = row {
                    let values: Vec<&str> = (0..row.len())
                        .map(|i| row.get(i).unwrap_or("")) // Convert each field to String
                        .collect();
                    writer.serialize(&values).await?;
                }
            }
        }
        _ => {
            // TODO (SangJunBak): Use `WITH (HEADER TRUE)` once database-issues#2846 is implemented.
            file.write_all((column_names.join(",") + "\n").as_bytes())
                .await?;
            let copy_query = format!(
                "COPY (SELECT * FROM {}) TO STDOUT WITH (FORMAT CSV)",
                relation.name
            );
            write_copy_stream(transaction, &copy_query, &mut file, relation.name).await?;
        }
    };

    info!("Copied {} to {}", relation.name, file_path_name.display());
    Ok::<(), anyhow::Error>(())
}

pub async fn query_column_names(
    pg_client: &PgClient,
    relation: &Relation,
) -> Result<Vec<String>, anyhow::Error> {
    let relation_name = relation.name;
    // We query the column names to write the header row of the CSV file.
    let mut column_names = pg_client
        .query(&format!("SHOW COLUMNS FROM {}", &relation_name), &[])
        .await
        .context(format!("Failed to get column names for {}", relation_name))?
        .into_iter()
        .map(|row| match row.try_get::<_, String>("name") {
            Ok(name) => Some(name),
            Err(_) => None,
        })
        .filter_map(|row| row)
        .collect::<Vec<_>>();

    match relation.category {
        RelationCategory::Retained => {
            column_names.splice(0..0, ["mz_timestamp".to_string(), "mz_diff".to_string()]);
        }
        _ => (),
    }

    Ok(column_names)
}

pub async fn query_relation(
    transaction: &Transaction<'_>,
    base_path: PathBuf,
    relation: &Relation,
    column_names: &Vec<String>,
    cluster_replica: Option<&ClusterReplica>,
) -> Result<(), anyhow::Error> {
    let relation_name = relation.name;
    let relation_category = &relation.category;

    // Some queries (i.e. mz_introspection relations) require the cluster and replica to be set.
    if let Some(cluster_replica) = &cluster_replica {
        transaction
            .execute(
                &format!("SET LOCAL CLUSTER = '{}'", cluster_replica.cluster_name),
                &[],
            )
            .await
            .context(format!(
                "Failed to set cluster to {}",
                cluster_replica.cluster_name
            ))?;
        transaction
            .execute(
                &format!(
                    "SET LOCAL CLUSTER_REPLICA = '{}'",
                    cluster_replica.replica_name
                ),
                &[],
            )
            .await
            .context(format!(
                "Failed to set cluster replica to {}",
                cluster_replica.replica_name
            ))?;
    }

    match relation_category {
        RelationCategory::Basic => {
            let file_path = format_file_path(base_path, None);
            let file_path_name = file_path.join(relation_name).with_extension("csv");
            tokio::fs::create_dir_all(&file_path).await?;

            copy_relation_to_csv(transaction, file_path_name, column_names, relation).await?;
        }
        RelationCategory::Introspection => {
            let file_path = format_file_path(base_path, cluster_replica);
            tokio::fs::create_dir_all(&file_path).await?;

            let file_path_name = file_path.join(relation_name).with_extension("csv");

            copy_relation_to_csv(transaction, file_path_name, column_names, relation).await?;
        }
        RelationCategory::Retained => {
            // Copy the current state and retained subscribe state
            let file_path = format_file_path(base_path, None);
            let file_path_name = file_path
                .join(format!("{}_subscribe", relation_name))
                .with_extension("csv");
            tokio::fs::create_dir_all(&file_path).await?;

            copy_relation_to_csv(transaction, file_path_name, column_names, relation).await?;
        }
    }
    Ok::<(), anyhow::Error>(())
}

impl SystemCatalogDumper {
    pub async fn new(connection_url: &str, base_path: PathBuf) -> Result<Self, anyhow::Error> {
        let (pg_client, pg_conn, pg_tls) = create_postgres_connection(connection_url).await?;

        info!("Connected to PostgreSQL server at {}", connection_url);

        let handle = task::spawn(|| "postgres-connection", pg_conn);

        // Set search path to system catalog tables
        pg_client
            .execute(SET_SEARCH_PATH_QUERY, &[])
            .await
            .context("Failed to set search path")?;

        // We need to get all cluster replicas to dump introspection relations.
        let cluster_replicas = match pg_client.query(SELECT_CLUSTER_REPLICAS_QUERY, &[]).await {
            Ok(rows) => rows
                .into_iter()
                .map(|row| {
                    let cluster_name = row.try_get::<_, String>("cluster_name");
                    let replica_name = row.try_get::<_, String>("replica_name");

                    if let (Ok(cluster_name), Ok(replica_name)) = (cluster_name, replica_name) {
                        Some(ClusterReplica {
                            cluster_name,
                            replica_name,
                        })
                    } else {
                        None
                    }
                })
                .filter_map(|row| row)
                .collect::<Vec<_>>(),
            Err(e) => {
                warn!("Failed to get replica names: {}", e);
                vec![]
            }
        };

        Ok(Self {
            base_path,
            pg_client: Arc::new(Mutex::new(pg_client)),
            pg_tls,
            cluster_replicas,
            _pg_conn_handle: handle,
        })
    }

    pub async fn dump_relation(
        &self,
        relation: &Relation,
        cluster_replica: Option<&ClusterReplica>,
    ) -> Result<(), anyhow::Error> {
        info!(
            "Copying relation {}{}{}",
            relation.name,
            match relation.category {
                RelationCategory::Retained => " (subscribe history)",
                _ => "",
            },
            cluster_replica.map_or_else(|| "".to_string(), |replica| format!(" in {}", replica))
        );

        let base_path = self.base_path.clone();
        let pg_client = &self.pg_client;

        let relation_name = relation.name.to_string();

        if let Err(err) = retry::Retry::default()
            .max_duration(PG_QUERY_TIMEOUT)
            .initial_backoff(Duration::from_secs(2))
            .retry_async_canceling(|_| {
                let base_path = base_path.clone();
                let relation_name = relation.name;
                let cluster_replica = cluster_replica.clone();

                async move {
                    // TODO (debug_tool3): Use a transaction for the entire dump instead of per query.
                    let mut pg_client = pg_client.lock().await;

                    match async {
                        // We cannot query the column names in the transaction because SUBSCRIBE queries
                        // cannot be executed with SELECT and SHOW queries in the same transaction.
                        let column_names = query_column_names(&pg_client, relation).await?;

                        let transaction = pg_client.transaction().await?;
                        query_relation(
                            &transaction,
                            base_path,
                            relation,
                            &column_names,
                            cluster_replica,
                        )
                        .await?;

                        Ok::<(), anyhow::Error>(())
                    }
                    .await
                    {
                        Ok(()) => Ok(()),
                        Err(err) => {
                            warn!(
                                "{}: {:#}. Retrying...",
                                format_catalog_dump_error_message(relation_name, cluster_replica),
                                err
                            );
                            Err(err)
                        }
                    }
                }
            })
            .await
        {
            let pg_client_lock = pg_client.lock().await;

            let cancel_token = pg_client_lock.cancel_token();

            if let Err(_) = async {
                let tls = self.pg_tls.clone();

                cancel_token.cancel_query(tls).await?;
                Ok::<(), anyhow::Error>(())
            }
            .await
            {
                warn!(
                    "Failed to cancel query for {}{}",
                    relation_name,
                    cluster_replica
                        .map_or_else(|| "".to_string(), |replica| format!(" for {}", replica))
                );
            }

            return Err(err);
        }

        Ok(())
    }

    pub async fn dump_all_relations(&self) {
        let cluster_replicas = &self.cluster_replicas;

        // Create a map to count errors by cluster replica..
        let mut cluster_replica_error_counts: HashMap<ClusterReplica, usize> = HashMap::new();
        for replica in cluster_replicas {
            cluster_replica_error_counts
                .entry(replica.clone())
                .insert_entry(0);
        }

        let non_introspection_iter = RELATIONS
            .iter()
            .filter(|relation| {
                matches!(
                    relation.category,
                    RelationCategory::Basic | RelationCategory::Retained
                )
            })
            .map(|relation| (relation, None::<&ClusterReplica>));

        let introspection_iter = RELATIONS
            .iter()
            .filter(|relation| matches!(relation.category, RelationCategory::Introspection))
            .collect::<Vec<_>>();

        let introspection_iter = cluster_replicas.iter().flat_map(|replica| {
            introspection_iter
                .iter()
                .map(move |relation| (*relation, Some(replica)))
        });

        // Combine and iterate over all relation/replica pairs
        for (relation, replica) in non_introspection_iter.chain(introspection_iter) {
            let replica_key = if let Some(replica) = replica {
                replica
            } else {
                // If the replica is null, we assume it's  mz_catalog_server.
                &ClusterReplica::default()
            };

            // If the cluster replica has more than `MAX_CLUSTER_REPLICA_ERROR_COUNT` errors,
            // we can skip it since we can assume it's not responsive and don't want to hold up
            // following queries.
            if cluster_replica_error_counts.get(replica_key).unwrap_or(&0)
                >= &MAX_CLUSTER_REPLICA_ERROR_COUNT
            {
                info!(
                    "Skipping {}{}",
                    relation.name,
                    replica.map_or_else(|| "".to_string(), |replica| format!(" for {}", replica))
                );
                continue;
            }

            if let Err(err) = self.dump_relation(relation, replica).await {
                warn!(
                    "{}: {:#}.",
                    format_catalog_dump_error_message(relation.name, replica),
                    err,
                );

                if err.to_string().contains("deadline has elapsed") {
                    let docs_link = if replica.is_none()
                        || replica.map_or(false, |r| r.cluster_name == "mz_catalog_server")
                    {
                        "https://materialize.com/docs/self-managed/v25.1/installation/troubleshooting/#troubleshooting-console-unresponsiveness"
                    } else {
                        "https://materialize.com/docs/sql/alter-cluster/#resizing-1"
                    };
                    warn!("Consider increasing the size of the cluster {}", docs_link);
                }

                let is_missing_catalog_item_err = match err.downcast_ref::<tokio_postgres::Error>()
                {
                    Some(pg_err) => pg_err
                        .to_string()
                        .to_lowercase()
                        .contains("unknown catalog item"),
                    None => false,
                };

                // If the error is due to a missing catalog item,
                // we don't count it as an error since we expect some
                // catalog items to be missing. This is because mz-debug
                // is meant to be backwards compatible with older versions
                // of Materialize.
                if !is_missing_catalog_item_err {
                    cluster_replica_error_counts
                        .entry(replica_key.clone())
                        .and_modify(|count| *count += 1)
                        .or_insert(1);
                }
            }
        }
    }
}

fn format_catalog_dump_error_message(
    relation_name: &str,
    cluster_replica: Option<&ClusterReplica>,
) -> String {
    format!(
        "Failed to dump relation {}{}",
        relation_name,
        cluster_replica.map_or_else(|| "".to_string(), |replica| format!(" for {}", replica))
    )
}

fn format_file_path(base_path: PathBuf, cluster_replica: Option<&ClusterReplica>) -> PathBuf {
    let path = base_path.join(SYSTEM_CATALOG_DUMP_DIR);
    if let Some(cluster_replica) = cluster_replica {
        path.join(cluster_replica.cluster_name.as_str())
            .join(cluster_replica.replica_name.as_str())
    } else {
        path
    }
}
