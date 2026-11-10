// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use mz_adapter_types::connection::ConnectionId;
use mz_catalog::memory::objects::CatalogItem;
use mz_controller::clusters::{ManagedReplicaLocation, ReplicaLocation};
use mz_controller_types::ClusterId;
use mz_ore::soft_panic_or_log;
use mz_repr::adt::numeric::Numeric;
use mz_sql::names::ResolvedDatabaseSpecifier;
use mz_sql::plan::ConnectionDetails;
use mz_sql::session::vars::{
    MAX_AWS_PRIVATELINK_CONNECTIONS, MAX_CLUSTERS, MAX_CREDIT_CONSUMPTION_RATE, MAX_DATABASES,
    MAX_KAFKA_CONNECTIONS, MAX_MATERIALIZED_VIEWS, MAX_MYSQL_CONNECTIONS, MAX_NETWORK_POLICIES,
    MAX_OBJECTS_PER_SCHEMA, MAX_POSTGRES_CONNECTIONS, MAX_REPLICAS_PER_CLUSTER, MAX_ROLES,
    MAX_SCHEMAS_PER_DATABASE, MAX_SECRETS, MAX_SINKS, MAX_SOURCES, MAX_SQL_SERVER_CONNECTIONS,
    MAX_TABLES, SystemVars, Var,
};

use crate::AdapterError;
use crate::catalog::{CatalogState, DropObjectInfo, Op};

impl CatalogState {
    /// Validate all resource limits in a catalog transaction and return an error if that limit is
    /// exceeded.
    pub(crate) fn validate_resource_limits(
        &self,
        ops: &[Op],
        conn_id: &ConnectionId,
    ) -> Result<(), AdapterError> {
        let mut new_kafka_connections = 0;
        let mut new_postgres_connections = 0;
        let mut new_mysql_connections = 0;
        let mut new_sql_server_connections = 0;
        let mut new_aws_privatelink_connections = 0;
        let mut new_tables = 0;
        let mut new_sources = 0;
        let mut new_sinks = 0;
        let mut new_materialized_views = 0;
        let mut new_clusters = 0;
        let mut new_replicas_per_cluster = BTreeMap::new();
        let mut new_credit_consumption_rate = Numeric::zero();
        let mut new_databases = 0;
        let mut new_schemas_per_database = BTreeMap::new();
        let mut new_objects_per_schema = BTreeMap::new();
        let mut new_secrets = 0;
        let mut new_roles = 0;
        let mut new_network_policies = 0;
        for op in ops {
            match op {
                Op::CreateDatabase { .. } => {
                    new_databases += 1;
                }
                Op::CreateSchema { database_id, .. } => {
                    if let ResolvedDatabaseSpecifier::Id(database_id) = database_id {
                        *new_schemas_per_database.entry(database_id).or_insert(0) += 1;
                    }
                }
                Op::CreateRole { .. } => {
                    new_roles += 1;
                }
                Op::CreateNetworkPolicy { .. } => {
                    new_network_policies += 1;
                }
                Op::CreateCluster { .. } => {
                    // TODO(benesch): having deprecated linked clusters, remove
                    // the `max_sources` and `max_sinks` limit, and set a higher
                    // max cluster limit?
                    new_clusters += 1;
                }
                Op::CreateClusterReplica {
                    cluster_id, config, ..
                } => {
                    if cluster_id.is_user() {
                        *new_replicas_per_cluster.entry(*cluster_id).or_insert(0) += 1;
                        if let ReplicaLocation::Managed(location) = &config.location {
                            new_credit_consumption_rate += self.replica_credits_per_hour(location);
                        }
                    }
                }
                Op::CreateItem { name, item, .. } => {
                    *new_objects_per_schema
                        .entry((
                            name.qualifiers.database_spec.clone(),
                            name.qualifiers.schema_spec.clone(),
                        ))
                        .or_insert(0) += 1;
                    match item {
                        CatalogItem::Connection(connection) => match connection.details {
                            ConnectionDetails::Kafka(_) => new_kafka_connections += 1,
                            ConnectionDetails::Postgres(_) => new_postgres_connections += 1,
                            ConnectionDetails::MySql(_) => new_mysql_connections += 1,
                            ConnectionDetails::SqlServer(_) => new_sql_server_connections += 1,
                            ConnectionDetails::AwsPrivatelink(_) => {
                                new_aws_privatelink_connections += 1
                            }
                            ConnectionDetails::Csr(_)
                            | ConnectionDetails::GlueSchemaRegistry(_)
                            | ConnectionDetails::Ssh { .. }
                            | ConnectionDetails::Aws(_)
                            | ConnectionDetails::Gcp(_)
                            | ConnectionDetails::IcebergCatalog(_) => {}
                        },
                        CatalogItem::Table(_) => {
                            new_tables += 1;
                        }
                        CatalogItem::Source(source) => {
                            new_sources += source.user_controllable_persist_shard_count()
                        }
                        CatalogItem::Sink(_) => new_sinks += 1,
                        CatalogItem::MaterializedView(_) => {
                            new_materialized_views += 1;
                        }
                        CatalogItem::Secret(_) => {
                            new_secrets += 1;
                        }
                        CatalogItem::Log(_)
                        | CatalogItem::View(_)
                        | CatalogItem::Index(_)
                        | CatalogItem::Type(_)
                        | CatalogItem::Func(_)
                        | CatalogItem::MetricSink(_) => {}
                    }
                }
                Op::DropObjects(drop_object_infos) => {
                    for drop_object_info in drop_object_infos {
                        match drop_object_info {
                            DropObjectInfo::Cluster(_) => {
                                new_clusters -= 1;
                            }
                            DropObjectInfo::ClusterReplica((cluster_id, replica_id, _reason)) => {
                                if cluster_id.is_user() {
                                    *new_replicas_per_cluster.entry(*cluster_id).or_insert(0) -= 1;
                                    let cluster =
                                        self.get_cluster_replica(*cluster_id, *replica_id);
                                    if let ReplicaLocation::Managed(location) =
                                        &cluster.config.location
                                    {
                                        new_credit_consumption_rate -=
                                            self.replica_credits_per_hour(location);
                                    }
                                }
                            }
                            DropObjectInfo::Database(_) => {
                                new_databases -= 1;
                            }
                            DropObjectInfo::Schema((database_spec, _)) => {
                                if let ResolvedDatabaseSpecifier::Id(database_id) = database_spec {
                                    *new_schemas_per_database.entry(database_id).or_insert(0) -= 1;
                                }
                            }
                            DropObjectInfo::Role(_) => {
                                new_roles -= 1;
                            }
                            DropObjectInfo::NetworkPolicy(_) => {
                                new_network_policies -= 1;
                            }
                            DropObjectInfo::Item(id) => {
                                let entry = self.get_entry(id);
                                *new_objects_per_schema
                                    .entry((
                                        entry.name().qualifiers.database_spec.clone(),
                                        entry.name().qualifiers.schema_spec.clone(),
                                    ))
                                    .or_insert(0) -= 1;
                                match entry.item() {
                                    CatalogItem::Connection(connection) => match connection.details
                                    {
                                        ConnectionDetails::AwsPrivatelink(_) => {
                                            new_aws_privatelink_connections -= 1;
                                        }
                                        _ => (),
                                    },
                                    CatalogItem::Table(_) => {
                                        new_tables -= 1;
                                    }
                                    CatalogItem::Source(source) => {
                                        new_sources -=
                                            source.user_controllable_persist_shard_count()
                                    }
                                    CatalogItem::Sink(_) => new_sinks -= 1,
                                    CatalogItem::MaterializedView(_) => {
                                        new_materialized_views -= 1;
                                    }
                                    CatalogItem::Secret(_) => {
                                        new_secrets -= 1;
                                    }
                                    CatalogItem::Log(_)
                                    | CatalogItem::View(_)
                                    | CatalogItem::Index(_)
                                    | CatalogItem::Type(_)
                                    | CatalogItem::Func(_)
                                    | CatalogItem::MetricSink(_) => {}
                                }
                            }
                        }
                    }
                }
                Op::UpdateItem {
                    name: _,
                    id,
                    to_item,
                } => match to_item {
                    CatalogItem::Source(source) => {
                        let current_source = self
                            .get_entry(id)
                            .source()
                            .expect("source update is for source item");

                        new_sources += source.user_controllable_persist_shard_count()
                            - current_source.user_controllable_persist_shard_count();
                    }
                    CatalogItem::Connection(_)
                    | CatalogItem::Table(_)
                    | CatalogItem::Sink(_)
                    | CatalogItem::MaterializedView(_)
                    | CatalogItem::Secret(_)
                    | CatalogItem::Log(_)
                    | CatalogItem::View(_)
                    | CatalogItem::Index(_)
                    | CatalogItem::Type(_)
                    | CatalogItem::Func(_)
                    | CatalogItem::MetricSink(_) => {}
                },
                Op::AlterRole { .. }
                | Op::AlterRetainHistory { .. }
                | Op::AlterSourceTimestampInterval { .. }
                | Op::AlterNetworkPolicy { .. }
                | Op::AlterAddColumn { .. }
                | Op::AlterMaterializedViewApplyReplacement { .. }
                | Op::UpdatePrivilege { .. }
                | Op::UpdateDefaultPrivilege { .. }
                | Op::GrantRole { .. }
                | Op::RenameCluster { .. }
                | Op::RenameClusterReplica { .. }
                | Op::RenameItem { .. }
                | Op::RenameSchema { .. }
                | Op::UpdateOwner { .. }
                | Op::RevokeRole { .. }
                | Op::UpdateClusterConfig { .. }
                | Op::UpdateSourceReferences { .. }
                | Op::UpdateSystemConfiguration { .. }
                | Op::ResetSystemConfiguration { .. }
                | Op::ResetAllSystemConfiguration { .. }
                | Op::UpdateScopedSystemParameters { .. }
                | Op::Comment { .. }
                | Op::RenameCommand(_)
                | Op::CheckClusterState { .. }
                | Op::InjectAuditEvents { .. } => {}
            }
        }

        let mut current_aws_privatelink_connections = 0;
        let mut current_postgres_connections = 0;
        let mut current_mysql_connections = 0;
        let mut current_sql_server_connections = 0;
        let mut current_kafka_connections = 0;
        for c in self.user_connections() {
            let connection = c
                .connection()
                .expect("`user_connections()` only returns connection objects");

            match connection.details {
                ConnectionDetails::AwsPrivatelink(_) => current_aws_privatelink_connections += 1,
                ConnectionDetails::Postgres(_) => current_postgres_connections += 1,
                ConnectionDetails::MySql(_) => current_mysql_connections += 1,
                ConnectionDetails::SqlServer(_) => current_sql_server_connections += 1,
                ConnectionDetails::Kafka(_) => current_kafka_connections += 1,
                ConnectionDetails::Csr(_)
                | ConnectionDetails::GlueSchemaRegistry(_)
                | ConnectionDetails::Ssh { .. }
                | ConnectionDetails::Aws(_)
                | ConnectionDetails::Gcp(_)
                | ConnectionDetails::IcebergCatalog(_) => {}
            }
        }
        self.validate_resource_limit(
            current_kafka_connections,
            new_kafka_connections,
            SystemVars::max_kafka_connections,
            "Kafka Connection",
            MAX_KAFKA_CONNECTIONS.name(),
        )?;
        self.validate_resource_limit(
            current_postgres_connections,
            new_postgres_connections,
            SystemVars::max_postgres_connections,
            "PostgreSQL Connection",
            MAX_POSTGRES_CONNECTIONS.name(),
        )?;
        self.validate_resource_limit(
            current_mysql_connections,
            new_mysql_connections,
            SystemVars::max_mysql_connections,
            "MySQL Connection",
            MAX_MYSQL_CONNECTIONS.name(),
        )?;
        self.validate_resource_limit(
            current_sql_server_connections,
            new_sql_server_connections,
            SystemVars::max_sql_server_connections,
            "SQL Server Connection",
            MAX_SQL_SERVER_CONNECTIONS.name(),
        )?;
        self.validate_resource_limit(
            current_aws_privatelink_connections,
            new_aws_privatelink_connections,
            SystemVars::max_aws_privatelink_connections,
            "AWS PrivateLink Connection",
            MAX_AWS_PRIVATELINK_CONNECTIONS.name(),
        )?;
        self.validate_resource_limit(
            self.user_tables().count(),
            new_tables,
            SystemVars::max_tables,
            "table",
            MAX_TABLES.name(),
        )?;

        let current_sources: usize = self
            .user_sources()
            .filter_map(|source| source.source())
            .map(|source| source.user_controllable_persist_shard_count())
            .sum::<i64>()
            .try_into()
            .expect("non-negative sum of sources");

        self.validate_resource_limit(
            current_sources,
            new_sources,
            SystemVars::max_sources,
            "source",
            MAX_SOURCES.name(),
        )?;
        self.validate_resource_limit(
            self.user_sinks().count(),
            new_sinks,
            SystemVars::max_sinks,
            "sink",
            MAX_SINKS.name(),
        )?;
        self.validate_resource_limit(
            self.user_materialized_views().count(),
            new_materialized_views,
            SystemVars::max_materialized_views,
            "materialized view",
            MAX_MATERIALIZED_VIEWS.name(),
        )?;
        self.validate_resource_limit(
            // Linked compute clusters don't count against the limit, since
            // we have a separate sources and sinks limit.
            //
            // TODO(benesch): remove the `max_sources` and `max_sinks` limit,
            // and set a higher max cluster limit?
            self.user_clusters().count(),
            new_clusters,
            SystemVars::max_clusters,
            "cluster",
            MAX_CLUSTERS.name(),
        )?;
        for (cluster_id, new_replicas) in new_replicas_per_cluster {
            // It's possible that the cluster hasn't been created yet.
            let current_amount = self
                .try_get_cluster(cluster_id)
                .map(|instance| instance.user_replicas().count())
                .unwrap_or(0);
            self.validate_resource_limit(
                current_amount,
                new_replicas,
                SystemVars::max_replicas_per_cluster,
                "cluster replica",
                MAX_REPLICAS_PER_CLUSTER.name(),
            )?;
        }
        self.validate_resource_limit_numeric(
            self.current_credit_consumption_rate(None),
            new_credit_consumption_rate,
            |system_vars| {
                self.license_key
                    .max_credit_consumption_rate()
                    .map_or_else(|| system_vars.max_credit_consumption_rate(), Numeric::from)
            },
            "cluster replica",
            MAX_CREDIT_CONSUMPTION_RATE.name(),
        )?;
        self.validate_resource_limit(
            self.databases().count(),
            new_databases,
            SystemVars::max_databases,
            "database",
            MAX_DATABASES.name(),
        )?;
        for (database_id, new_schemas) in new_schemas_per_database {
            self.validate_resource_limit(
                self.get_database(database_id).schemas_by_id.len(),
                new_schemas,
                SystemVars::max_schemas_per_database,
                "schema",
                MAX_SCHEMAS_PER_DATABASE.name(),
            )?;
        }
        for ((database_spec, schema_spec), new_objects) in new_objects_per_schema {
            // For temporary schemas that don't exist yet (lazy creation),
            // treat them as having 0 items.
            let current_items = self
                .try_get_schema(&database_spec, &schema_spec, conn_id)
                .map(|schema| schema.items.len())
                .unwrap_or(0);
            self.validate_resource_limit(
                current_items,
                new_objects,
                SystemVars::max_objects_per_schema,
                "object",
                MAX_OBJECTS_PER_SCHEMA.name(),
            )?;
        }
        self.validate_resource_limit(
            self.user_secrets().count(),
            new_secrets,
            SystemVars::max_secrets,
            "secret",
            MAX_SECRETS.name(),
        )?;
        self.validate_resource_limit(
            self.user_roles().count(),
            new_roles,
            SystemVars::max_roles,
            "role",
            MAX_ROLES.name(),
        )?;
        self.validate_resource_limit(
            self.user_network_policies().count(),
            new_network_policies,
            SystemVars::max_network_policies,
            "network_policy",
            MAX_NETWORK_POLICIES.name(),
        )?;
        Ok(())
    }

    /// Validate a specific type of resource limit and return an error if that limit is exceeded.
    pub(crate) fn validate_resource_limit<F>(
        &self,
        current_amount: usize,
        new_instances: i64,
        resource_limit: F,
        resource_type: &str,
        limit_name: &str,
    ) -> Result<(), AdapterError>
    where
        F: Fn(&SystemVars) -> u32,
    {
        if new_instances <= 0 {
            return Ok(());
        }

        let limit: i64 = resource_limit(self.system_config()).into();
        let current_amount: Option<i64> = current_amount.try_into().ok();
        let desired =
            current_amount.and_then(|current_amount| current_amount.checked_add(new_instances));

        let exceeds_limit = if let Some(desired) = desired {
            desired > limit
        } else {
            true
        };

        let desired = desired
            .map(|desired| desired.to_string())
            .unwrap_or_else(|| format!("more than {}", i64::MAX));
        let current = current_amount
            .map(|current| current.to_string())
            .unwrap_or_else(|| format!("more than {}", i64::MAX));
        if exceeds_limit {
            Err(AdapterError::ResourceExhaustion {
                resource_type: resource_type.to_string(),
                limit_name: limit_name.to_string(),
                desired,
                limit: limit.to_string(),
                current,
            })
        } else {
            Ok(())
        }
    }

    /// Validate a specific type of float resource limit and return an error if that limit is exceeded.
    ///
    /// This is very similar to [`Self::validate_resource_limit`] but for numerics.
    pub(crate) fn validate_resource_limit_numeric<F>(
        &self,
        current_amount: Numeric,
        new_amount: Numeric,
        resource_limit: F,
        resource_type: &str,
        limit_name: &str,
    ) -> Result<(), AdapterError>
    where
        F: Fn(&SystemVars) -> Numeric,
    {
        if new_amount <= Numeric::zero() {
            return Ok(());
        }

        let limit = resource_limit(self.system_config());
        // Floats will overflow to infinity instead of panicking, which has the correct comparison
        // semantics.
        // NaN should be impossible here since both values are positive.
        let desired = current_amount + new_amount;
        if desired > limit {
            Err(AdapterError::ResourceExhaustion {
                resource_type: resource_type.to_string(),
                limit_name: limit_name.to_string(),
                desired: desired.to_string(),
                limit: limit.to_string(),
                current: current_amount.to_string(),
            })
        } else {
            Ok(())
        }
    }
    /// The environment's current credit consumption rate, summed over all user
    /// cluster replicas except those of `exclude_cluster`.
    pub(crate) fn current_credit_consumption_rate(
        &self,
        exclude_cluster: Option<ClusterId>,
    ) -> Numeric {
        self.user_cluster_replicas()
            .filter(|replica| Some(replica.cluster_id) != exclude_cluster)
            .filter_map(|replica| match &replica.config.location {
                ReplicaLocation::Managed(location) => Some(self.replica_credits_per_hour(location)),
                ReplicaLocation::Unmanaged(_) => None,
            })
            .sum()
    }

    /// The credit rate of a managed replica, read from the size map by its billing size.
    ///
    /// An unknown billing size counts as free. DDL validates `SIZE` and `BILLED AS` against
    /// the map at replica creation, but the map is external configuration and can lose a
    /// size later. That case is a soft panic rather than a hard one, so that in production
    /// such a replica can still be dropped from SQL.
    pub(crate) fn replica_credits_per_hour(&self, location: &ManagedReplicaLocation) -> Numeric {
        let size = location.size_for_billing();
        match self.cluster_replica_sizes().0.get(size) {
            Some(allocation) => allocation.credits_per_hour,
            None => {
                soft_panic_or_log!(
                    "replica of size {:?} bills as unknown replica size {:?}, counting it as free",
                    location.size,
                    size,
                );
                Numeric::zero()
            }
        }
    }
}
