// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod notice;

use bytesize::ByteSize;
use ipnet::IpNet;
use mz_adapter_types::compaction::CompactionWindow;
use mz_adapter_types::dyncfgs::ENABLE_PASSWORD_AUTH;
use mz_audit_log::{EventDetails, EventType, ObjectType, VersionedEvent, VersionedStorageUsage};
use mz_catalog::SYSTEM_CONN_ID;
use mz_catalog::builtin::{
    BuiltinTable, MZ_AGGREGATES, MZ_ARRAY_TYPES, MZ_AUDIT_EVENTS, MZ_AWS_CONNECTIONS,
    MZ_AWS_PRIVATELINK_CONNECTIONS, MZ_BASE_TYPES, MZ_CLUSTER_REPLICA_SIZES, MZ_CLUSTER_REPLICAS,
    MZ_CLUSTER_SCHEDULES, MZ_CLUSTER_WORKLOAD_CLASSES, MZ_CLUSTERS, MZ_COLUMNS, MZ_COMMENTS,
    MZ_CONNECTIONS, MZ_CONTINUAL_TASKS, MZ_DATABASES, MZ_DEFAULT_PRIVILEGES, MZ_EGRESS_IPS,
    MZ_FUNCTIONS, MZ_HISTORY_RETENTION_STRATEGIES, MZ_ICEBERG_SINKS, MZ_INDEX_COLUMNS, MZ_INDEXES,
    MZ_INTERNAL_CLUSTER_REPLICAS, MZ_KAFKA_CONNECTIONS, MZ_KAFKA_SINKS, MZ_KAFKA_SOURCE_TABLES,
    MZ_KAFKA_SOURCES, MZ_LICENSE_KEYS, MZ_LIST_TYPES, MZ_MAP_TYPES,
    MZ_MATERIALIZED_VIEW_REFRESH_STRATEGIES, MZ_MATERIALIZED_VIEWS, MZ_MYSQL_SOURCE_TABLES,
    MZ_NETWORK_POLICIES, MZ_NETWORK_POLICY_RULES, MZ_OBJECT_DEPENDENCIES, MZ_OBJECT_GLOBAL_IDS,
    MZ_OPERATORS, MZ_PENDING_CLUSTER_REPLICAS, MZ_POSTGRES_SOURCE_TABLES, MZ_POSTGRES_SOURCES,
    MZ_PSEUDO_TYPES, MZ_REPLACEMENTS, MZ_ROLE_AUTH, MZ_ROLE_MEMBERS, MZ_ROLE_PARAMETERS, MZ_ROLES,
    MZ_SCHEMAS, MZ_SECRETS, MZ_SESSIONS, MZ_SINKS, MZ_SOURCE_REFERENCES, MZ_SOURCES,
    MZ_SQL_SERVER_SOURCE_TABLES, MZ_SSH_TUNNEL_CONNECTIONS, MZ_STORAGE_USAGE_BY_SHARD,
    MZ_SUBSCRIPTIONS, MZ_SYSTEM_PRIVILEGES, MZ_TABLES, MZ_TYPE_PG_METADATA, MZ_TYPES, MZ_VIEWS,
    MZ_WEBHOOKS_SOURCES,
};
use mz_catalog::config::AwsPrincipalContext;
use mz_catalog::durable::SourceReferences;
use mz_catalog::memory::error::{Error, ErrorKind};
use mz_catalog::memory::objects::{
    CatalogEntry, CatalogItem, ClusterVariant, Connection, ContinualTask, DataSourceDesc, Func,
    Index, MaterializedView, Sink, Table, TableDataSource, Type, View,
};
use mz_controller::clusters::{
    ManagedReplicaAvailabilityZones, ManagedReplicaLocation, ReplicaLocation,
};
use mz_controller_types::ClusterId;
use mz_expr::MirScalarExpr;
use mz_license_keys::ValidatedLicenseKey;
use mz_orchestrator::{CpuLimit, DiskLimit, MemoryLimit};
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_persist_client::batch::ProtoBatch;
use mz_repr::adt::array::ArrayDimension;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::jsonb::Jsonb;
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem, PrivilegeMap};
use mz_repr::adt::regex;
use mz_repr::network_policy_id::NetworkPolicyId;
use mz_repr::refresh_schedule::RefreshEvery;
use mz_repr::role_id::RoleId;
use mz_repr::{CatalogItemId, Datum, Diff, GlobalId, Row, RowPacker, SqlScalarType, Timestamp};
use mz_sql::ast::{ContinualTaskStmt, CreateIndexStatement, Statement, UnresolvedItemName};
use mz_sql::catalog::{
    CatalogCluster, CatalogDatabase, CatalogSchema, CatalogType, DefaultPrivilegeObject,
    TypeCategory,
};
use mz_sql::func::FuncImplCatalogDetails;
use mz_sql::names::{
    CommentObjectId, DatabaseId, ResolvedDatabaseSpecifier, SchemaId, SchemaSpecifier,
};
use mz_sql::plan::{ClusterSchedule, ConnectionDetails, SshKey};
use mz_sql::session::user::{MZ_SUPPORT_ROLE_ID, MZ_SYSTEM_ROLE_ID, SYSTEM_USER};
use mz_sql::session::vars::SessionVars;
use mz_sql_parser::ast::display::AstDisplay;
use mz_storage_client::client::TableData;
use mz_storage_types::connections::KafkaConnection;
use mz_storage_types::connections::aws::{AwsAuth, AwsConnection};
use mz_storage_types::connections::inline::ReferencedConnection;
use mz_storage_types::connections::string_or_secret::StringOrSecret;
use mz_storage_types::sinks::{IcebergSinkConnection, KafkaSinkConnection, StorageSinkConnection};
use mz_storage_types::sources::{
    GenericSourceConnection, KafkaSourceConnection, PostgresSourceConnection, SourceConnection,
};
use smallvec::smallvec;

// DO NOT add any more imports from `crate` outside of `crate::catalog`.
use crate::active_compute_sink::ActiveSubscribe;
use crate::catalog::CatalogState;
use crate::coord::ConnMeta;

/// An update to a built-in table.
#[derive(Debug, Clone)]
pub struct BuiltinTableUpdate<T = CatalogItemId> {
    /// The reference of the table to update.
    pub id: T,
    /// The data to put into the table.
    pub data: TableData,
}

impl<T> BuiltinTableUpdate<T> {
    /// Create a [`BuiltinTableUpdate`] from a [`Row`].
    pub fn row(id: T, row: Row, diff: Diff) -> BuiltinTableUpdate<T> {
        BuiltinTableUpdate {
            id,
            data: TableData::Rows(vec![(row, diff)]),
        }
    }

    pub fn batch(id: T, batch: ProtoBatch) -> BuiltinTableUpdate<T> {
        BuiltinTableUpdate {
            id,
            data: TableData::Batches(smallvec![batch]),
        }
    }
}

impl CatalogState {
    pub fn resolve_builtin_table_updates(
        &self,
        builtin_table_update: Vec<BuiltinTableUpdate<&'static BuiltinTable>>,
    ) -> Vec<BuiltinTableUpdate<CatalogItemId>> {
        builtin_table_update
            .into_iter()
            .map(|builtin_table_update| self.resolve_builtin_table_update(builtin_table_update))
            .collect()
    }

    pub fn resolve_builtin_table_update(
        &self,
        BuiltinTableUpdate { id, data }: BuiltinTableUpdate<&'static BuiltinTable>,
    ) -> BuiltinTableUpdate<CatalogItemId> {
        let id = self.resolve_builtin_table(id);
        BuiltinTableUpdate { id, data }
    }

    pub fn pack_depends_update(
        &self,
        depender: CatalogItemId,
        dependee: CatalogItemId,
        diff: Diff,
    ) -> BuiltinTableUpdate<&'static BuiltinTable> {
        let row = Row::pack_slice(&[
            Datum::String(&depender.to_string()),
            Datum::String(&dependee.to_string()),
        ]);
        BuiltinTableUpdate::row(&*MZ_OBJECT_DEPENDENCIES, row, diff)
    }

    pub(super) fn pack_database_update(
        &self,
        database_id: &DatabaseId,
        diff: Diff,
    ) -> BuiltinTableUpdate<&'static BuiltinTable> {
        let database = self.get_database(database_id);
        let row = self.pack_privilege_array_row(database.privileges());
        let privileges = row.unpack_first();
        BuiltinTableUpdate::row(
            &*MZ_DATABASES,
            Row::pack_slice(&[
                Datum::String(&database.id.to_string()),
                Datum::UInt32(database.oid),
                Datum::String(database.name()),
                Datum::String(&database.owner_id.to_string()),
                privileges,
            ]),
            diff,
        )
    }

    pub(super) fn pack_schema_update(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_id: &SchemaId,
        diff: Diff,
    ) -> BuiltinTableUpdate<&'static BuiltinTable> {
        let (database_id, schema) = match database_spec {
            ResolvedDatabaseSpecifier::Ambient => (None, &self.ambient_schemas_by_id[schema_id]),
            ResolvedDatabaseSpecifier::Id(id) => (
                Some(id.to_string()),
                &self.database_by_id[id].schemas_by_id[schema_id],
            ),
        };
        let row = self.pack_privilege_array_row(schema.privileges());
        let privileges = row.unpack_first();
        BuiltinTableUpdate::row(
            &*MZ_SCHEMAS,
            Row::pack_slice(&[
                Datum::String(&schema_id.to_string()),
                Datum::UInt32(schema.oid),
                Datum::from(database_id.as_deref()),
                Datum::String(&schema.name.schema),
                Datum::String(&schema.owner_id.to_string()),
                privileges,
            ]),
            diff,
        )
    }

    pub(super) fn pack_role_auth_update(
        &self,
        id: RoleId,
        diff: Diff,
    ) -> BuiltinTableUpdate<&'static BuiltinTable> {
        let role_auth = self.get_role_auth(&id);
        let role = self.get_role(&id);
        BuiltinTableUpdate::row(
            &*MZ_ROLE_AUTH,
            Row::pack_slice(&[
                Datum::String(&role_auth.role_id.to_string()),
                Datum::UInt32(role.oid),
                match &role_auth.password_hash {
                    Some(hash) => Datum::String(hash),
                    None => Datum::Null,
                },
                Datum::TimestampTz(
                    mz_ore::now::to_datetime(role_auth.updated_at)
                        .try_into()
                        .expect("must fit"),
                ),
            ]),
            diff,
        )
    }

    pub(super) fn pack_role_update(
        &self,
        id: RoleId,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        match id {
            // PUBLIC role should not show up in mz_roles.
            RoleId::Public => vec![],
            id => {
                let role = self.get_role(&id);
                let self_managed_auth_enabled = self
                    .system_config()
                    .get(ENABLE_PASSWORD_AUTH.name())
                    .ok()
                    .map(|v| v.value() == "on")
                    .unwrap_or(false);

                let builtin_supers = [MZ_SYSTEM_ROLE_ID, MZ_SUPPORT_ROLE_ID];

                // For self managed auth, we can get the actual login and superuser bits
                // directly from the role. For cloud auth, we have to do some heuristics.
                // We determine login status each time a role logs in, so there's no clean
                // way to accurately determine this in the catalog. Instead we do something
                // a little gross. For system roles, we hardcode the known roles that can
                // log in. For user roles, we determine `rolcanlogin` based on whether the
                // role name looks like an email address.
                //
                // This works for the vast majority of cases in production. Roles that users
                // log in to come from Frontegg and therefore *must* be valid email
                // addresses, while roles that are created via `CREATE ROLE` (e.g.,
                // `admin`, `prod_app`) almost certainly are not named to look like email
                // addresses.
                //
                // For the moment, we're comfortable with the edge cases here. If we discover
                // that folks are regularly creating non-login roles with names that look
                // like an email address (e.g., `admins@sysops.foocorp`), we can change
                // course.

                let cloud_login_regex = "^[^@]+@[^@]+\\.[^@]+$";
                let matches = regex::Regex::new(cloud_login_regex, true)
                    .expect("valid regex")
                    .is_match(&role.name);

                let rolcanlogin = if self_managed_auth_enabled {
                    role.attributes.login.unwrap_or(false)
                } else {
                    builtin_supers.contains(&role.id) || matches
                };

                let rolsuper = if self_managed_auth_enabled {
                    Datum::from(role.attributes.superuser.unwrap_or(false))
                } else if builtin_supers.contains(&role.id) {
                    Datum::from(true)
                } else {
                    Datum::Null
                };

                let role_update = BuiltinTableUpdate::row(
                    &*MZ_ROLES,
                    Row::pack_slice(&[
                        Datum::String(&role.id.to_string()),
                        Datum::UInt32(role.oid),
                        Datum::String(&role.name),
                        Datum::from(role.attributes.inherit),
                        Datum::from(rolcanlogin),
                        rolsuper,
                    ]),
                    diff,
                );
                let mut updates = vec![role_update];

                // HACK/TODO(parkmycar): Creating an empty SessionVars like this is pretty hacky,
                // we should instead have a static list of all session vars.
                let session_vars_reference = SessionVars::new_unchecked(
                    &mz_build_info::DUMMY_BUILD_INFO,
                    SYSTEM_USER.clone(),
                    None,
                );

                for (name, val) in role.vars() {
                    let result = session_vars_reference
                        .inspect(name)
                        .and_then(|var| var.check(val.borrow()));
                    let Ok(formatted_val) = result else {
                        // Note: all variables should have been validated by this point, so we
                        // shouldn't ever hit this.
                        tracing::error!(?name, ?val, ?result, "found invalid role default var");
                        continue;
                    };

                    let role_var_update = BuiltinTableUpdate::row(
                        &*MZ_ROLE_PARAMETERS,
                        Row::pack_slice(&[
                            Datum::String(&role.id.to_string()),
                            Datum::String(name),
                            Datum::String(&formatted_val),
                        ]),
                        diff,
                    );
                    updates.push(role_var_update);
                }

                updates
            }
        }
    }

    pub(super) fn pack_role_members_update(
        &self,
        role_id: RoleId,
        member_id: RoleId,
        diff: Diff,
    ) -> BuiltinTableUpdate<&'static BuiltinTable> {
        let grantor_id = self
            .get_role(&member_id)
            .membership
            .map
            .get(&role_id)
            .expect("catalog out of sync");
        BuiltinTableUpdate::row(
            &*MZ_ROLE_MEMBERS,
            Row::pack_slice(&[
                Datum::String(&role_id.to_string()),
                Datum::String(&member_id.to_string()),
                Datum::String(&grantor_id.to_string()),
            ]),
            diff,
        )
    }

    pub(super) fn pack_cluster_update(
        &self,
        name: &str,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        let id = self.clusters_by_name[name];
        let cluster = &self.clusters_by_id[&id];
        let row = self.pack_privilege_array_row(cluster.privileges());
        let privileges = row.unpack_first();
        let (size, disk, replication_factor, azs, introspection_debugging, introspection_interval) =
            match &cluster.config.variant {
                ClusterVariant::Managed(config) => (
                    Some(config.size.as_str()),
                    Some(self.cluster_replica_size_has_disk(&config.size)),
                    Some(config.replication_factor),
                    if config.availability_zones.is_empty() {
                        None
                    } else {
                        Some(config.availability_zones.clone())
                    },
                    Some(config.logging.log_logging),
                    config.logging.interval.map(|d| {
                        Interval::from_duration(&d)
                            .expect("planning ensured this convertible back to interval")
                    }),
                ),
                ClusterVariant::Unmanaged => (None, None, None, None, None, None),
            };

        let mut row = Row::default();
        let mut packer = row.packer();
        packer.extend([
            Datum::String(&id.to_string()),
            Datum::String(name),
            Datum::String(&cluster.owner_id.to_string()),
            privileges,
            cluster.is_managed().into(),
            size.into(),
            replication_factor.into(),
            disk.into(),
        ]);
        if let Some(azs) = azs {
            packer.push_list(azs.iter().map(|az| Datum::String(az)));
        } else {
            packer.push(Datum::Null);
        }
        packer.push(Datum::from(introspection_debugging));
        packer.push(Datum::from(introspection_interval));

        let mut updates = Vec::new();

        updates.push(BuiltinTableUpdate::row(&*MZ_CLUSTERS, row, diff));

        if let ClusterVariant::Managed(managed_config) = &cluster.config.variant {
            let row = match managed_config.schedule {
                ClusterSchedule::Manual => Row::pack_slice(&[
                    Datum::String(&id.to_string()),
                    Datum::String("manual"),
                    Datum::Null,
                ]),
                ClusterSchedule::Refresh {
                    hydration_time_estimate,
                } => Row::pack_slice(&[
                    Datum::String(&id.to_string()),
                    Datum::String("on-refresh"),
                    Datum::Interval(
                        Interval::from_duration(&hydration_time_estimate)
                            .expect("planning ensured that this is convertible back to Interval"),
                    ),
                ]),
            };
            updates.push(BuiltinTableUpdate::row(&*MZ_CLUSTER_SCHEDULES, row, diff));
        }

        updates.push(BuiltinTableUpdate::row(
            &*MZ_CLUSTER_WORKLOAD_CLASSES,
            Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::from(cluster.config.workload_class.as_deref()),
            ]),
            diff,
        ));

        updates
    }

    pub(super) fn pack_cluster_replica_update(
        &self,
        cluster_id: ClusterId,
        name: &str,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        let cluster = &self.clusters_by_id[&cluster_id];
        let id = cluster.replica_id(name).expect("Must exist");
        let replica = cluster.replica(id).expect("Must exist");

        let (size, disk, az, internal, pending) = match &replica.config.location {
            // TODO(guswynn): The column should be `availability_zones`, not
            // `availability_zone`.
            ReplicaLocation::Managed(ManagedReplicaLocation {
                size,
                availability_zones: ManagedReplicaAvailabilityZones::FromReplica(Some(az)),
                allocation: _,
                billed_as: _,
                internal,
                pending,
            }) => (
                Some(&**size),
                Some(self.cluster_replica_size_has_disk(size)),
                Some(az.as_str()),
                *internal,
                *pending,
            ),
            ReplicaLocation::Managed(ManagedReplicaLocation {
                size,
                availability_zones: _,
                allocation: _,
                billed_as: _,
                internal,
                pending,
            }) => (
                Some(&**size),
                Some(self.cluster_replica_size_has_disk(size)),
                None,
                *internal,
                *pending,
            ),
            _ => (None, None, None, false, false),
        };

        let cluster_replica_update = BuiltinTableUpdate::row(
            &*MZ_CLUSTER_REPLICAS,
            Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::String(name),
                Datum::String(&cluster_id.to_string()),
                Datum::from(size),
                Datum::from(az),
                Datum::String(&replica.owner_id.to_string()),
                Datum::from(disk),
            ]),
            diff,
        );

        let mut updates = vec![cluster_replica_update];

        if internal {
            let update = BuiltinTableUpdate::row(
                &*MZ_INTERNAL_CLUSTER_REPLICAS,
                Row::pack_slice(&[Datum::String(&id.to_string())]),
                diff,
            );
            updates.push(update);
        }

        if pending {
            let update = BuiltinTableUpdate::row(
                &*MZ_PENDING_CLUSTER_REPLICAS,
                Row::pack_slice(&[Datum::String(&id.to_string())]),
                diff,
            );
            updates.push(update);
        }

        updates
    }

    pub(crate) fn pack_network_policy_update(
        &self,
        policy_id: &NetworkPolicyId,
        diff: Diff,
    ) -> Result<Vec<BuiltinTableUpdate<&'static BuiltinTable>>, Error> {
        let policy = self.get_network_policy(policy_id);
        let row = self.pack_privilege_array_row(&policy.privileges);
        let privileges = row.unpack_first();
        let mut updates = Vec::new();
        for ref rule in policy.rules.clone() {
            updates.push(BuiltinTableUpdate::row(
                &*MZ_NETWORK_POLICY_RULES,
                Row::pack_slice(&[
                    Datum::String(&rule.name),
                    Datum::String(&policy.id.to_string()),
                    Datum::String(&rule.action.to_string()),
                    Datum::String(&rule.address.to_string()),
                    Datum::String(&rule.direction.to_string()),
                ]),
                diff,
            ));
        }
        updates.push(BuiltinTableUpdate::row(
            &*MZ_NETWORK_POLICIES,
            Row::pack_slice(&[
                Datum::String(&policy.id.to_string()),
                Datum::String(&policy.name),
                Datum::String(&policy.owner_id.to_string()),
                privileges,
                Datum::UInt32(policy.oid.clone()),
            ]),
            diff,
        ));

        Ok(updates)
    }

    pub(super) fn pack_item_update(
        &self,
        id: CatalogItemId,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        let entry = self.get_entry(&id);
        let oid = entry.oid();
        let conn_id = entry.item().conn_id().unwrap_or(&SYSTEM_CONN_ID);
        let schema_id = &self
            .get_schema(
                &entry.name().qualifiers.database_spec,
                &entry.name().qualifiers.schema_spec,
                conn_id,
            )
            .id;
        let name = &entry.name().item;
        let owner_id = entry.owner_id();
        let privileges_row = self.pack_privilege_array_row(entry.privileges());
        let privileges = privileges_row.unpack_first();
        let mut updates = match entry.item() {
            CatalogItem::Log(_) => self.pack_source_update(
                id, oid, schema_id, name, "log", None, None, None, None, None, owner_id,
                privileges, diff, None,
            ),
            CatalogItem::Index(index) => {
                self.pack_index_update(id, oid, name, owner_id, index, diff)
            }
            CatalogItem::Table(table) => {
                let mut updates = self
                    .pack_table_update(id, oid, schema_id, name, owner_id, privileges, diff, table);

                if let TableDataSource::DataSource {
                    desc: data_source,
                    timeline: _,
                } = &table.data_source
                {
                    updates.extend(match data_source {
                        DataSourceDesc::IngestionExport {
                            ingestion_id,
                            external_reference: UnresolvedItemName(external_reference),
                            details: _,
                            data_config: _,
                        } => {
                            let ingestion_entry = self
                                .get_entry(ingestion_id)
                                .source_desc()
                                .expect("primary source exists")
                                .expect("primary source is a source");

                            match ingestion_entry.connection.name() {
                                "postgres" => {
                                    mz_ore::soft_assert_eq_no_log!(external_reference.len(), 3);
                                    // The left-most qualification of Postgres
                                    // tables is the database, but this
                                    // information is redundant because each
                                    // Postgres connection connects to only one
                                    // database.
                                    let schema_name = external_reference[1].to_ast_string_simple();
                                    let table_name = external_reference[2].to_ast_string_simple();

                                    self.pack_postgres_source_tables_update(
                                        id,
                                        &schema_name,
                                        &table_name,
                                        diff,
                                    )
                                }
                                "mysql" => {
                                    mz_ore::soft_assert_eq_no_log!(external_reference.len(), 2);
                                    let schema_name = external_reference[0].to_ast_string_simple();
                                    let table_name = external_reference[1].to_ast_string_simple();

                                    self.pack_mysql_source_tables_update(
                                        id,
                                        &schema_name,
                                        &table_name,
                                        diff,
                                    )
                                }
                                "sql-server" => {
                                    mz_ore::soft_assert_eq_no_log!(external_reference.len(), 3);
                                    // The left-most qualification of SQL Server tables is
                                    // the database, but this information is redundant
                                    // because each SQL Server connection connects to
                                    // only one database.
                                    let schema_name = external_reference[1].to_ast_string_simple();
                                    let table_name = external_reference[2].to_ast_string_simple();

                                    self.pack_sql_server_source_table_update(
                                        id,
                                        &schema_name,
                                        &table_name,
                                        diff,
                                    )
                                }
                                // Load generator sources don't have any special
                                // updates.
                                "load-generator" => vec![],
                                "kafka" => {
                                    mz_ore::soft_assert_eq_no_log!(external_reference.len(), 1);
                                    let topic = external_reference[0].to_ast_string_simple();
                                    let envelope = data_source.envelope();
                                    let (key_format, value_format) = data_source.formats();

                                    self.pack_kafka_source_tables_update(
                                        id,
                                        &topic,
                                        envelope,
                                        key_format,
                                        value_format,
                                        diff,
                                    )
                                }
                                s => unreachable!("{s} sources do not have tables"),
                            }
                        }
                        _ => vec![],
                    });
                }

                updates
            }
            CatalogItem::Source(source) => {
                let source_type = source.source_type();
                let connection_id = source.connection_id();
                let envelope = source.data_source.envelope();
                let cluster_entry = match source.data_source {
                    // Ingestion exports don't have their own cluster, but
                    // run on their ingestion's cluster.
                    DataSourceDesc::IngestionExport { ingestion_id, .. } => {
                        self.get_entry(&ingestion_id)
                    }
                    _ => entry,
                };

                let cluster_id = cluster_entry.item().cluster_id().map(|id| id.to_string());

                let (key_format, value_format) = source.data_source.formats();

                let mut updates = self.pack_source_update(
                    id,
                    oid,
                    schema_id,
                    name,
                    source_type,
                    connection_id,
                    envelope,
                    key_format,
                    value_format,
                    cluster_id.as_deref(),
                    owner_id,
                    privileges,
                    diff,
                    source.create_sql.as_ref(),
                );

                updates.extend(match &source.data_source {
                    DataSourceDesc::Ingestion { desc, .. }
                    | DataSourceDesc::OldSyntaxIngestion { desc, .. } => match &desc.connection {
                        GenericSourceConnection::Postgres(postgres) => {
                            self.pack_postgres_source_update(id, postgres, diff)
                        }
                        GenericSourceConnection::Kafka(kafka) => {
                            self.pack_kafka_source_update(id, source.global_id(), kafka, diff)
                        }
                        _ => vec![],
                    },
                    DataSourceDesc::IngestionExport {
                        ingestion_id,
                        external_reference: UnresolvedItemName(external_reference),
                        details: _,
                        data_config: _,
                    } => {
                        let ingestion_entry = self
                            .get_entry(ingestion_id)
                            .source_desc()
                            .expect("primary source exists")
                            .expect("primary source is a source");

                        match ingestion_entry.connection.name() {
                            "postgres" => {
                                mz_ore::soft_assert_eq_no_log!(external_reference.len(), 3);
                                // The left-most qualification of Postgres
                                // tables is the database, but this
                                // information is redundant because each
                                // Postgres connection connects to only one
                                // database.
                                let schema_name = external_reference[1].to_ast_string_simple();
                                let table_name = external_reference[2].to_ast_string_simple();

                                self.pack_postgres_source_tables_update(
                                    id,
                                    &schema_name,
                                    &table_name,
                                    diff,
                                )
                            }
                            "mysql" => {
                                mz_ore::soft_assert_eq_no_log!(external_reference.len(), 2);
                                let schema_name = external_reference[0].to_ast_string_simple();
                                let table_name = external_reference[1].to_ast_string_simple();

                                self.pack_mysql_source_tables_update(
                                    id,
                                    &schema_name,
                                    &table_name,
                                    diff,
                                )
                            }
                            "sql-server" => {
                                mz_ore::soft_assert_eq_no_log!(external_reference.len(), 3);
                                // The left-most qualification of SQL Server tables is
                                // the database, but this information is redundant
                                // because each SQL Server connection connects to
                                // only one database.
                                let schema_name = external_reference[1].to_ast_string_simple();
                                let table_name = external_reference[2].to_ast_string_simple();

                                self.pack_sql_server_source_table_update(
                                    id,
                                    &schema_name,
                                    &table_name,
                                    diff,
                                )
                            }
                            // Load generator sources don't have any special
                            // updates.
                            "load-generator" => vec![],
                            s => unreachable!("{s} sources do not have subsources"),
                        }
                    }
                    DataSourceDesc::Webhook { .. } => {
                        vec![self.pack_webhook_source_update(id, diff)]
                    }
                    _ => vec![],
                });

                updates
            }
            CatalogItem::View(view) => {
                self.pack_view_update(id, oid, schema_id, name, owner_id, privileges, view, diff)
            }
            CatalogItem::MaterializedView(mview) => self.pack_materialized_view_update(
                id, oid, schema_id, name, owner_id, privileges, mview, diff,
            ),
            CatalogItem::Sink(sink) => {
                self.pack_sink_update(id, oid, schema_id, name, owner_id, sink, diff)
            }
            CatalogItem::Type(ty) => {
                self.pack_type_update(id, oid, schema_id, name, owner_id, privileges, ty, diff)
            }
            CatalogItem::Func(func) => {
                self.pack_func_update(id, schema_id, name, owner_id, func, diff)
            }
            CatalogItem::Secret(_) => {
                self.pack_secret_update(id, oid, schema_id, name, owner_id, privileges, diff)
            }
            CatalogItem::Connection(connection) => self.pack_connection_update(
                id, oid, schema_id, name, owner_id, privileges, connection, diff,
            ),
            CatalogItem::ContinualTask(ct) => self.pack_continual_task_update(
                id, oid, schema_id, name, owner_id, privileges, ct, diff,
            ),
        };

        if !entry.item().is_temporary() {
            // Populate or clean up the `mz_object_dependencies` table.
            // TODO(jkosh44) Unclear if this table wants to include all uses or only references.
            for dependee in entry.item().references().items() {
                updates.push(self.pack_depends_update(id, *dependee, diff))
            }
        }

        // Always report the latest for an objects columns.
        if let Some(desc) = entry.relation_desc_latest() {
            let defaults = match entry.item() {
                CatalogItem::Table(Table {
                    data_source: TableDataSource::TableWrites { defaults },
                    ..
                }) => Some(defaults),
                _ => None,
            };
            for (i, (column_name, column_type)) in desc.iter().enumerate() {
                let default: Option<String> = defaults.map(|d| d[i].to_ast_string_stable());
                let default: Datum = default
                    .as_ref()
                    .map(|d| Datum::String(d))
                    .unwrap_or(Datum::Null);
                let pgtype = mz_pgrepr::Type::from(&column_type.scalar_type);
                let (type_name, type_oid) = match &column_type.scalar_type {
                    SqlScalarType::List {
                        custom_id: Some(custom_id),
                        ..
                    }
                    | SqlScalarType::Map {
                        custom_id: Some(custom_id),
                        ..
                    }
                    | SqlScalarType::Record {
                        custom_id: Some(custom_id),
                        ..
                    } => {
                        let entry = self.get_entry(custom_id);
                        // NOTE(benesch): the `mz_columns.type text` field is
                        // wrong. Types do not have a name that can be
                        // represented as a single textual field. There can be
                        // multiple types with the same name in different
                        // schemas and databases. We should eventually deprecate
                        // the `type` field in favor of a new `type_id` field
                        // that can be joined against `mz_types`.
                        //
                        // For now, in the interest of pragmatism, we just use
                        // the type's item name, and accept that there may be
                        // ambiguity if the same type name is used in multiple
                        // schemas. The ambiguity is mitigated by the OID, which
                        // can be joined against `mz_types.oid` to resolve the
                        // ambiguity.
                        let name = &*entry.name().item;
                        let oid = entry.oid();
                        (name, oid)
                    }
                    _ => (pgtype.name(), pgtype.oid()),
                };
                updates.push(BuiltinTableUpdate::row(
                    &*MZ_COLUMNS,
                    Row::pack_slice(&[
                        Datum::String(&id.to_string()),
                        Datum::String(column_name),
                        Datum::UInt64(u64::cast_from(i + 1)),
                        Datum::from(column_type.nullable),
                        Datum::String(type_name),
                        default,
                        Datum::UInt32(type_oid),
                        Datum::Int32(pgtype.typmod()),
                    ]),
                    diff,
                ));
            }
        }

        // Use initial lcw so that we can tell apart default from non-existent windows.
        if let Some(cw) = entry.item().initial_logical_compaction_window() {
            updates.push(self.pack_history_retention_strategy_update(id, cw, diff));
        }

        updates.extend(Self::pack_item_global_id_update(entry, diff));

        updates
    }

    fn pack_item_global_id_update(
        entry: &CatalogEntry,
        diff: Diff,
    ) -> impl Iterator<Item = BuiltinTableUpdate<&'static BuiltinTable>> + use<'_> {
        let id = entry.id().to_string();
        let global_ids = entry.global_ids();
        global_ids.map(move |global_id| {
            BuiltinTableUpdate::row(
                &*MZ_OBJECT_GLOBAL_IDS,
                Row::pack_slice(&[Datum::String(&id), Datum::String(&global_id.to_string())]),
                diff,
            )
        })
    }

    fn pack_history_retention_strategy_update(
        &self,
        id: CatalogItemId,
        cw: CompactionWindow,
        diff: Diff,
    ) -> BuiltinTableUpdate<&'static BuiltinTable> {
        let cw: u64 = cw.comparable_timestamp().into();
        let cw = Jsonb::from_serde_json(serde_json::Value::Number(serde_json::Number::from(cw)))
            .expect("must serialize");
        BuiltinTableUpdate::row(
            &*MZ_HISTORY_RETENTION_STRATEGIES,
            Row::pack_slice(&[
                Datum::String(&id.to_string()),
                // FOR is the only strategy at the moment. We may introduce FROM or others later.
                Datum::String("FOR"),
                cw.into_row().into_element(),
            ]),
            diff,
        )
    }

    fn pack_table_update(
        &self,
        id: CatalogItemId,
        oid: u32,
        schema_id: &SchemaSpecifier,
        name: &str,
        owner_id: &RoleId,
        privileges: Datum,
        diff: Diff,
        table: &Table,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        let redacted = table.create_sql.as_ref().map(|create_sql| {
            mz_sql::parse::parse(create_sql)
                .unwrap_or_else(|_| panic!("create_sql cannot be invalid: {}", create_sql))
                .into_element()
                .ast
                .to_ast_string_redacted()
        });
        let source_id = if let TableDataSource::DataSource {
            desc: DataSourceDesc::IngestionExport { ingestion_id, .. },
            ..
        } = &table.data_source
        {
            Some(ingestion_id.to_string())
        } else {
            None
        };

        vec![BuiltinTableUpdate::row(
            &*MZ_TABLES,
            Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::String(&schema_id.to_string()),
                Datum::String(name),
                Datum::String(&owner_id.to_string()),
                privileges,
                if let Some(create_sql) = &table.create_sql {
                    Datum::String(create_sql)
                } else {
                    Datum::Null
                },
                if let Some(redacted) = &redacted {
                    Datum::String(redacted)
                } else {
                    Datum::Null
                },
                if let Some(source_id) = source_id.as_ref() {
                    Datum::String(source_id)
                } else {
                    Datum::Null
                },
            ]),
            diff,
        )]
    }

    fn pack_source_update(
        &self,
        id: CatalogItemId,
        oid: u32,
        schema_id: &SchemaSpecifier,
        name: &str,
        source_desc_name: &str,
        connection_id: Option<CatalogItemId>,
        envelope: Option<&str>,
        key_format: Option<&str>,
        value_format: Option<&str>,
        cluster_id: Option<&str>,
        owner_id: &RoleId,
        privileges: Datum,
        diff: Diff,
        create_sql: Option<&String>,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        let redacted = create_sql.map(|create_sql| {
            let create_stmt = mz_sql::parse::parse(create_sql)
                .unwrap_or_else(|_| panic!("create_sql cannot be invalid: {}", create_sql))
                .into_element()
                .ast;
            create_stmt.to_ast_string_redacted()
        });
        vec![BuiltinTableUpdate::row(
            &*MZ_SOURCES,
            Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::String(&schema_id.to_string()),
                Datum::String(name),
                Datum::String(source_desc_name),
                Datum::from(connection_id.map(|id| id.to_string()).as_deref()),
                // This is the "source size", which is a remnant from linked
                // clusters.
                Datum::Null,
                Datum::from(envelope),
                Datum::from(key_format),
                Datum::from(value_format),
                Datum::from(cluster_id),
                Datum::String(&owner_id.to_string()),
                privileges,
                if let Some(create_sql) = create_sql {
                    Datum::String(create_sql)
                } else {
                    Datum::Null
                },
                if let Some(redacted) = &redacted {
                    Datum::String(redacted)
                } else {
                    Datum::Null
                },
            ]),
            diff,
        )]
    }

    fn pack_postgres_source_update(
        &self,
        id: CatalogItemId,
        postgres: &PostgresSourceConnection<ReferencedConnection>,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        vec![BuiltinTableUpdate::row(
            &*MZ_POSTGRES_SOURCES,
            Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::String(&postgres.publication_details.slot),
                Datum::from(postgres.publication_details.timeline_id),
            ]),
            diff,
        )]
    }

    fn pack_kafka_source_update(
        &self,
        item_id: CatalogItemId,
        collection_id: GlobalId,
        kafka: &KafkaSourceConnection<ReferencedConnection>,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        vec![BuiltinTableUpdate::row(
            &*MZ_KAFKA_SOURCES,
            Row::pack_slice(&[
                Datum::String(&item_id.to_string()),
                Datum::String(&kafka.group_id(&self.config.connection_context, collection_id)),
                Datum::String(&kafka.topic),
            ]),
            diff,
        )]
    }

    fn pack_postgres_source_tables_update(
        &self,
        id: CatalogItemId,
        schema_name: &str,
        table_name: &str,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        vec![BuiltinTableUpdate::row(
            &*MZ_POSTGRES_SOURCE_TABLES,
            Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::String(schema_name),
                Datum::String(table_name),
            ]),
            diff,
        )]
    }

    fn pack_mysql_source_tables_update(
        &self,
        id: CatalogItemId,
        schema_name: &str,
        table_name: &str,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        vec![BuiltinTableUpdate::row(
            &*MZ_MYSQL_SOURCE_TABLES,
            Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::String(schema_name),
                Datum::String(table_name),
            ]),
            diff,
        )]
    }

    fn pack_sql_server_source_table_update(
        &self,
        id: CatalogItemId,
        schema_name: &str,
        table_name: &str,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        vec![BuiltinTableUpdate::row(
            &*MZ_SQL_SERVER_SOURCE_TABLES,
            Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::String(schema_name),
                Datum::String(table_name),
            ]),
            diff,
        )]
    }

    fn pack_kafka_source_tables_update(
        &self,
        id: CatalogItemId,
        topic: &str,
        envelope: Option<&str>,
        key_format: Option<&str>,
        value_format: Option<&str>,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        vec![BuiltinTableUpdate::row(
            &*MZ_KAFKA_SOURCE_TABLES,
            Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::String(topic),
                Datum::from(envelope),
                Datum::from(key_format),
                Datum::from(value_format),
            ]),
            diff,
        )]
    }

    fn pack_connection_update(
        &self,
        id: CatalogItemId,
        oid: u32,
        schema_id: &SchemaSpecifier,
        name: &str,
        owner_id: &RoleId,
        privileges: Datum,
        connection: &Connection,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        let create_stmt = mz_sql::parse::parse(&connection.create_sql)
            .unwrap_or_else(|_| panic!("create_sql cannot be invalid: {}", connection.create_sql))
            .into_element()
            .ast;
        let mut updates = vec![BuiltinTableUpdate::row(
            &*MZ_CONNECTIONS,
            Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::String(&schema_id.to_string()),
                Datum::String(name),
                Datum::String(match connection.details {
                    ConnectionDetails::Kafka { .. } => "kafka",
                    ConnectionDetails::Csr { .. } => "confluent-schema-registry",
                    ConnectionDetails::Postgres { .. } => "postgres",
                    ConnectionDetails::Aws(..) => "aws",
                    ConnectionDetails::AwsPrivatelink(..) => "aws-privatelink",
                    ConnectionDetails::Ssh { .. } => "ssh-tunnel",
                    ConnectionDetails::MySql { .. } => "mysql",
                    ConnectionDetails::SqlServer(_) => "sql-server",
                    ConnectionDetails::IcebergCatalog(_) => "iceberg-catalog",
                }),
                Datum::String(&owner_id.to_string()),
                privileges,
                Datum::String(&connection.create_sql),
                Datum::String(&create_stmt.to_ast_string_redacted()),
            ]),
            diff,
        )];
        match connection.details {
            ConnectionDetails::Kafka(ref kafka) => {
                updates.extend(self.pack_kafka_connection_update(id, kafka, diff));
            }
            ConnectionDetails::Aws(ref aws_config) => {
                match self.pack_aws_connection_update(id, aws_config, diff) {
                    Ok(update) => {
                        updates.push(update);
                    }
                    Err(e) => {
                        tracing::error!(%id, %e, "failed writing row to mz_aws_connections table");
                    }
                }
            }
            ConnectionDetails::AwsPrivatelink(_) => {
                if let Some(aws_principal_context) = self.aws_principal_context.as_ref() {
                    updates.push(self.pack_aws_privatelink_connection_update(
                        id,
                        aws_principal_context,
                        diff,
                    ));
                } else {
                    tracing::error!(%id, "missing AWS principal context; cannot write row to mz_aws_privatelink_connections table");
                }
            }
            ConnectionDetails::Ssh {
                ref key_1,
                ref key_2,
                ..
            } => {
                updates.push(self.pack_ssh_tunnel_connection_update(id, key_1, key_2, diff));
            }
            ConnectionDetails::Csr(_)
            | ConnectionDetails::Postgres(_)
            | ConnectionDetails::MySql(_)
            | ConnectionDetails::SqlServer(_)
            | ConnectionDetails::IcebergCatalog(_) => (),
        };
        updates
    }

    pub(crate) fn pack_ssh_tunnel_connection_update(
        &self,
        id: CatalogItemId,
        key_1: &SshKey,
        key_2: &SshKey,
        diff: Diff,
    ) -> BuiltinTableUpdate<&'static BuiltinTable> {
        BuiltinTableUpdate::row(
            &*MZ_SSH_TUNNEL_CONNECTIONS,
            Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::String(key_1.public_key().as_str()),
                Datum::String(key_2.public_key().as_str()),
            ]),
            diff,
        )
    }

    fn pack_kafka_connection_update(
        &self,
        id: CatalogItemId,
        kafka: &KafkaConnection<ReferencedConnection>,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        let progress_topic = kafka.progress_topic(&self.config.connection_context, id);
        let mut row = Row::default();
        row.packer()
            .try_push_array(
                &[ArrayDimension {
                    lower_bound: 1,
                    length: kafka.brokers.len(),
                }],
                kafka
                    .brokers
                    .iter()
                    .map(|broker| Datum::String(&broker.address)),
            )
            .expect("kafka.brokers is 1 dimensional, and its length is used for the array length");
        let brokers = row.unpack_first();
        vec![BuiltinTableUpdate::row(
            &*MZ_KAFKA_CONNECTIONS,
            Row::pack_slice(&[
                Datum::String(&id.to_string()),
                brokers,
                Datum::String(&progress_topic),
            ]),
            diff,
        )]
    }

    pub fn pack_aws_privatelink_connection_update(
        &self,
        connection_id: CatalogItemId,
        aws_principal_context: &AwsPrincipalContext,
        diff: Diff,
    ) -> BuiltinTableUpdate<&'static BuiltinTable> {
        let id = &MZ_AWS_PRIVATELINK_CONNECTIONS;
        let row = Row::pack_slice(&[
            Datum::String(&connection_id.to_string()),
            Datum::String(&aws_principal_context.to_principal_string(connection_id)),
        ]);
        BuiltinTableUpdate::row(id, row, diff)
    }

    pub fn pack_aws_connection_update(
        &self,
        connection_id: CatalogItemId,
        aws_config: &AwsConnection,
        diff: Diff,
    ) -> Result<BuiltinTableUpdate<&'static BuiltinTable>, anyhow::Error> {
        let id = &MZ_AWS_CONNECTIONS;

        let mut access_key_id = None;
        let mut access_key_id_secret_id = None;
        let mut secret_access_key_secret_id = None;
        let mut session_token = None;
        let mut session_token_secret_id = None;
        let mut assume_role_arn = None;
        let mut assume_role_session_name = None;
        let mut principal = None;
        let mut external_id = None;
        let mut example_trust_policy = None;
        match &aws_config.auth {
            AwsAuth::Credentials(credentials) => {
                match &credentials.access_key_id {
                    StringOrSecret::String(s) => access_key_id = Some(s.as_str()),
                    StringOrSecret::Secret(s) => access_key_id_secret_id = Some(s.to_string()),
                }
                secret_access_key_secret_id = Some(credentials.secret_access_key.to_string());
                match credentials.session_token.as_ref() {
                    None => (),
                    Some(StringOrSecret::String(s)) => session_token = Some(s.as_str()),
                    Some(StringOrSecret::Secret(s)) => {
                        session_token_secret_id = Some(s.to_string())
                    }
                }
            }
            AwsAuth::AssumeRole(assume_role) => {
                assume_role_arn = Some(assume_role.arn.as_str());
                assume_role_session_name = assume_role.session_name.as_deref();
                principal = self
                    .config
                    .connection_context
                    .aws_connection_role_arn
                    .as_deref();
                external_id =
                    Some(assume_role.external_id(&self.config.connection_context, connection_id)?);
                example_trust_policy = {
                    let policy = assume_role
                        .example_trust_policy(&self.config.connection_context, connection_id)?;
                    let policy = Jsonb::from_serde_json(policy).expect("valid json");
                    Some(policy.into_row())
                };
            }
        }

        let row = Row::pack_slice(&[
            Datum::String(&connection_id.to_string()),
            Datum::from(aws_config.endpoint.as_deref()),
            Datum::from(aws_config.region.as_deref()),
            Datum::from(access_key_id),
            Datum::from(access_key_id_secret_id.as_deref()),
            Datum::from(secret_access_key_secret_id.as_deref()),
            Datum::from(session_token),
            Datum::from(session_token_secret_id.as_deref()),
            Datum::from(assume_role_arn),
            Datum::from(assume_role_session_name),
            Datum::from(principal),
            Datum::from(external_id.as_deref()),
            Datum::from(example_trust_policy.as_ref().map(|p| p.into_element())),
        ]);

        Ok(BuiltinTableUpdate::row(id, row, diff))
    }

    fn pack_view_update(
        &self,
        id: CatalogItemId,
        oid: u32,
        schema_id: &SchemaSpecifier,
        name: &str,
        owner_id: &RoleId,
        privileges: Datum,
        view: &View,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        let create_stmt = mz_sql::parse::parse(&view.create_sql)
            .unwrap_or_else(|e| {
                panic!(
                    "create_sql cannot be invalid: `{}` --- error: `{}`",
                    view.create_sql, e
                )
            })
            .into_element()
            .ast;
        let query = match &create_stmt {
            Statement::CreateView(stmt) => &stmt.definition.query,
            _ => unreachable!(),
        };

        let mut query_string = query.to_ast_string_stable();
        // PostgreSQL appends a semicolon in `pg_views.definition`, we
        // do the same for compatibility's sake.
        query_string.push(';');

        vec![BuiltinTableUpdate::row(
            &*MZ_VIEWS,
            Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::String(&schema_id.to_string()),
                Datum::String(name),
                Datum::String(&query_string),
                Datum::String(&owner_id.to_string()),
                privileges,
                Datum::String(&view.create_sql),
                Datum::String(&create_stmt.to_ast_string_redacted()),
            ]),
            diff,
        )]
    }

    fn pack_materialized_view_update(
        &self,
        id: CatalogItemId,
        oid: u32,
        schema_id: &SchemaSpecifier,
        name: &str,
        owner_id: &RoleId,
        privileges: Datum,
        mview: &MaterializedView,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        let create_stmt = mz_sql::parse::parse(&mview.create_sql)
            .unwrap_or_else(|e| {
                panic!(
                    "create_sql cannot be invalid: `{}` --- error: `{}`",
                    mview.create_sql, e
                )
            })
            .into_element()
            .ast;
        let query_string = match &create_stmt {
            Statement::CreateMaterializedView(stmt) => {
                let mut query_string = stmt.query.to_ast_string_stable();
                // PostgreSQL appends a semicolon in `pg_matviews.definition`, we
                // do the same for compatibility's sake.
                query_string.push(';');
                query_string
            }
            _ => unreachable!(),
        };

        let mut updates = Vec::new();

        updates.push(BuiltinTableUpdate::row(
            &*MZ_MATERIALIZED_VIEWS,
            Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::String(&schema_id.to_string()),
                Datum::String(name),
                Datum::String(&mview.cluster_id.to_string()),
                Datum::String(&query_string),
                Datum::String(&owner_id.to_string()),
                privileges,
                Datum::String(&mview.create_sql),
                Datum::String(&create_stmt.to_ast_string_redacted()),
            ]),
            diff,
        ));

        if let Some(refresh_schedule) = &mview.refresh_schedule {
            // This can't be `ON COMMIT`, because that is represented by a `None` instead of an
            // empty `RefreshSchedule`.
            assert!(!refresh_schedule.is_empty());
            for RefreshEvery {
                interval,
                aligned_to,
            } in refresh_schedule.everies.iter()
            {
                let aligned_to_dt = mz_ore::now::to_datetime(
                    <&Timestamp as TryInto<u64>>::try_into(aligned_to).expect("undoes planning"),
                );
                updates.push(BuiltinTableUpdate::row(
                    &*MZ_MATERIALIZED_VIEW_REFRESH_STRATEGIES,
                    Row::pack_slice(&[
                        Datum::String(&id.to_string()),
                        Datum::String("every"),
                        Datum::Interval(
                            Interval::from_duration(interval).expect(
                                "planning ensured that this is convertible back to Interval",
                            ),
                        ),
                        Datum::TimestampTz(aligned_to_dt.try_into().expect("undoes planning")),
                        Datum::Null,
                    ]),
                    diff,
                ));
            }
            for at in refresh_schedule.ats.iter() {
                let at_dt = mz_ore::now::to_datetime(
                    <&Timestamp as TryInto<u64>>::try_into(at).expect("undoes planning"),
                );
                updates.push(BuiltinTableUpdate::row(
                    &*MZ_MATERIALIZED_VIEW_REFRESH_STRATEGIES,
                    Row::pack_slice(&[
                        Datum::String(&id.to_string()),
                        Datum::String("at"),
                        Datum::Null,
                        Datum::Null,
                        Datum::TimestampTz(at_dt.try_into().expect("undoes planning")),
                    ]),
                    diff,
                ));
            }
        } else {
            updates.push(BuiltinTableUpdate::row(
                &*MZ_MATERIALIZED_VIEW_REFRESH_STRATEGIES,
                Row::pack_slice(&[
                    Datum::String(&id.to_string()),
                    Datum::String("on-commit"),
                    Datum::Null,
                    Datum::Null,
                    Datum::Null,
                ]),
                diff,
            ));
        }

        if let Some(target_id) = mview.replacement_target {
            updates.push(BuiltinTableUpdate::row(
                &*MZ_REPLACEMENTS,
                Row::pack_slice(&[
                    Datum::String(&id.to_string()),
                    Datum::String(&target_id.to_string()),
                ]),
                diff,
            ));
        }

        updates
    }

    fn pack_continual_task_update(
        &self,
        id: CatalogItemId,
        oid: u32,
        schema_id: &SchemaSpecifier,
        name: &str,
        owner_id: &RoleId,
        privileges: Datum,
        ct: &ContinualTask,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        let create_stmt = mz_sql::parse::parse(&ct.create_sql)
            .unwrap_or_else(|e| {
                panic!(
                    "create_sql cannot be invalid: `{}` --- error: `{}`",
                    ct.create_sql, e
                )
            })
            .into_element()
            .ast;
        let query_string = match &create_stmt {
            Statement::CreateContinualTask(stmt) => {
                let mut query_string = String::new();
                for stmt in &stmt.stmts {
                    let s = match stmt {
                        ContinualTaskStmt::Insert(stmt) => stmt.to_ast_string_stable(),
                        ContinualTaskStmt::Delete(stmt) => stmt.to_ast_string_stable(),
                    };
                    if query_string.is_empty() {
                        query_string = s;
                    } else {
                        query_string.push_str("; ");
                        query_string.push_str(&s);
                    }
                }
                query_string
            }
            _ => unreachable!(),
        };

        vec![BuiltinTableUpdate::row(
            &*MZ_CONTINUAL_TASKS,
            Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::String(&schema_id.to_string()),
                Datum::String(name),
                Datum::String(&ct.cluster_id.to_string()),
                Datum::String(&query_string),
                Datum::String(&owner_id.to_string()),
                privileges,
                Datum::String(&ct.create_sql),
                Datum::String(&create_stmt.to_ast_string_redacted()),
            ]),
            diff,
        )]
    }

    fn pack_sink_update(
        &self,
        id: CatalogItemId,
        oid: u32,
        schema_id: &SchemaSpecifier,
        name: &str,
        owner_id: &RoleId,
        sink: &Sink,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        let mut updates = vec![];
        match &sink.connection {
            StorageSinkConnection::Kafka(KafkaSinkConnection {
                topic: topic_name, ..
            }) => {
                updates.push(BuiltinTableUpdate::row(
                    &*MZ_KAFKA_SINKS,
                    Row::pack_slice(&[
                        Datum::String(&id.to_string()),
                        Datum::String(topic_name.as_str()),
                    ]),
                    diff,
                ));
            }
            StorageSinkConnection::Iceberg(IcebergSinkConnection {
                namespace, table, ..
            }) => {
                updates.push(BuiltinTableUpdate::row(
                    &*MZ_ICEBERG_SINKS,
                    Row::pack_slice(&[
                        Datum::String(&id.to_string()),
                        Datum::String(namespace.as_str()),
                        Datum::String(table.as_str()),
                    ]),
                    diff,
                ));
            }
        };

        let create_stmt = mz_sql::parse::parse(&sink.create_sql)
            .unwrap_or_else(|_| panic!("create_sql cannot be invalid: {}", sink.create_sql))
            .into_element()
            .ast;

        let envelope = sink.envelope();

        // The combined format string is used for the deprecated `format` column.
        let combined_format = sink.combined_format();
        let (key_format, value_format) = match sink.formats() {
            Some((key_format, value_format)) => (key_format, Some(value_format)),
            None => (None, None),
        };

        updates.push(BuiltinTableUpdate::row(
            &*MZ_SINKS,
            Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::String(&schema_id.to_string()),
                Datum::String(name),
                Datum::String(sink.connection.name()),
                Datum::from(sink.connection_id().map(|id| id.to_string()).as_deref()),
                // size column now deprecated w/o linked clusters
                Datum::Null,
                Datum::from(envelope),
                // FIXME: These key/value formats are kinda leaky! Should probably live in
                // the kafka sink table.
                Datum::from(combined_format.as_ref().map(|f| f.as_ref())),
                Datum::from(key_format),
                Datum::from(value_format),
                Datum::String(&sink.cluster_id.to_string()),
                Datum::String(&owner_id.to_string()),
                Datum::String(&sink.create_sql),
                Datum::String(&create_stmt.to_ast_string_redacted()),
            ]),
            diff,
        ));

        updates
    }

    fn pack_index_update(
        &self,
        id: CatalogItemId,
        oid: u32,
        name: &str,
        owner_id: &RoleId,
        index: &Index,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        let mut updates = vec![];

        let create_stmt = mz_sql::parse::parse(&index.create_sql)
            .unwrap_or_else(|e| {
                panic!(
                    "create_sql cannot be invalid: `{}` --- error: `{}`",
                    index.create_sql, e
                )
            })
            .into_element()
            .ast;

        let key_sqls = match &create_stmt {
            Statement::CreateIndex(CreateIndexStatement { key_parts, .. }) => key_parts
                .as_ref()
                .expect("key_parts is filled in during planning"),
            _ => unreachable!(),
        };
        let on_item_id = self.get_entry_by_global_id(&index.on).id();

        updates.push(BuiltinTableUpdate::row(
            &*MZ_INDEXES,
            Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::String(name),
                Datum::String(&on_item_id.to_string()),
                Datum::String(&index.cluster_id.to_string()),
                Datum::String(&owner_id.to_string()),
                Datum::String(&index.create_sql),
                Datum::String(&create_stmt.to_ast_string_redacted()),
            ]),
            diff,
        ));

        for (i, key) in index.keys.iter().enumerate() {
            let on_entry = self.get_entry_by_global_id(&index.on);
            let on_desc = on_entry
                .relation_desc()
                .expect("can only create indexes on items with a valid description");
            let nullable = key.typ(&on_desc.typ().column_types).nullable;
            let seq_in_index = u64::cast_from(i + 1);
            let key_sql = key_sqls
                .get(i)
                .expect("missing sql information for index key")
                .to_ast_string_simple();
            let (field_number, expression) = match key {
                MirScalarExpr::Column(col, _) => {
                    (Datum::UInt64(u64::cast_from(*col + 1)), Datum::Null)
                }
                _ => (Datum::Null, Datum::String(&key_sql)),
            };
            updates.push(BuiltinTableUpdate::row(
                &*MZ_INDEX_COLUMNS,
                Row::pack_slice(&[
                    Datum::String(&id.to_string()),
                    Datum::UInt64(seq_in_index),
                    field_number,
                    expression,
                    Datum::from(nullable),
                ]),
                diff,
            ));
        }

        updates
    }

    fn pack_type_update(
        &self,
        id: CatalogItemId,
        oid: u32,
        schema_id: &SchemaSpecifier,
        name: &str,
        owner_id: &RoleId,
        privileges: Datum,
        typ: &Type,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        let mut out = vec![];

        let redacted = typ.create_sql.as_ref().map(|create_sql| {
            mz_sql::parse::parse(create_sql)
                .unwrap_or_else(|_| panic!("create_sql cannot be invalid: {}", create_sql))
                .into_element()
                .ast
                .to_ast_string_redacted()
        });

        out.push(BuiltinTableUpdate::row(
            &*MZ_TYPES,
            Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::String(&schema_id.to_string()),
                Datum::String(name),
                Datum::String(&TypeCategory::from_catalog_type(&typ.details.typ).to_string()),
                Datum::String(&owner_id.to_string()),
                privileges,
                if let Some(create_sql) = &typ.create_sql {
                    Datum::String(create_sql)
                } else {
                    Datum::Null
                },
                if let Some(redacted) = &redacted {
                    Datum::String(redacted)
                } else {
                    Datum::Null
                },
            ]),
            diff,
        ));

        let mut row = Row::default();
        let mut packer = row.packer();

        fn append_modifier(packer: &mut RowPacker<'_>, mods: &[i64]) {
            if mods.is_empty() {
                packer.push(Datum::Null);
            } else {
                packer.push_list(mods.iter().map(|m| Datum::Int64(*m)));
            }
        }

        let index_id = match &typ.details.typ {
            CatalogType::Array {
                element_reference: element_id,
            } => {
                packer.push(Datum::String(&id.to_string()));
                packer.push(Datum::String(&element_id.to_string()));
                &MZ_ARRAY_TYPES
            }
            CatalogType::List {
                element_reference: element_id,
                element_modifiers,
            } => {
                packer.push(Datum::String(&id.to_string()));
                packer.push(Datum::String(&element_id.to_string()));
                append_modifier(&mut packer, element_modifiers);
                &MZ_LIST_TYPES
            }
            CatalogType::Map {
                key_reference: key_id,
                value_reference: value_id,
                key_modifiers,
                value_modifiers,
            } => {
                packer.push(Datum::String(&id.to_string()));
                packer.push(Datum::String(&key_id.to_string()));
                packer.push(Datum::String(&value_id.to_string()));
                append_modifier(&mut packer, key_modifiers);
                append_modifier(&mut packer, value_modifiers);
                &MZ_MAP_TYPES
            }
            CatalogType::Pseudo => {
                packer.push(Datum::String(&id.to_string()));
                &MZ_PSEUDO_TYPES
            }
            _ => {
                packer.push(Datum::String(&id.to_string()));
                &MZ_BASE_TYPES
            }
        };
        out.push(BuiltinTableUpdate::row(index_id, row, diff));

        if let Some(pg_metadata) = &typ.details.pg_metadata {
            out.push(BuiltinTableUpdate::row(
                &*MZ_TYPE_PG_METADATA,
                Row::pack_slice(&[
                    Datum::String(&id.to_string()),
                    Datum::UInt32(pg_metadata.typinput_oid),
                    Datum::UInt32(pg_metadata.typreceive_oid),
                ]),
                diff,
            ));
        }

        out
    }

    fn pack_func_update(
        &self,
        id: CatalogItemId,
        schema_id: &SchemaSpecifier,
        name: &str,
        owner_id: &RoleId,
        func: &Func,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        let mut updates = vec![];
        for func_impl_details in func.inner.func_impls() {
            let arg_type_ids = func_impl_details
                .arg_typs
                .iter()
                .map(|typ| self.get_system_type(typ).id().to_string())
                .collect::<Vec<_>>();

            let mut row = Row::default();
            row.packer()
                .try_push_array(
                    &[ArrayDimension {
                        lower_bound: 1,
                        length: arg_type_ids.len(),
                    }],
                    arg_type_ids.iter().map(|id| Datum::String(id)),
                )
                .expect(
                    "arg_type_ids is 1 dimensional, and its length is used for the array length",
                );
            let arg_type_ids = row.unpack_first();

            updates.push(BuiltinTableUpdate::row(
                &*MZ_FUNCTIONS,
                Row::pack_slice(&[
                    Datum::String(&id.to_string()),
                    Datum::UInt32(func_impl_details.oid),
                    Datum::String(&schema_id.to_string()),
                    Datum::String(name),
                    arg_type_ids,
                    Datum::from(
                        func_impl_details
                            .variadic_typ
                            .map(|typ| self.get_system_type(typ).id().to_string())
                            .as_deref(),
                    ),
                    Datum::from(
                        func_impl_details
                            .return_typ
                            .map(|typ| self.get_system_type(typ).id().to_string())
                            .as_deref(),
                    ),
                    func_impl_details.return_is_set.into(),
                    Datum::String(&owner_id.to_string()),
                ]),
                diff,
            ));

            if let mz_sql::func::Func::Aggregate(_) = func.inner {
                updates.push(BuiltinTableUpdate::row(
                    &*MZ_AGGREGATES,
                    Row::pack_slice(&[
                        Datum::UInt32(func_impl_details.oid),
                        // TODO(database-issues#1064): Support ordered-set aggregate functions.
                        Datum::String("n"),
                        Datum::Int16(0),
                    ]),
                    diff,
                ));
            }
        }
        updates
    }

    pub fn pack_op_update(
        &self,
        operator: &str,
        func_impl_details: FuncImplCatalogDetails,
        diff: Diff,
    ) -> BuiltinTableUpdate<&'static BuiltinTable> {
        let arg_type_ids = func_impl_details
            .arg_typs
            .iter()
            .map(|typ| self.get_system_type(typ).id().to_string())
            .collect::<Vec<_>>();

        let mut row = Row::default();
        row.packer()
            .try_push_array(
                &[ArrayDimension {
                    lower_bound: 1,
                    length: arg_type_ids.len(),
                }],
                arg_type_ids.iter().map(|id| Datum::String(id)),
            )
            .expect("arg_type_ids is 1 dimensional, and its length is used for the array length");
        let arg_type_ids = row.unpack_first();

        BuiltinTableUpdate::row(
            &*MZ_OPERATORS,
            Row::pack_slice(&[
                Datum::UInt32(func_impl_details.oid),
                Datum::String(operator),
                arg_type_ids,
                Datum::from(
                    func_impl_details
                        .return_typ
                        .map(|typ| self.get_system_type(typ).id().to_string())
                        .as_deref(),
                ),
            ]),
            diff,
        )
    }

    fn pack_secret_update(
        &self,
        id: CatalogItemId,
        oid: u32,
        schema_id: &SchemaSpecifier,
        name: &str,
        owner_id: &RoleId,
        privileges: Datum,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        vec![BuiltinTableUpdate::row(
            &*MZ_SECRETS,
            Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::String(&schema_id.to_string()),
                Datum::String(name),
                Datum::String(&owner_id.to_string()),
                privileges,
            ]),
            diff,
        )]
    }

    pub fn pack_audit_log_update(
        &self,
        event: &VersionedEvent,
        diff: Diff,
    ) -> Result<BuiltinTableUpdate<&'static BuiltinTable>, Error> {
        let (event_type, object_type, details, user, occurred_at): (
            &EventType,
            &ObjectType,
            &EventDetails,
            &Option<String>,
            u64,
        ) = match event {
            VersionedEvent::V1(ev) => (
                &ev.event_type,
                &ev.object_type,
                &ev.details,
                &ev.user,
                ev.occurred_at,
            ),
        };
        let details = Jsonb::from_serde_json(details.as_json())
            .map_err(|e| {
                Error::new(ErrorKind::Unstructured(format!(
                    "could not pack audit log update: {}",
                    e
                )))
            })?
            .into_row();
        let details = details
            .iter()
            .next()
            .expect("details created above with a single jsonb column");
        let dt = mz_ore::now::to_datetime(occurred_at);
        let id = event.sortable_id();
        Ok(BuiltinTableUpdate::row(
            &*MZ_AUDIT_EVENTS,
            Row::pack_slice(&[
                Datum::UInt64(id),
                Datum::String(&format!("{}", event_type)),
                Datum::String(&format!("{}", object_type)),
                details,
                match user {
                    Some(user) => Datum::String(user),
                    None => Datum::Null,
                },
                Datum::TimestampTz(dt.try_into().expect("must fit")),
            ]),
            diff,
        ))
    }

    pub fn pack_storage_usage_update(
        &self,
        VersionedStorageUsage::V1(event): VersionedStorageUsage,
        diff: Diff,
    ) -> BuiltinTableUpdate<&'static BuiltinTable> {
        let id = &MZ_STORAGE_USAGE_BY_SHARD;
        let row = Row::pack_slice(&[
            Datum::UInt64(event.id),
            Datum::from(event.shard_id.as_deref()),
            Datum::UInt64(event.size_bytes),
            Datum::TimestampTz(
                mz_ore::now::to_datetime(event.collection_timestamp)
                    .try_into()
                    .expect("must fit"),
            ),
        ]);
        BuiltinTableUpdate::row(id, row, diff)
    }

    pub fn pack_egress_ip_update(
        &self,
        ip: &IpNet,
    ) -> Result<BuiltinTableUpdate<&'static BuiltinTable>, Error> {
        let id = &MZ_EGRESS_IPS;
        let addr = ip.addr();
        let row = Row::pack_slice(&[
            Datum::String(&addr.to_string()),
            Datum::Int32(ip.prefix_len().into()),
            Datum::String(&format!("{}/{}", addr, ip.prefix_len())),
        ]);
        Ok(BuiltinTableUpdate::row(id, row, Diff::ONE))
    }

    pub fn pack_license_key_update(
        &self,
        license_key: &ValidatedLicenseKey,
    ) -> Result<BuiltinTableUpdate<&'static BuiltinTable>, Error> {
        let id = &MZ_LICENSE_KEYS;
        let row = Row::pack_slice(&[
            Datum::String(&license_key.id),
            Datum::String(&license_key.organization),
            Datum::String(&license_key.environment_id),
            Datum::TimestampTz(
                mz_ore::now::to_datetime(license_key.expiration * 1000)
                    .try_into()
                    .expect("must fit"),
            ),
            Datum::TimestampTz(
                mz_ore::now::to_datetime(license_key.not_before * 1000)
                    .try_into()
                    .expect("must fit"),
            ),
        ]);
        Ok(BuiltinTableUpdate::row(id, row, Diff::ONE))
    }

    pub fn pack_all_replica_size_updates(&self) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        let mut updates = Vec::new();
        for (size, alloc) in &self.cluster_replica_sizes.0 {
            if alloc.disabled {
                continue;
            }

            // Just invent something when the limits are `None`, which only happens in non-prod
            // environments (tests, process orchestrator, etc.)
            let cpu_limit = alloc.cpu_limit.unwrap_or(CpuLimit::MAX);
            let MemoryLimit(ByteSize(memory_bytes)) =
                (alloc.memory_limit).unwrap_or(MemoryLimit::MAX);
            let DiskLimit(ByteSize(disk_bytes)) =
                (alloc.disk_limit).unwrap_or(DiskLimit::ARBITRARY);

            let row = Row::pack_slice(&[
                size.as_str().into(),
                u64::cast_from(alloc.scale).into(),
                u64::cast_from(alloc.workers).into(),
                cpu_limit.as_nanocpus().into(),
                memory_bytes.into(),
                disk_bytes.into(),
                (alloc.credits_per_hour).into(),
            ]);

            updates.push(BuiltinTableUpdate::row(
                &*MZ_CLUSTER_REPLICA_SIZES,
                row,
                Diff::ONE,
            ));
        }

        updates
    }

    pub fn pack_subscribe_update(
        &self,
        id: GlobalId,
        subscribe: &ActiveSubscribe,
        diff: Diff,
    ) -> BuiltinTableUpdate<&'static BuiltinTable> {
        let mut row = Row::default();
        let mut packer = row.packer();
        packer.push(Datum::String(&id.to_string()));
        packer.push(Datum::Uuid(subscribe.session_uuid));
        packer.push(Datum::String(&subscribe.cluster_id.to_string()));

        let start_dt = mz_ore::now::to_datetime(subscribe.start_time);
        packer.push(Datum::TimestampTz(start_dt.try_into().expect("must fit")));

        let depends_on: Vec<_> = subscribe
            .depends_on
            .iter()
            .map(|id| id.to_string())
            .collect();
        packer.push_list(depends_on.iter().map(|s| Datum::String(s)));

        BuiltinTableUpdate::row(&*MZ_SUBSCRIPTIONS, row, diff)
    }

    pub fn pack_session_update(
        &self,
        conn: &ConnMeta,
        diff: Diff,
    ) -> BuiltinTableUpdate<&'static BuiltinTable> {
        let connect_dt = mz_ore::now::to_datetime(conn.connected_at());
        BuiltinTableUpdate::row(
            &*MZ_SESSIONS,
            Row::pack_slice(&[
                Datum::Uuid(conn.uuid()),
                Datum::UInt32(conn.conn_id().unhandled()),
                Datum::String(&conn.authenticated_role_id().to_string()),
                Datum::from(conn.client_ip().map(|ip| ip.to_string()).as_deref()),
                Datum::TimestampTz(connect_dt.try_into().expect("must fit")),
            ]),
            diff,
        )
    }

    pub fn pack_default_privileges_update(
        &self,
        default_privilege_object: &DefaultPrivilegeObject,
        grantee: &RoleId,
        acl_mode: &AclMode,
        diff: Diff,
    ) -> BuiltinTableUpdate<&'static BuiltinTable> {
        BuiltinTableUpdate::row(
            &*MZ_DEFAULT_PRIVILEGES,
            Row::pack_slice(&[
                default_privilege_object.role_id.to_string().as_str().into(),
                default_privilege_object
                    .database_id
                    .map(|database_id| database_id.to_string())
                    .as_deref()
                    .into(),
                default_privilege_object
                    .schema_id
                    .map(|schema_id| schema_id.to_string())
                    .as_deref()
                    .into(),
                default_privilege_object
                    .object_type
                    .to_string()
                    .to_lowercase()
                    .as_str()
                    .into(),
                grantee.to_string().as_str().into(),
                acl_mode.to_string().as_str().into(),
            ]),
            diff,
        )
    }

    pub fn pack_system_privileges_update(
        &self,
        privileges: MzAclItem,
        diff: Diff,
    ) -> BuiltinTableUpdate<&'static BuiltinTable> {
        BuiltinTableUpdate::row(
            &*MZ_SYSTEM_PRIVILEGES,
            Row::pack_slice(&[privileges.into()]),
            diff,
        )
    }

    fn pack_privilege_array_row(&self, privileges: &PrivilegeMap) -> Row {
        let mut row = Row::default();
        let flat_privileges: Vec<_> = privileges.all_values_owned().collect();
        row.packer()
            .try_push_array(
                &[ArrayDimension {
                    lower_bound: 1,
                    length: flat_privileges.len(),
                }],
                flat_privileges
                    .into_iter()
                    .map(|mz_acl_item| Datum::MzAclItem(mz_acl_item.clone())),
            )
            .expect("privileges is 1 dimensional, and its length is used for the array length");
        row
    }

    pub fn pack_comment_update(
        &self,
        object_id: CommentObjectId,
        column_pos: Option<usize>,
        comment: &str,
        diff: Diff,
    ) -> BuiltinTableUpdate<&'static BuiltinTable> {
        // Use the audit log representation so it's easier to join against.
        let object_type = mz_sql::catalog::ObjectType::from(object_id);
        let audit_type = super::object_type_to_audit_object_type(object_type);
        let object_type_str = audit_type.to_string();

        let object_id_str = match object_id {
            CommentObjectId::Table(global_id)
            | CommentObjectId::View(global_id)
            | CommentObjectId::MaterializedView(global_id)
            | CommentObjectId::Source(global_id)
            | CommentObjectId::Sink(global_id)
            | CommentObjectId::Index(global_id)
            | CommentObjectId::Func(global_id)
            | CommentObjectId::Connection(global_id)
            | CommentObjectId::Secret(global_id)
            | CommentObjectId::Type(global_id)
            | CommentObjectId::ContinualTask(global_id) => global_id.to_string(),
            CommentObjectId::Role(role_id) => role_id.to_string(),
            CommentObjectId::Database(database_id) => database_id.to_string(),
            CommentObjectId::Schema((_, schema_id)) => schema_id.to_string(),
            CommentObjectId::Cluster(cluster_id) => cluster_id.to_string(),
            CommentObjectId::ClusterReplica((_, replica_id)) => replica_id.to_string(),
            CommentObjectId::NetworkPolicy(network_policy_id) => network_policy_id.to_string(),
        };
        let column_pos_datum = match column_pos {
            Some(pos) => {
                // TODO(parkmycar): https://github.com/MaterializeInc/database-issues/issues/6711.
                let pos =
                    i32::try_from(pos).expect("we constrain this value in the planning layer");
                Datum::Int32(pos)
            }
            None => Datum::Null,
        };

        BuiltinTableUpdate::row(
            &*MZ_COMMENTS,
            Row::pack_slice(&[
                Datum::String(&object_id_str),
                Datum::String(&object_type_str),
                column_pos_datum,
                Datum::String(comment),
            ]),
            diff,
        )
    }

    pub fn pack_webhook_source_update(
        &self,
        item_id: CatalogItemId,
        diff: Diff,
    ) -> BuiltinTableUpdate<&'static BuiltinTable> {
        let url = self
            .try_get_webhook_url(&item_id)
            .expect("webhook source should exist");
        let url = url.to_string();
        let name = &self.get_entry(&item_id).name().item;
        let id_str = item_id.to_string();

        BuiltinTableUpdate::row(
            &*MZ_WEBHOOKS_SOURCES,
            Row::pack_slice(&[
                Datum::String(&id_str),
                Datum::String(name),
                Datum::String(&url),
            ]),
            diff,
        )
    }

    pub fn pack_source_references_update(
        &self,
        source_references: &SourceReferences,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        let source_id = source_references.source_id.to_string();
        let updated_at = &source_references.updated_at;
        source_references
            .references
            .iter()
            .map(|reference| {
                let mut row = Row::default();
                let mut packer = row.packer();
                packer.extend([
                    Datum::String(&source_id),
                    reference
                        .namespace
                        .as_ref()
                        .map(|s| Datum::String(s))
                        .unwrap_or(Datum::Null),
                    Datum::String(&reference.name),
                    Datum::TimestampTz(
                        mz_ore::now::to_datetime(*updated_at)
                            .try_into()
                            .expect("must fit"),
                    ),
                ]);
                if reference.columns.len() > 0 {
                    packer
                        .try_push_array(
                            &[ArrayDimension {
                                lower_bound: 1,
                                length: reference.columns.len(),
                            }],
                            reference.columns.iter().map(|col| Datum::String(col)),
                        )
                        .expect(
                            "columns is 1 dimensional, and its length is used for the array length",
                        );
                } else {
                    packer.push(Datum::Null);
                }

                BuiltinTableUpdate::row(&*MZ_SOURCE_REFERENCES, row, diff)
            })
            .collect()
    }
}
