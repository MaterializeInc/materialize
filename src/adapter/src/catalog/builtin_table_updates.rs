// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::Ipv4Addr;

use bytesize::ByteSize;
use chrono::{DateTime, Utc};
use mz_audit_log::{EventDetails, EventType, ObjectType, VersionedEvent, VersionedStorageUsage};
use mz_compute_client::controller::NewReplicaId;
use mz_controller::clusters::{
    ClusterId, ClusterStatus, ManagedReplicaLocation, ProcessId, ReplicaAllocation, ReplicaId,
    ReplicaLocation,
};
use mz_expr::MirScalarExpr;
use mz_orchestrator::{CpuLimit, MemoryLimit, NotReadyReason, ServiceProcessMetrics};
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_repr::adt::array::ArrayDimension;
use mz_repr::adt::jsonb::Jsonb;
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem, PrivilegeMap};
use mz_repr::role_id::RoleId;
use mz_repr::{Datum, Diff, GlobalId, Row};
use mz_sql::ast::{CreateIndexStatement, Statement};
use mz_sql::catalog::{CatalogCluster, CatalogDatabase, CatalogSchema, CatalogType, TypeCategory};
use mz_sql::func::FuncImplCatalogDetails;
use mz_sql::names::{ResolvedDatabaseSpecifier, SchemaId, SchemaSpecifier};
use mz_sql_parser::ast::display::AstDisplay;
use mz_storage_client::types::connections::KafkaConnection;
use mz_storage_client::types::sinks::{KafkaSinkConnection, StorageSinkConnection};
use mz_storage_client::types::sources::{
    GenericSourceConnection, KafkaSourceConnection, PostgresSourceConnection,
};

use crate::catalog::builtin::{
    MZ_AGGREGATES, MZ_ARRAY_TYPES, MZ_AUDIT_EVENTS, MZ_AWS_PRIVATELINK_CONNECTIONS, MZ_BASE_TYPES,
    MZ_CLUSTERS, MZ_CLUSTER_LINKS, MZ_CLUSTER_REPLICAS, MZ_CLUSTER_REPLICA_FRONTIERS,
    MZ_CLUSTER_REPLICA_HEARTBEATS, MZ_CLUSTER_REPLICA_METRICS, MZ_CLUSTER_REPLICA_SIZES,
    MZ_CLUSTER_REPLICA_STATUSES, MZ_COLUMNS, MZ_CONNECTIONS, MZ_DATABASES, MZ_DEFAULT_PRIVILEGES,
    MZ_EGRESS_IPS, MZ_FUNCTIONS, MZ_INDEXES, MZ_INDEX_COLUMNS, MZ_KAFKA_CONNECTIONS,
    MZ_KAFKA_SINKS, MZ_KAFKA_SOURCES, MZ_LIST_TYPES, MZ_MAP_TYPES, MZ_MATERIALIZED_VIEWS,
    MZ_OBJECT_DEPENDENCIES, MZ_OPERATORS, MZ_POSTGRES_SOURCES, MZ_PSEUDO_TYPES, MZ_ROLES,
    MZ_ROLE_MEMBERS, MZ_SCHEMAS, MZ_SECRETS, MZ_SESSIONS, MZ_SINKS, MZ_SOURCES,
    MZ_SSH_TUNNEL_CONNECTIONS, MZ_STORAGE_USAGE_BY_SHARD, MZ_SUBSCRIPTIONS, MZ_TABLES, MZ_TYPES,
    MZ_VIEWS,
};
use crate::catalog::{
    AwsPrincipalContext, CatalogItem, CatalogState, ClusterVariant, Connection, DataSourceDesc,
    Database, DefaultPrivilegeObject, Error, ErrorKind, Func, Index, MaterializedView, Sink,
    StorageSinkConnectionState, Type, View, SYSTEM_CONN_ID,
};
use crate::session::Session;
use crate::subscribe::ActiveSubscribe;

/// An update to a built-in table.
#[derive(Debug)]
pub struct BuiltinTableUpdate {
    /// The ID of the table to update.
    pub id: GlobalId,
    /// The data to put into the table.
    pub row: Row,
    /// The diff of the data.
    pub diff: Diff,
}

impl CatalogState {
    pub fn pack_depends_update(
        &self,
        depender: GlobalId,
        dependee: GlobalId,
        diff: Diff,
    ) -> BuiltinTableUpdate {
        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_OBJECT_DEPENDENCIES),
            row: Row::pack_slice(&[
                Datum::String(&depender.to_string()),
                Datum::String(&dependee.to_string()),
            ]),
            diff,
        }
    }

    pub(super) fn pack_database_update(
        &self,
        database: &Database,
        diff: Diff,
    ) -> BuiltinTableUpdate {
        let row = self.pack_privilege_array_row(database.privileges());
        let privileges = row.unpack_first();
        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_DATABASES),
            row: Row::pack_slice(&[
                Datum::String(&database.id.to_string()),
                Datum::UInt32(database.oid),
                Datum::String(database.name()),
                Datum::String(&database.owner_id.to_string()),
                privileges,
            ]),
            diff,
        }
    }

    pub(super) fn pack_schema_update(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_id: &SchemaId,
        diff: Diff,
    ) -> BuiltinTableUpdate {
        let (database_id, schema) = match database_spec {
            ResolvedDatabaseSpecifier::Ambient => (None, &self.ambient_schemas_by_id[schema_id]),
            ResolvedDatabaseSpecifier::Id(id) => (
                Some(id.to_string()),
                &self.database_by_id[id].schemas_by_id[schema_id],
            ),
        };
        let row = self.pack_privilege_array_row(schema.privileges());
        let privileges = row.unpack_first();
        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_SCHEMAS),
            row: Row::pack_slice(&[
                Datum::String(&schema_id.to_string()),
                Datum::UInt32(schema.oid),
                Datum::from(database_id.as_deref()),
                Datum::String(&schema.name.schema),
                Datum::String(&schema.owner_id.to_string()),
                privileges,
            ]),
            diff,
        }
    }

    pub(super) fn pack_role_update(&self, id: RoleId, diff: Diff) -> Option<BuiltinTableUpdate> {
        match id {
            // PUBLIC role should not show up in mz_roles.
            RoleId::Public => None,
            id => {
                let role = self.get_role(&id);
                Some(BuiltinTableUpdate {
                    id: self.resolve_builtin_table(&MZ_ROLES),
                    row: Row::pack_slice(&[
                        Datum::String(&role.id.to_string()),
                        Datum::UInt32(role.oid),
                        Datum::String(&role.name),
                        Datum::from(role.attributes.inherit),
                        Datum::from(role.attributes.create_role),
                        Datum::from(role.attributes.create_db),
                        Datum::from(role.attributes.create_cluster),
                    ]),
                    diff,
                })
            }
        }
    }

    pub(super) fn pack_role_members_update(
        &self,
        role_id: RoleId,
        member_id: RoleId,
        diff: Diff,
    ) -> BuiltinTableUpdate {
        let grantor_id = self
            .get_role(&member_id)
            .membership
            .map
            .get(&role_id)
            .expect("catalog out of sync");
        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_ROLE_MEMBERS),
            row: Row::pack_slice(&[
                Datum::String(&role_id.to_string()),
                Datum::String(&member_id.to_string()),
                Datum::String(&grantor_id.to_string()),
            ]),
            diff,
        }
    }

    pub(super) fn pack_cluster_update(&self, name: &str, diff: Diff) -> BuiltinTableUpdate {
        let id = self.clusters_by_name[name];
        let cluster = &self.clusters_by_id[&id];
        let row = self.pack_privilege_array_row(cluster.privileges());
        let privileges = row.unpack_first();
        let (size, replication_factor) = match &cluster.config.variant {
            ClusterVariant::Managed(config) => {
                (Some(config.size.as_str()), Some(config.replication_factor))
            }
            ClusterVariant::Unmanaged => (None, None),
        };
        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_CLUSTERS),
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::String(name),
                Datum::String(&cluster.owner_id.to_string()),
                privileges,
                cluster.is_managed().into(),
                size.into(),
                replication_factor.into(),
            ]),
            diff,
        }
    }

    pub(super) fn pack_cluster_replica_update(
        &self,
        cluster_id: ClusterId,
        name: &str,
        diff: Diff,
    ) -> BuiltinTableUpdate {
        let cluster = &self.clusters_by_id[&cluster_id];
        let id = cluster.replica_id_by_name[name];
        let replica = &cluster.replicas_by_id[&id];

        let (size, az) = match &replica.config.location {
            ReplicaLocation::Managed(ManagedReplicaLocation {
                size,
                availability_zone,
                az_user_specified: _,
                allocation: _,
            }) => (Some(&**size), Some(availability_zone.as_str())),
            ReplicaLocation::Unmanaged(_) => (None, None),
        };

        // TODO(#18377): Make replica IDs `NewReplicaId`s throughout the code.
        let id = NewReplicaId::User(id);

        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_CLUSTER_REPLICAS),
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::String(name),
                Datum::String(&cluster_id.to_string()),
                Datum::from(size),
                Datum::from(az),
                Datum::String(&replica.owner_id.to_string()),
            ]),
            diff,
        }
    }

    pub(super) fn pack_cluster_link_update(
        &self,
        cluster_name: &str,
        object_id: GlobalId,
        diff: Diff,
    ) -> BuiltinTableUpdate {
        let cluster_id = self.clusters_by_name[cluster_name];
        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_CLUSTER_LINKS),
            row: Row::pack_slice(&[
                Datum::String(&cluster_id.to_string()),
                Datum::String(&object_id.to_string()),
            ]),
            diff,
        }
    }

    pub(super) fn pack_cluster_replica_status_update(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        process_id: ProcessId,
        diff: Diff,
    ) -> BuiltinTableUpdate {
        let event = self.get_cluster_status(cluster_id, replica_id, process_id);
        let status = event.status.as_kebab_case_str();

        let not_ready_reason = match event.status {
            ClusterStatus::Ready => None,
            ClusterStatus::NotReady(None) => None,
            ClusterStatus::NotReady(Some(NotReadyReason::OomKilled)) => Some("oom-killed"),
        };

        // TODO(#18377): Make replica IDs `NewReplicaId`s throughout the code.
        let replica_id = NewReplicaId::User(replica_id);

        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_CLUSTER_REPLICA_STATUSES),
            row: Row::pack_slice(&[
                Datum::String(&replica_id.to_string()),
                Datum::UInt64(process_id),
                Datum::String(status),
                not_ready_reason.into(),
                Datum::TimestampTz(event.time.try_into().expect("must fit")),
            ]),
            diff,
        }
    }

    pub(super) fn pack_item_update(&self, id: GlobalId, diff: Diff) -> Vec<BuiltinTableUpdate> {
        let entry = self.get_entry(&id);
        let id = entry.id();
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
                id, oid, schema_id, name, "log", None, None, None, None, owner_id, privileges, diff,
            ),
            CatalogItem::Index(index) => {
                self.pack_index_update(id, oid, name, owner_id, index, diff)
            }
            CatalogItem::Table(_) => {
                self.pack_table_update(id, oid, schema_id, name, owner_id, privileges, diff)
            }
            CatalogItem::Source(source) => {
                let source_type = source.source_type();
                let connection_id = source.connection_id();
                let envelope = source.envelope();
                let cluster_id = entry.item().cluster_id().map(|id| id.to_string());

                let mut updates = self.pack_source_update(
                    id,
                    oid,
                    schema_id,
                    name,
                    source_type,
                    connection_id,
                    self.get_storage_object_size(id),
                    envelope,
                    cluster_id.as_deref(),
                    owner_id,
                    privileges,
                    diff,
                );

                updates.extend(match &source.data_source {
                    DataSourceDesc::Ingestion(ingestion) => match &ingestion.desc.connection {
                        GenericSourceConnection::Postgres(postgres) => {
                            self.pack_postgres_source_update(id, postgres, diff)
                        }
                        GenericSourceConnection::Kafka(kafka) => {
                            self.pack_kafka_source_update(id, kafka, diff)
                        }
                        _ => vec![],
                    },
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
        };

        if !entry.item().is_temporary() {
            // Populate or clean up the `mz_object_dependencies` table.
            for dependee in entry.item().uses() {
                updates.push(self.pack_depends_update(id, *dependee, diff))
            }
        }

        if let Ok(desc) = entry.desc(&self.resolve_full_name(entry.name(), entry.conn_id())) {
            let defaults = match entry.item() {
                CatalogItem::Table(table) => Some(&table.defaults),
                _ => None,
            };
            for (i, (column_name, column_type)) in desc.iter().enumerate() {
                let default: Option<String> = defaults.map(|d| d[i].to_ast_string_stable());
                let default: Datum = default
                    .as_ref()
                    .map(|d| Datum::String(d))
                    .unwrap_or(Datum::Null);
                let pgtype = mz_pgrepr::Type::from(&column_type.scalar_type);
                updates.push(BuiltinTableUpdate {
                    id: self.resolve_builtin_table(&MZ_COLUMNS),
                    row: Row::pack_slice(&[
                        Datum::String(&id.to_string()),
                        Datum::String(column_name.as_str()),
                        Datum::UInt64(u64::cast_from(i + 1)),
                        Datum::from(column_type.nullable),
                        Datum::String(pgtype.name()),
                        default,
                        Datum::UInt32(pgtype.oid()),
                        Datum::Int32(pgtype.typmod()),
                    ]),
                    diff,
                });
            }
        }

        updates
    }

    fn pack_table_update(
        &self,
        id: GlobalId,
        oid: u32,
        schema_id: &SchemaSpecifier,
        name: &str,
        owner_id: &RoleId,
        privileges: Datum,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        vec![BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_TABLES),
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::String(&schema_id.to_string()),
                Datum::String(name),
                Datum::String(&owner_id.to_string()),
                privileges,
            ]),
            diff,
        }]
    }

    fn pack_source_update(
        &self,
        id: GlobalId,
        oid: u32,
        schema_id: &SchemaSpecifier,
        name: &str,
        source_desc_name: &str,
        connection_id: Option<GlobalId>,
        size: Option<&str>,
        envelope: Option<&str>,
        cluster_id: Option<&str>,
        owner_id: &RoleId,
        privileges: Datum,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        vec![BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_SOURCES),
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::String(&schema_id.to_string()),
                Datum::String(name),
                Datum::String(source_desc_name),
                Datum::from(connection_id.map(|id| id.to_string()).as_deref()),
                Datum::from(size),
                Datum::from(envelope),
                Datum::from(cluster_id),
                Datum::String(&owner_id.to_string()),
                privileges,
            ]),
            diff,
        }]
    }

    fn pack_postgres_source_update(
        &self,
        id: GlobalId,
        postgres: &PostgresSourceConnection,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        vec![BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_POSTGRES_SOURCES),
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::String(&postgres.publication_details.slot),
            ]),
            diff,
        }]
    }

    fn pack_kafka_source_update(
        &self,
        id: GlobalId,
        kafka: &KafkaSourceConnection,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        vec![BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_KAFKA_SOURCES),
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::String(&kafka.group_id(id)),
            ]),
            diff,
        }]
    }

    fn pack_connection_update(
        &self,
        id: GlobalId,
        oid: u32,
        schema_id: &SchemaSpecifier,
        name: &str,
        owner_id: &RoleId,
        privileges: Datum,
        connection: &Connection,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        let mut updates = vec![BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_CONNECTIONS),
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::String(&schema_id.to_string()),
                Datum::String(name),
                Datum::String(match connection.connection {
                    mz_storage_client::types::connections::Connection::Kafka { .. } => "kafka",
                    mz_storage_client::types::connections::Connection::Csr { .. } => {
                        "confluent-schema-registry"
                    }
                    mz_storage_client::types::connections::Connection::Postgres { .. } => {
                        "postgres"
                    }
                    mz_storage_client::types::connections::Connection::Aws(..) => "aws",
                    mz_storage_client::types::connections::Connection::AwsPrivatelink(..) => {
                        "aws-privatelink"
                    }
                    mz_storage_client::types::connections::Connection::Ssh { .. } => "ssh-tunnel",
                }),
                Datum::String(&owner_id.to_string()),
                privileges,
            ]),
            diff,
        }];
        match connection.connection {
            mz_storage_client::types::connections::Connection::Ssh(ref ssh) => {
                if let Some(public_key_set) = ssh.public_keys.as_ref() {
                    updates.extend(self.pack_ssh_tunnel_connection_update(
                        id,
                        public_key_set,
                        diff,
                    ));
                } else {
                    tracing::error!("does this even happen?");
                }
            }
            mz_storage_client::types::connections::Connection::Kafka(ref kafka) => {
                updates.extend(self.pack_kafka_connection_update(id, kafka, diff));
            }
            mz_storage_client::types::connections::Connection::Csr(_)
            | mz_storage_client::types::connections::Connection::Postgres(_)
            | mz_storage_client::types::connections::Connection::Aws(_)
            | mz_storage_client::types::connections::Connection::AwsPrivatelink(_) => {
                if let Some(aws_principal_context) = self.aws_principal_context.as_ref() {
                    updates.extend(self.pack_aws_privatelink_connection_update(
                        id,
                        aws_principal_context,
                        diff,
                    ));
                } else {
                    tracing::error!("Missing AWS principal context, cannot write to mz_aws_privatelink_connections table");
                }
            }
        };
        updates
    }

    pub(crate) fn pack_ssh_tunnel_connection_update(
        &self,
        id: GlobalId,
        (public_key_primary, public_key_secondary): &(String, String),
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        vec![BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_SSH_TUNNEL_CONNECTIONS),
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::String(public_key_primary),
                Datum::String(public_key_secondary),
            ]),
            diff,
        }]
    }

    fn pack_kafka_connection_update(
        &self,
        id: GlobalId,
        kafka: &KafkaConnection,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        let progress_topic_holder;
        let progress_topic = match kafka.progress_topic {
            Some(ref topic) => Datum::String(topic),
            None => {
                progress_topic_holder = self.config.default_kafka_sink_progress_topic(id);
                Datum::String(&progress_topic_holder)
            }
        };
        let mut row = Row::default();
        row.packer()
            .push_array(
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
        vec![BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_KAFKA_CONNECTIONS),
            row: Row::pack_slice(&[Datum::String(&id.to_string()), brokers, progress_topic]),
            diff,
        }]
    }

    pub fn pack_aws_privatelink_connection_update(
        &self,
        connection_id: GlobalId,
        aws_principal_context: &AwsPrincipalContext,
        diff: Diff,
    ) -> Result<BuiltinTableUpdate, Error> {
        let id = self.resolve_builtin_table(&MZ_AWS_PRIVATELINK_CONNECTIONS);
        let row = Row::pack_slice(&[
            Datum::String(&connection_id.to_string()),
            Datum::String(&aws_principal_context.to_principal_string(connection_id)),
        ]);
        Ok(BuiltinTableUpdate { id, row, diff })
    }

    fn pack_view_update(
        &self,
        id: GlobalId,
        oid: u32,
        schema_id: &SchemaSpecifier,
        name: &str,
        owner_id: &RoleId,
        privileges: Datum,
        view: &View,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        let create_sql = mz_sql::parse::parse(&view.create_sql)
            .unwrap_or_else(|_| panic!("create_sql cannot be invalid: {}", view.create_sql))
            .into_element();
        let query = match create_sql {
            Statement::CreateView(stmt) => stmt.definition.query,
            _ => unreachable!(),
        };

        let mut query_string = query.to_ast_string_stable();
        // PostgreSQL appends a semicolon in `pg_views.definition`, we
        // do the same for compatibility's sake.
        query_string.push(';');

        vec![BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_VIEWS),
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::String(&schema_id.to_string()),
                Datum::String(name),
                Datum::String(&query_string),
                Datum::String(&owner_id.to_string()),
                privileges,
            ]),
            diff,
        }]
    }

    fn pack_materialized_view_update(
        &self,
        id: GlobalId,
        oid: u32,
        schema_id: &SchemaSpecifier,
        name: &str,
        owner_id: &RoleId,
        privileges: Datum,
        mview: &MaterializedView,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        let create_sql = mz_sql::parse::parse(&mview.create_sql)
            .unwrap_or_else(|_| panic!("create_sql cannot be invalid: {}", mview.create_sql))
            .into_element();
        let query = match create_sql {
            Statement::CreateMaterializedView(stmt) => stmt.query,
            _ => unreachable!(),
        };

        let mut query_string = query.to_ast_string_stable();
        // PostgreSQL appends a semicolon in `pg_matviews.definition`, we
        // do the same for compatibility's sake.
        query_string.push(';');

        vec![BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_MATERIALIZED_VIEWS),
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::String(&schema_id.to_string()),
                Datum::String(name),
                Datum::String(&mview.cluster_id.to_string()),
                Datum::String(&query_string),
                Datum::String(&owner_id.to_string()),
                privileges,
            ]),
            diff,
        }]
    }

    fn pack_sink_update(
        &self,
        id: GlobalId,
        oid: u32,
        schema_id: &SchemaSpecifier,
        name: &str,
        owner_id: &RoleId,
        sink: &Sink,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        let mut updates = vec![];
        if let Sink {
            connection: StorageSinkConnectionState::Ready(connection),
            ..
        } = sink
        {
            match connection {
                StorageSinkConnection::Kafka(KafkaSinkConnection { topic, .. }) => {
                    updates.push(BuiltinTableUpdate {
                        id: self.resolve_builtin_table(&MZ_KAFKA_SINKS),
                        row: Row::pack_slice(&[
                            Datum::String(&id.to_string()),
                            Datum::String(topic.as_str()),
                        ]),
                        diff,
                    });
                }
            };

            let envelope = sink.envelope();

            updates.push(BuiltinTableUpdate {
                id: self.resolve_builtin_table(&MZ_SINKS),
                row: Row::pack_slice(&[
                    Datum::String(&id.to_string()),
                    Datum::UInt32(oid),
                    Datum::String(&schema_id.to_string()),
                    Datum::String(name),
                    Datum::String(connection.name()),
                    Datum::from(sink.connection_id().map(|id| id.to_string()).as_deref()),
                    Datum::from(self.get_storage_object_size(id)),
                    Datum::from(envelope),
                    Datum::String(&sink.cluster_id.to_string()),
                    Datum::String(&owner_id.to_string()),
                ]),
                diff,
            });
        }
        updates
    }

    fn pack_index_update(
        &self,
        id: GlobalId,
        oid: u32,
        name: &str,
        owner_id: &RoleId,
        index: &Index,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        let mut updates = vec![];

        let key_sqls = match mz_sql::parse::parse(&index.create_sql)
            .unwrap_or_else(|_| panic!("create_sql cannot be invalid: {}", index.create_sql))
            .into_element()
        {
            Statement::CreateIndex(CreateIndexStatement { key_parts, .. }) => {
                key_parts.expect("key_parts is filled in during planning")
            }
            _ => unreachable!(),
        };

        updates.push(BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_INDEXES),
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::String(name),
                Datum::String(&index.on.to_string()),
                Datum::String(&index.cluster_id.to_string()),
                Datum::String(&owner_id.to_string()),
            ]),
            diff,
        });

        for (i, key) in index.keys.iter().enumerate() {
            let on_entry = self.get_entry(&index.on);
            let nullable = key
                .typ(
                    &on_entry
                        .desc(&self.resolve_full_name(on_entry.name(), on_entry.conn_id()))
                        .expect("can only create indexes on items with a valid description")
                        .typ()
                        .column_types,
                )
                .nullable;
            let seq_in_index = u64::cast_from(i + 1);
            let key_sql = key_sqls
                .get(i)
                .expect("missing sql information for index key")
                .to_ast_string();
            let (field_number, expression) = match key {
                MirScalarExpr::Column(col) => {
                    (Datum::UInt64(u64::cast_from(*col + 1)), Datum::Null)
                }
                _ => (Datum::Null, Datum::String(&key_sql)),
            };
            updates.push(BuiltinTableUpdate {
                id: self.resolve_builtin_table(&MZ_INDEX_COLUMNS),
                row: Row::pack_slice(&[
                    Datum::String(&id.to_string()),
                    Datum::UInt64(seq_in_index),
                    field_number,
                    expression,
                    Datum::from(nullable),
                ]),
                diff,
            });
        }

        updates
    }

    fn pack_type_update(
        &self,
        id: GlobalId,
        oid: u32,
        schema_id: &SchemaSpecifier,
        name: &str,
        owner_id: &RoleId,
        privileges: Datum,
        typ: &Type,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        let generic_update = BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_TYPES),
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::String(&schema_id.to_string()),
                Datum::String(name),
                Datum::String(&TypeCategory::from_catalog_type(&typ.details.typ).to_string()),
                Datum::String(&owner_id.to_string()),
                privileges,
            ]),
            diff,
        };

        let (index_id, update) = match typ.details.typ {
            CatalogType::Array {
                element_reference: element_id,
            } => (
                self.resolve_builtin_table(&MZ_ARRAY_TYPES),
                vec![id.to_string(), element_id.to_string()],
            ),
            CatalogType::List {
                element_reference: element_id,
            } => (
                self.resolve_builtin_table(&MZ_LIST_TYPES),
                vec![id.to_string(), element_id.to_string()],
            ),
            CatalogType::Map {
                key_reference: key_id,
                value_reference: value_id,
            } => (
                self.resolve_builtin_table(&MZ_MAP_TYPES),
                vec![id.to_string(), key_id.to_string(), value_id.to_string()],
            ),
            CatalogType::Pseudo => (
                self.resolve_builtin_table(&MZ_PSEUDO_TYPES),
                vec![id.to_string()],
            ),
            _ => (
                self.resolve_builtin_table(&MZ_BASE_TYPES),
                vec![id.to_string()],
            ),
        };
        let specific_update = BuiltinTableUpdate {
            id: index_id,
            row: Row::pack_slice(&update.iter().map(|c| Datum::String(c)).collect::<Vec<_>>()[..]),
            diff,
        };

        vec![generic_update, specific_update]
    }

    fn pack_func_update(
        &self,
        id: GlobalId,
        schema_id: &SchemaSpecifier,
        name: &str,
        owner_id: &RoleId,
        func: &Func,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        let mut updates = vec![];
        for func_impl_details in func.inner.func_impls() {
            let arg_type_ids = func_impl_details
                .arg_typs
                .iter()
                .map(|typ| self.get_entry_in_system_schemas(typ).id().to_string())
                .collect::<Vec<_>>();

            let mut row = Row::default();
            row.packer()
                .push_array(
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

            updates.push(BuiltinTableUpdate {
                id: self.resolve_builtin_table(&MZ_FUNCTIONS),
                row: Row::pack_slice(&[
                    Datum::String(&id.to_string()),
                    Datum::UInt32(func_impl_details.oid),
                    Datum::String(&schema_id.to_string()),
                    Datum::String(name),
                    arg_type_ids,
                    Datum::from(
                        func_impl_details
                            .variadic_typ
                            .map(|typ| self.get_entry_in_system_schemas(typ).id().to_string())
                            .as_deref(),
                    ),
                    Datum::from(
                        func_impl_details
                            .return_typ
                            .map(|typ| self.get_entry_in_system_schemas(typ).id().to_string())
                            .as_deref(),
                    ),
                    func_impl_details.return_is_set.into(),
                    Datum::String(&owner_id.to_string()),
                ]),
                diff,
            });

            if let mz_sql::func::Func::Aggregate(_) = func.inner {
                updates.push(BuiltinTableUpdate {
                    id: self.resolve_builtin_table(&MZ_AGGREGATES),
                    row: Row::pack_slice(&[
                        Datum::UInt32(func_impl_details.oid),
                        // TODO(materialize#3326): Support ordered-set aggregate functions.
                        Datum::String("n"),
                        Datum::Int16(0),
                    ]),
                    diff,
                });
            }
        }
        updates
    }

    pub fn pack_op_update(
        &self,
        operator: &str,
        func_impl_details: FuncImplCatalogDetails,
        diff: Diff,
    ) -> BuiltinTableUpdate {
        let arg_type_ids = func_impl_details
            .arg_typs
            .iter()
            .map(|typ| self.get_entry_in_system_schemas(typ).id().to_string())
            .collect::<Vec<_>>();

        let mut row = Row::default();
        row.packer()
            .push_array(
                &[ArrayDimension {
                    lower_bound: 1,
                    length: arg_type_ids.len(),
                }],
                arg_type_ids.iter().map(|id| Datum::String(id)),
            )
            .expect("arg_type_ids is 1 dimensional, and its length is used for the array length");
        let arg_type_ids = row.unpack_first();

        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_OPERATORS),
            row: Row::pack_slice(&[
                Datum::UInt32(func_impl_details.oid),
                Datum::String(operator),
                arg_type_ids,
                Datum::from(
                    func_impl_details
                        .return_typ
                        .map(|typ| self.get_entry_in_system_schemas(typ).id().to_string())
                        .as_deref(),
                ),
            ]),
            diff,
        }
    }

    fn pack_secret_update(
        &self,
        id: GlobalId,
        oid: u32,
        schema_id: &SchemaSpecifier,
        name: &str,
        owner_id: &RoleId,
        privileges: Datum,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        vec![BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_SECRETS),
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::String(&schema_id.to_string()),
                Datum::String(name),
                Datum::String(&owner_id.to_string()),
                privileges,
            ]),
            diff,
        }]
    }

    pub fn pack_audit_log_update(
        &self,
        event: &VersionedEvent,
    ) -> Result<BuiltinTableUpdate, Error> {
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
        let dt = mz_ore::now::to_datetime(occurred_at).naive_utc();
        let id = event.sortable_id();
        Ok(BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_AUDIT_EVENTS),
            row: Row::pack_slice(&[
                Datum::UInt64(id),
                Datum::String(&format!("{}", event_type)),
                Datum::String(&format!("{}", object_type)),
                details,
                match user {
                    Some(user) => Datum::String(user),
                    None => Datum::Null,
                },
                Datum::TimestampTz(DateTime::from_utc(dt, Utc).try_into().expect("must fit")),
            ]),
            diff: 1,
        })
    }

    pub fn pack_replica_heartbeat_update(
        &self,
        id: ReplicaId,
        last_heartbeat: DateTime<Utc>,
        diff: Diff,
    ) -> BuiltinTableUpdate {
        // TODO(#18377): Make replica IDs `NewReplicaId`s throughout the code.
        let id = NewReplicaId::User(id);

        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_CLUSTER_REPLICA_HEARTBEATS),
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::TimestampTz(last_heartbeat.try_into().expect("must fit")),
            ]),
            diff,
        }
    }

    pub fn pack_storage_usage_update(
        &self,
        VersionedStorageUsage::V1(event): &VersionedStorageUsage,
    ) -> Result<BuiltinTableUpdate, Error> {
        let id = self.resolve_builtin_table(&MZ_STORAGE_USAGE_BY_SHARD);
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
        Ok(BuiltinTableUpdate { id, row, diff: 1 })
    }

    pub fn pack_egress_ip_update(&self, ip: &Ipv4Addr) -> Result<BuiltinTableUpdate, Error> {
        let id = self.resolve_builtin_table(&MZ_EGRESS_IPS);
        let row = Row::pack_slice(&[Datum::String(&ip.to_string())]);
        Ok(BuiltinTableUpdate { id, row, diff: 1 })
    }

    pub fn pack_replica_metric_updates(
        &self,
        replica_id: ReplicaId,
        updates: &[ServiceProcessMetrics],
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        let id = self.resolve_builtin_table(&MZ_CLUSTER_REPLICA_METRICS);

        // TODO(#18377): Make replica IDs `NewReplicaId`s throughout the code.
        let replica_id = NewReplicaId::User(replica_id);

        let rows = updates.iter().enumerate().map(
            |(
                process_id,
                ServiceProcessMetrics {
                    cpu_nano_cores,
                    memory_bytes,
                },
            )| {
                Row::pack_slice(&[
                    Datum::String(&replica_id.to_string()),
                    u64::cast_from(process_id).into(),
                    (*cpu_nano_cores).into(),
                    (*memory_bytes).into(),
                    // TODO(guswynn): disk usage will be filled in later.
                    Datum::Null,
                ])
            },
        );
        let updates = rows
            .map(|row| BuiltinTableUpdate { id, row, diff })
            .collect();
        updates
    }

    pub fn pack_all_replica_size_updates(&self) -> Vec<BuiltinTableUpdate> {
        let id = self.resolve_builtin_table(&MZ_CLUSTER_REPLICA_SIZES);
        let updates = self
            .cluster_replica_sizes
            .0
            .iter()
            .map(
                |(
                    size,
                    ReplicaAllocation {
                        memory_limit,
                        cpu_limit,
                        scale,
                        workers,
                        credits_per_hour,
                    },
                )| {
                    // Just invent something when the limits are `None`,
                    // which only happens in non-prod environments (tests, process orchestrator, etc.)
                    let cpu_limit = cpu_limit.unwrap_or(CpuLimit::MAX);
                    let MemoryLimit(ByteSize(memory_bytes)) =
                        (*memory_limit).unwrap_or(MemoryLimit::MAX);
                    let row = Row::pack_slice(&[
                        size.as_str().into(),
                        u64::from(*scale).into(),
                        u64::cast_from(*workers).into(),
                        cpu_limit.as_nanocpus().into(),
                        memory_bytes.into(),
                        // TODO(guswynn): disk size will be filled in later.
                        Datum::Null,
                        (*credits_per_hour).into(),
                    ]);
                    BuiltinTableUpdate { id, row, diff: 1 }
                },
            )
            .collect();
        updates
    }

    pub fn pack_replica_write_frontiers_updates(
        &self,
        replica_id: ReplicaId,
        updates: &[(GlobalId, mz_repr::Timestamp)],
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        let id = self.resolve_builtin_table(&MZ_CLUSTER_REPLICA_FRONTIERS);

        // TODO(#18377): Make replica IDs `NewReplicaId`s throughout the code.
        let replica_id = NewReplicaId::User(replica_id);

        let rows = updates.into_iter().map(|(coll_id, time)| {
            Row::pack_slice(&[
                Datum::String(&replica_id.to_string()),
                Datum::String(&coll_id.to_string()),
                Datum::MzTimestamp(*time),
            ])
        });
        let updates = rows
            .map(|row| BuiltinTableUpdate { id, row, diff })
            .collect();
        updates
    }

    pub fn pack_subscribe_update(
        &self,
        id: GlobalId,
        subscribe: &ActiveSubscribe,
        diff: Diff,
    ) -> BuiltinTableUpdate {
        let mut row = Row::default();
        let mut packer = row.packer();
        packer.push(Datum::String(&id.to_string()));
        packer.push(Datum::UInt32(subscribe.conn_id.unhandled()));
        packer.push(Datum::String(&subscribe.cluster_id.to_string()));

        let start_dt = mz_ore::now::to_datetime(subscribe.start_time);
        packer.push(Datum::TimestampTz(start_dt.try_into().expect("must fit")));

        let depends_on: Vec<_> = subscribe
            .depends_on
            .iter()
            .map(|id| id.to_string())
            .collect();
        packer.push_list(depends_on.iter().map(|s| Datum::String(s)));

        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_SUBSCRIPTIONS),
            row,
            diff,
        }
    }

    pub fn pack_session_update(&self, session: &Session, diff: Diff) -> BuiltinTableUpdate {
        let connect_dt = mz_ore::now::to_datetime(session.connect_time());
        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_SESSIONS),
            row: Row::pack_slice(&[
                Datum::UInt32(session.conn_id().unhandled()),
                Datum::String(&session.session_role_id().to_string()),
                Datum::TimestampTz(connect_dt.try_into().expect("must fit")),
            ]),
            diff,
        }
    }

    pub fn pack_default_privileges_update(
        &self,
        default_privilege_object: &DefaultPrivilegeObject,
        grantee: &RoleId,
        acl_mode: &AclMode,
        diff: Diff,
    ) -> BuiltinTableUpdate {
        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_DEFAULT_PRIVILEGES),
            row: Row::pack_slice(&[
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
                    .as_str()
                    .into(),
                grantee.to_string().as_str().into(),
                acl_mode.to_string().as_str().into(),
            ]),
            diff,
        }
    }

    fn pack_privilege_array_row(&self, privileges: &PrivilegeMap) -> Row {
        let mut row = Row::default();
        let flat_privileges = MzAclItem::flatten(privileges);
        row.packer()
            .push_array(
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
}
