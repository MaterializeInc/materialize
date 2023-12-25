// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod notice;

use std::net::Ipv4Addr;

use bytesize::ByteSize;
use chrono::{DateTime, Utc};
use mz_audit_log::{EventDetails, EventType, ObjectType, VersionedEvent, VersionedStorageUsage};
use mz_catalog::builtin::{
    MZ_AGGREGATES, MZ_ARRAY_TYPES, MZ_AUDIT_EVENTS, MZ_AWS_CONNECTIONS,
    MZ_AWS_PRIVATELINK_CONNECTIONS, MZ_BASE_TYPES, MZ_CLUSTERS, MZ_CLUSTER_LINKS,
    MZ_CLUSTER_REPLICAS, MZ_CLUSTER_REPLICA_METRICS, MZ_CLUSTER_REPLICA_SIZES,
    MZ_CLUSTER_REPLICA_STATUSES, MZ_COLUMNS, MZ_COMMENTS, MZ_CONNECTIONS, MZ_DATABASES,
    MZ_DEFAULT_PRIVILEGES, MZ_EGRESS_IPS, MZ_FUNCTIONS, MZ_INDEXES, MZ_INDEX_COLUMNS,
    MZ_INTERNAL_CLUSTER_REPLICAS, MZ_KAFKA_CONNECTIONS, MZ_KAFKA_SINKS, MZ_KAFKA_SOURCES,
    MZ_LIST_TYPES, MZ_MAP_TYPES, MZ_MATERIALIZED_VIEWS, MZ_OBJECT_DEPENDENCIES, MZ_OPERATORS,
    MZ_POSTGRES_SOURCES, MZ_PSEUDO_TYPES, MZ_ROLES, MZ_ROLE_MEMBERS, MZ_SCHEMAS, MZ_SECRETS,
    MZ_SESSIONS, MZ_SINKS, MZ_SOURCES, MZ_SSH_TUNNEL_CONNECTIONS, MZ_STORAGE_USAGE_BY_SHARD,
    MZ_SUBSCRIPTIONS, MZ_SYSTEM_PRIVILEGES, MZ_TABLES, MZ_TYPES, MZ_TYPE_PG_METADATA, MZ_VIEWS,
    MZ_WEBHOOKS_SOURCES,
};
use mz_catalog::memory::error::{Error, ErrorKind};
use mz_catalog::memory::objects::Table;
use mz_catalog::SYSTEM_CONN_ID;
use mz_controller::clusters::{
    ClusterStatus, ManagedReplicaAvailabilityZones, ManagedReplicaLocation, ProcessId,
    ReplicaAllocation, ReplicaLocation,
};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_expr::MirScalarExpr;
use mz_orchestrator::{CpuLimit, DiskLimit, MemoryLimit, NotReadyReason, ServiceProcessMetrics};
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_repr::adt::array::ArrayDimension;
use mz_repr::adt::jsonb::Jsonb;
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem, PrivilegeMap};
use mz_repr::role_id::RoleId;
use mz_repr::{Datum, Diff, GlobalId, Row, RowPacker};
use mz_sql::ast::{CreateIndexStatement, Statement};
use mz_sql::catalog::{CatalogCluster, CatalogDatabase, CatalogSchema, CatalogType, TypeCategory};
use mz_sql::func::FuncImplCatalogDetails;
use mz_sql::names::{CommentObjectId, ResolvedDatabaseSpecifier, SchemaId, SchemaSpecifier};
use mz_sql_parser::ast::display::AstDisplay;
use mz_storage_types::connections::aws::{AwsAuth, AwsConnection};
use mz_storage_types::connections::inline::ReferencedConnection;
use mz_storage_types::connections::{KafkaConnection, StringOrSecret};
use mz_storage_types::sinks::{KafkaSinkConnection, StorageSinkConnection};
use mz_storage_types::sources::{
    GenericSourceConnection, KafkaSourceConnection, PostgresSourceConnection,
};

// DO NOT add any more imports from `crate` outside of `crate::catalog`.
use crate::catalog::{
    AwsPrincipalContext, CatalogItem, CatalogState, ClusterVariant, Connection, DataSourceDesc,
    Database, DefaultPrivilegeObject, Func, Index, MaterializedView, Sink, Type, View,
};
use crate::coord::ConnMeta;
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
        let (size, disk, replication_factor, azs) = match &cluster.config.variant {
            ClusterVariant::Managed(config) => (
                Some(config.size.as_str()),
                Some(config.disk),
                Some(config.replication_factor),
                if config.availability_zones.is_empty() {
                    None
                } else {
                    Some(config.availability_zones.clone())
                },
            ),
            ClusterVariant::Unmanaged => (None, None, None, None),
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
        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_CLUSTERS),
            row,
            diff,
        }
    }

    pub(super) fn pack_cluster_replica_update(
        &self,
        cluster_id: ClusterId,
        name: &str,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        let cluster = &self.clusters_by_id[&cluster_id];
        let id = cluster.replica_id(name).expect("Must exist");
        let replica = cluster.replica(id).expect("Must exist");

        let (size, disk, az, internal) = match &replica.config.location {
            // TODO(guswynn): The column should be `availability_zones`, not
            // `availability_zone`.
            ReplicaLocation::Managed(ManagedReplicaLocation {
                size,
                availability_zones: ManagedReplicaAvailabilityZones::FromReplica(Some(az)),
                allocation: _,
                disk,
                billed_as: _,
                internal,
            }) => (Some(&**size), Some(*disk), Some(az.as_str()), *internal),
            ReplicaLocation::Managed(ManagedReplicaLocation {
                size,
                availability_zones: _,
                allocation: _,
                disk,
                billed_as: _,
                internal,
            }) => (Some(&**size), Some(*disk), None, *internal),
            _ => (None, None, None, false),
        };

        let cluster_replica_update = BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_CLUSTER_REPLICAS),
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::String(name),
                Datum::String(&cluster_id.to_string()),
                Datum::from(size),
                Datum::from(az),
                Datum::String(&replica.owner_id.to_string()),
                Datum::from(disk),
            ]),
            diff,
        };

        let mut updates = vec![cluster_replica_update];

        if internal {
            let update = BuiltinTableUpdate {
                id: self.resolve_builtin_table(&MZ_INTERNAL_CLUSTER_REPLICAS),
                row: Row::pack_slice(&[Datum::String(&id.to_string())]),
                diff,
            };
            updates.push(update);
        }

        updates
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
        let mut updates =
            match entry.item() {
                CatalogItem::Log(_) => self.pack_source_update(
                    id, oid, schema_id, name, "log", None, None, None, None, None, None, owner_id,
                    privileges, diff, None,
                ),
                CatalogItem::Index(index) => {
                    self.pack_index_update(id, oid, name, owner_id, index, diff)
                }
                CatalogItem::Table(table) => self
                    .pack_table_update(id, oid, schema_id, name, owner_id, privileges, diff, table),
                CatalogItem::Source(source) => {
                    let source_type = source.source_type();
                    let connection_id = source.connection_id();
                    let envelope = source.envelope();
                    let cluster_id = entry.item().cluster_id().map(|id| id.to_string());

                    let (key_format, value_format) = source.formats();

                    let mut updates = self.pack_source_update(
                        id,
                        oid,
                        schema_id,
                        name,
                        source_type,
                        connection_id,
                        self.get_storage_object_size(id),
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
                        DataSourceDesc::Ingestion(ingestion) => match &ingestion.desc.connection {
                            GenericSourceConnection::Postgres(postgres) => {
                                self.pack_postgres_source_update(id, postgres, diff)
                            }
                            GenericSourceConnection::Kafka(kafka) => {
                                self.pack_kafka_source_update(id, kafka, diff)
                            }
                            _ => vec![],
                        },
                        DataSourceDesc::Webhook { .. } => {
                            vec![self.pack_webhook_source_update(id, diff)]
                        }
                        _ => vec![],
                    });

                    updates
                }
                CatalogItem::View(view) => self
                    .pack_view_update(id, oid, schema_id, name, owner_id, privileges, view, diff),
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
            // TODO(jkosh44) Unclear if this table wants to include all uses or only references.
            for dependee in &entry.item().references().0 {
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
        table: &Table,
    ) -> Vec<BuiltinTableUpdate> {
        let redacted = table.create_sql.as_ref().map(|create_sql| {
            mz_sql::parse::parse(create_sql)
                .unwrap_or_else(|_| panic!("create_sql cannot be invalid: {}", create_sql))
                .into_element()
                .ast
                .to_ast_string_redacted()
        });

        vec![BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_TABLES),
            row: Row::pack_slice(&[
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
        key_format: Option<&str>,
        value_format: Option<&str>,
        cluster_id: Option<&str>,
        owner_id: &RoleId,
        privileges: Datum,
        diff: Diff,
        create_sql: Option<&String>,
    ) -> Vec<BuiltinTableUpdate> {
        let redacted = create_sql.map(|create_sql| {
            let create_stmt = mz_sql::parse::parse(create_sql)
                .unwrap_or_else(|_| panic!("create_sql cannot be invalid: {}", create_sql))
                .into_element()
                .ast;
            create_stmt.to_ast_string_redacted()
        });
        vec![BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_SOURCES),
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::String(&schema_id.to_string()),
                Datum::String(name),
                Datum::String(source_desc_name),
                Datum::from(connection_id.map(|id| id.to_string()).as_deref()),
                // TODO: this will always be null when we remove the notion of
                // linked clusters.
                Datum::from(size),
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
        }]
    }

    fn pack_postgres_source_update(
        &self,
        id: GlobalId,
        postgres: &PostgresSourceConnection<ReferencedConnection>,
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
        kafka: &KafkaSourceConnection<ReferencedConnection>,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        vec![BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_KAFKA_SOURCES),
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::String(&kafka.group_id(&self.config.connection_context, id)),
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
        let create_stmt = mz_sql::parse::parse(&connection.create_sql)
            .unwrap_or_else(|_| panic!("create_sql cannot be invalid: {}", connection.create_sql))
            .into_element()
            .ast;
        let mut updates = vec![BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_CONNECTIONS),
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::String(&schema_id.to_string()),
                Datum::String(name),
                Datum::String(match connection.connection {
                    mz_storage_types::connections::Connection::Kafka { .. } => "kafka",
                    mz_storage_types::connections::Connection::Csr { .. } => {
                        "confluent-schema-registry"
                    }
                    mz_storage_types::connections::Connection::Postgres { .. } => "postgres",
                    mz_storage_types::connections::Connection::Aws(..) => "aws",
                    mz_storage_types::connections::Connection::AwsPrivatelink(..) => {
                        "aws-privatelink"
                    }
                    mz_storage_types::connections::Connection::Ssh { .. } => "ssh-tunnel",
                    mz_storage_types::connections::Connection::MySql { .. } => "mysql",
                }),
                Datum::String(&owner_id.to_string()),
                privileges,
                Datum::String(&connection.create_sql),
                Datum::String(&create_stmt.to_ast_string_redacted()),
            ]),
            diff,
        }];
        match connection.connection {
            mz_storage_types::connections::Connection::Ssh(ref ssh) => {
                if let Some(public_key_set) = ssh.public_keys.as_ref() {
                    updates.extend(self.pack_ssh_tunnel_connection_update(
                        id,
                        public_key_set,
                        diff,
                    ));
                } else {
                    tracing::error!(%id, "missing SSH public key; cannot write row to mz_ssh_connections table");
                }
            }
            mz_storage_types::connections::Connection::Kafka(ref kafka) => {
                updates.extend(self.pack_kafka_connection_update(id, kafka, diff));
            }
            mz_storage_types::connections::Connection::Aws(ref aws_config) => {
                match self.pack_aws_connection_update(id, aws_config, diff) {
                    Ok(update) => {
                        updates.push(update);
                    }
                    Err(e) => {
                        tracing::error!(%id, %e, "failed writing row to mz_aws_connections table");
                    }
                }
            }
            mz_storage_types::connections::Connection::AwsPrivatelink(_) => {
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
            mz_storage_types::connections::Connection::Csr(_)
            | mz_storage_types::connections::Connection::Postgres(_)
            | mz_storage_types::connections::Connection::MySql(_) => (),
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
        kafka: &KafkaConnection<ReferencedConnection>,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        let progress_topic = kafka.progress_topic(&self.config.connection_context, id);
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
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                brokers,
                Datum::String(&progress_topic),
            ]),
            diff,
        }]
    }

    pub fn pack_aws_privatelink_connection_update(
        &self,
        connection_id: GlobalId,
        aws_principal_context: &AwsPrincipalContext,
        diff: Diff,
    ) -> BuiltinTableUpdate {
        let id = self.resolve_builtin_table(&MZ_AWS_PRIVATELINK_CONNECTIONS);
        let row = Row::pack_slice(&[
            Datum::String(&connection_id.to_string()),
            Datum::String(&aws_principal_context.to_principal_string(connection_id)),
        ]);
        BuiltinTableUpdate { id, row, diff }
    }

    pub fn pack_aws_connection_update(
        &self,
        connection_id: GlobalId,
        aws_config: &AwsConnection,
        diff: Diff,
    ) -> Result<BuiltinTableUpdate, anyhow::Error> {
        let id = self.resolve_builtin_table(&MZ_AWS_CONNECTIONS);

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
        let create_stmt = mz_sql::parse::parse(&view.create_sql)
            .unwrap_or_else(|_| panic!("create_sql cannot be invalid: {}", view.create_sql))
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
                Datum::String(&view.create_sql),
                Datum::String(&create_stmt.to_ast_string_redacted()),
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
        let create_stmt = mz_sql::parse::parse(&mview.create_sql)
            .unwrap_or_else(|_| panic!("create_sql cannot be invalid: {}", mview.create_sql))
            .into_element()
            .ast;
        let query = match &create_stmt {
            Statement::CreateMaterializedView(stmt) => &stmt.query,
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
                Datum::String(&mview.create_sql),
                Datum::String(&create_stmt.to_ast_string_redacted()),
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
        match &sink.connection {
            StorageSinkConnection::Kafka(KafkaSinkConnection {
                topic: topic_name, ..
            }) => {
                updates.push(BuiltinTableUpdate {
                    id: self.resolve_builtin_table(&MZ_KAFKA_SINKS),
                    row: Row::pack_slice(&[
                        Datum::String(&id.to_string()),
                        Datum::String(topic_name.as_str()),
                    ]),
                    diff,
                });
            }
        };

        let create_stmt = mz_sql::parse::parse(&sink.create_sql)
            .unwrap_or_else(|_| panic!("create_sql cannot be invalid: {}", sink.create_sql))
            .into_element()
            .ast;

        let envelope = sink.envelope();
        let format = sink.format();

        updates.push(BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_SINKS),
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::String(&schema_id.to_string()),
                Datum::String(name),
                Datum::String(sink.connection.name()),
                Datum::from(sink.connection_id().map(|id| id.to_string()).as_deref()),
                Datum::from(self.get_storage_object_size(id)),
                Datum::from(envelope),
                Datum::from(format),
                Datum::String(&sink.cluster_id.to_string()),
                Datum::String(&owner_id.to_string()),
                Datum::String(&sink.create_sql),
                Datum::String(&create_stmt.to_ast_string_redacted()),
            ]),
            diff,
        });

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

        let create_stmt = mz_sql::parse::parse(&index.create_sql)
            .unwrap_or_else(|_| panic!("create_sql cannot be invalid: {}", index.create_sql))
            .into_element()
            .ast;

        let key_sqls = match &create_stmt {
            Statement::CreateIndex(CreateIndexStatement { key_parts, .. }) => key_parts
                .as_ref()
                .expect("key_parts is filled in during planning"),
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
                Datum::String(&index.create_sql),
                Datum::String(&create_stmt.to_ast_string_redacted()),
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
        let mut out = vec![];

        let redacted = typ.create_sql.as_ref().map(|create_sql| {
            mz_sql::parse::parse(create_sql)
                .unwrap_or_else(|_| panic!("create_sql cannot be invalid: {}", create_sql))
                .into_element()
                .ast
                .to_ast_string_redacted()
        });

        out.push(BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_TYPES),
            row: Row::pack_slice(&[
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
        });

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
                self.resolve_builtin_table(&MZ_ARRAY_TYPES)
            }
            CatalogType::List {
                element_reference: element_id,
                element_modifiers,
            } => {
                packer.push(Datum::String(&id.to_string()));
                packer.push(Datum::String(&element_id.to_string()));
                append_modifier(&mut packer, element_modifiers);
                self.resolve_builtin_table(&MZ_LIST_TYPES)
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
                self.resolve_builtin_table(&MZ_MAP_TYPES)
            }
            CatalogType::Pseudo => {
                packer.push(Datum::String(&id.to_string()));
                self.resolve_builtin_table(&MZ_PSEUDO_TYPES)
            }
            _ => {
                packer.push(Datum::String(&id.to_string()));
                self.resolve_builtin_table(&MZ_BASE_TYPES)
            }
        };
        out.push(BuiltinTableUpdate {
            id: index_id,
            row,
            diff,
        });

        if let Some(pg_metadata) = &typ.details.pg_metadata {
            out.push(BuiltinTableUpdate {
                id: self.resolve_builtin_table(&MZ_TYPE_PG_METADATA),
                row: Row::pack_slice(&[
                    Datum::String(&id.to_string()),
                    Datum::UInt32(pg_metadata.typinput_oid),
                    Datum::UInt32(pg_metadata.typreceive_oid),
                ]),
                diff,
            });
        }

        out
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
                .map(|typ| self.get_system_type(typ).id().to_string())
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
            .map(|typ| self.get_system_type(typ).id().to_string())
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
                        .map(|typ| self.get_system_type(typ).id().to_string())
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
        let rows = updates.iter().enumerate().map(
            |(
                process_id,
                ServiceProcessMetrics {
                    cpu_nano_cores,
                    memory_bytes,
                    disk_usage_bytes,
                },
            )| {
                Row::pack_slice(&[
                    Datum::String(&replica_id.to_string()),
                    u64::cast_from(process_id).into(),
                    (*cpu_nano_cores).into(),
                    (*memory_bytes).into(),
                    (*disk_usage_bytes).into(),
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
            .filter(|(_, a)| !a.disabled)
            .map(
                |(
                    size,
                    ReplicaAllocation {
                        memory_limit,
                        cpu_limit,
                        disk_limit,
                        scale,
                        workers,
                        credits_per_hour,
                        disabled: _,
                    },
                )| {
                    // Just invent something when the limits are `None`,
                    // which only happens in non-prod environments (tests, process orchestrator, etc.)
                    let cpu_limit = cpu_limit.unwrap_or(CpuLimit::MAX);
                    let MemoryLimit(ByteSize(memory_bytes)) =
                        (*memory_limit).unwrap_or(MemoryLimit::MAX);
                    let DiskLimit(ByteSize(disk_bytes)) =
                        (*disk_limit).unwrap_or(DiskLimit::ARBITRARY);
                    let row = Row::pack_slice(&[
                        size.as_str().into(),
                        u64::from(*scale).into(),
                        u64::cast_from(*workers).into(),
                        cpu_limit.as_nanocpus().into(),
                        memory_bytes.into(),
                        // TODO(guswynn): disk size will be filled in later.
                        disk_bytes.into(),
                        (*credits_per_hour).into(),
                    ]);
                    BuiltinTableUpdate { id, row, diff: 1 }
                },
            )
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

    pub fn pack_session_update(&self, conn: &ConnMeta, diff: Diff) -> BuiltinTableUpdate {
        let connect_dt = mz_ore::now::to_datetime(conn.connected_at());
        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_SESSIONS),
            row: Row::pack_slice(&[
                Datum::UInt32(conn.conn_id().unhandled()),
                Datum::String(&conn.authenticated_role_id().to_string()),
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
                    .to_lowercase()
                    .as_str()
                    .into(),
                grantee.to_string().as_str().into(),
                acl_mode.to_string().as_str().into(),
            ]),
            diff,
        }
    }

    pub fn pack_system_privileges_update(
        &self,
        privileges: MzAclItem,
        diff: Diff,
    ) -> BuiltinTableUpdate {
        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_SYSTEM_PRIVILEGES),
            row: Row::pack_slice(&[privileges.into()]),
            diff,
        }
    }

    fn pack_privilege_array_row(&self, privileges: &PrivilegeMap) -> Row {
        let mut row = Row::default();
        let flat_privileges: Vec<_> = privileges.all_values_owned().collect();
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

    pub fn pack_comment_update(
        &self,
        object_id: CommentObjectId,
        column_pos: Option<usize>,
        comment: &str,
        diff: Diff,
    ) -> BuiltinTableUpdate {
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
            | CommentObjectId::Type(global_id) => global_id.to_string(),
            CommentObjectId::Role(role_id) => role_id.to_string(),
            CommentObjectId::Database(database_id) => database_id.to_string(),
            CommentObjectId::Schema((_, schema_id)) => schema_id.to_string(),
            CommentObjectId::Cluster(cluster_id) => cluster_id.to_string(),
            CommentObjectId::ClusterReplica((_, replica_id)) => replica_id.to_string(),
        };
        let column_pos_datum = match column_pos {
            Some(pos) => {
                // TODO(parkmycar): https://github.com/MaterializeInc/materialize/issues/22246.
                let pos =
                    i32::try_from(pos).expect("we constrain this value in the planning layer");
                Datum::Int32(pos)
            }
            None => Datum::Null,
        };

        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_COMMENTS),
            row: Row::pack_slice(&[
                Datum::String(&object_id_str),
                Datum::String(&object_type_str),
                column_pos_datum,
                Datum::String(comment),
            ]),
            diff,
        }
    }

    pub fn pack_webhook_source_update(
        &self,
        source_id: GlobalId,
        diff: Diff,
    ) -> BuiltinTableUpdate {
        let url = self
            .try_get_webhook_url(&source_id)
            .expect("webhook source should exist");
        let url = url.to_string();
        let name = &self.get_entry(&source_id).name().item;
        let id_str = source_id.to_string();

        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_WEBHOOKS_SOURCES),
            row: Row::pack_slice(&[
                Datum::String(&id_str),
                Datum::String(name),
                Datum::String(&url),
            ]),
            diff,
        }
    }
}
