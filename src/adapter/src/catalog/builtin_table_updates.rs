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
use mz_compute_client::controller::{
    ComputeReplicaAllocation, ComputeReplicaLocation, ProcessId, ReplicaId,
};
use mz_controller::clusters::{ClusterId, ClusterStatus};
use mz_expr::MirScalarExpr;
use mz_orchestrator::{CpuLimit, MemoryLimit, ServiceProcessMetrics};
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_repr::adt::array::ArrayDimension;
use mz_repr::adt::jsonb::Jsonb;
use mz_repr::{Datum, Diff, GlobalId, Row};
use mz_sql::ast::{CreateIndexStatement, Statement};
use mz_sql::catalog::{CatalogDatabase, CatalogType, TypeCategory};
use mz_sql::names::{ResolvedDatabaseSpecifier, SchemaId, SchemaSpecifier};
use mz_sql_parser::ast::display::AstDisplay;
use mz_storage_client::types::connections::KafkaConnection;
use mz_storage_client::types::sinks::{KafkaSinkConnection, StorageSinkConnection};
use mz_storage_client::types::sources::{GenericSourceConnection, PostgresSourceConnection};

use crate::catalog::builtin::{
    MZ_ARRAY_TYPES, MZ_AUDIT_EVENTS, MZ_BASE_TYPES, MZ_CLUSTERS, MZ_CLUSTER_LINKS,
    MZ_CLUSTER_REPLICAS, MZ_CLUSTER_REPLICA_FRONTIERS, MZ_CLUSTER_REPLICA_HEARTBEATS,
    MZ_CLUSTER_REPLICA_METRICS, MZ_CLUSTER_REPLICA_STATUSES, MZ_COLUMNS, MZ_CONNECTIONS,
    MZ_DATABASES, MZ_EGRESS_IPS, MZ_FUNCTIONS, MZ_INDEXES, MZ_INDEX_COLUMNS, MZ_KAFKA_CONNECTIONS,
    MZ_KAFKA_SINKS, MZ_LIST_TYPES, MZ_MAP_TYPES, MZ_MATERIALIZED_VIEWS, MZ_OBJECT_DEPENDENCIES,
    MZ_POSTGRES_SOURCES, MZ_PSEUDO_TYPES, MZ_ROLES, MZ_SCHEMAS, MZ_SECRETS, MZ_SINKS, MZ_SOURCES,
    MZ_SSH_TUNNEL_CONNECTIONS, MZ_STORAGE_USAGE_BY_SHARD, MZ_TABLES, MZ_TYPES, MZ_VIEWS,
};
use crate::catalog::{
    CatalogItem, CatalogState, Connection, DataSourceDesc, Database, Error, ErrorKind, Func, Index,
    MaterializedView, Role, Sink, StorageSinkConnectionState, Type, View, SYSTEM_CONN_ID,
};

use super::builtin::{MZ_AWS_PRIVATELINK_CONNECTIONS, MZ_CLUSTER_REPLICA_SIZES};
use super::AwsPrincipalContext;

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
    pub(super) fn pack_depends_update(
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
        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_DATABASES),
            row: Row::pack_slice(&[
                Datum::UInt64(database.id.0),
                Datum::UInt32(database.oid),
                Datum::String(database.name()),
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
                Some(id.0),
                &self.database_by_id[id].schemas_by_id[schema_id],
            ),
        };
        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_SCHEMAS),
            row: Row::pack_slice(&[
                Datum::UInt64(schema_id.0),
                Datum::UInt32(schema.oid),
                Datum::from(database_id),
                Datum::String(&schema.name.schema),
            ]),
            diff,
        }
    }

    pub(super) fn pack_role_update(&self, role: &Role, diff: Diff) -> BuiltinTableUpdate {
        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_ROLES),
            row: Row::pack_slice(&[
                Datum::String(&role.id.to_string()),
                Datum::UInt32(role.oid),
                Datum::String(&role.name),
            ]),
            diff,
        }
    }

    pub(super) fn pack_cluster_update(&self, name: &str, diff: Diff) -> BuiltinTableUpdate {
        let id = self.clusters_by_name[name];
        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_CLUSTERS),
            row: Row::pack_slice(&[Datum::String(&id.to_string()), Datum::String(name)]),
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
            ComputeReplicaLocation::Managed {
                size,
                availability_zone,
                az_user_specified: _,
                allocation: _,
            } => (Some(&**size), Some(availability_zone.as_str())),
            ComputeReplicaLocation::Remote { .. } => (None, None),
        };

        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_CLUSTER_REPLICAS),
            row: Row::pack_slice(&[
                Datum::UInt64(id),
                Datum::String(name),
                Datum::String(&cluster_id.to_string()),
                Datum::from(size),
                Datum::from(az),
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
        let status = match event.status {
            ClusterStatus::Ready => "ready",
            ClusterStatus::NotReady => "not-ready",
        };

        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_CLUSTER_REPLICA_STATUSES),
            row: Row::pack_slice(&[
                Datum::UInt64(replica_id),
                Datum::UInt64(process_id),
                Datum::String(status),
                Datum::TimestampTz(event.time.try_into().expect("must fit")),
            ]),
            diff,
        }
    }

    pub(super) fn pack_item_update(&self, id: GlobalId, diff: Diff) -> Vec<BuiltinTableUpdate> {
        let entry = self.get_entry(&id);
        let id = entry.id();
        let oid = entry.oid();
        let conn_id = entry.item().conn_id().unwrap_or(SYSTEM_CONN_ID);
        let schema_id = &self
            .get_schema(
                &entry.name().qualifiers.database_spec,
                &entry.name().qualifiers.schema_spec,
                conn_id,
            )
            .id;
        let name = &entry.name().item;
        let mut updates = match entry.item() {
            CatalogItem::Log(_) => self.pack_source_update(
                id, oid, schema_id, name, "log", None, None, None, None, diff,
            ),
            CatalogItem::Index(index) => self.pack_index_update(id, oid, name, index, diff),
            CatalogItem::Table(_) => self.pack_table_update(id, oid, schema_id, name, diff),
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
                    diff,
                );

                updates.extend(match &source.data_source {
                    DataSourceDesc::Ingestion(ingestion) => match &ingestion.desc.connection {
                        GenericSourceConnection::Postgres(postgres) => {
                            self.pack_postgres_source_update(id, postgres, diff)
                        }
                        _ => vec![],
                    },
                    _ => vec![],
                });

                updates
            }
            CatalogItem::View(view) => self.pack_view_update(id, oid, schema_id, name, view, diff),
            CatalogItem::MaterializedView(mview) => {
                self.pack_materialized_view_update(id, oid, schema_id, name, mview, diff)
            }
            CatalogItem::Sink(sink) => self.pack_sink_update(id, oid, schema_id, name, sink, diff),
            CatalogItem::Type(ty) => self.pack_type_update(id, oid, schema_id, name, ty, diff),
            CatalogItem::Func(func) => self.pack_func_update(id, schema_id, name, func, diff),
            CatalogItem::Secret(_) => self.pack_secret_update(id, schema_id, name, diff),
            CatalogItem::Connection(connection) => {
                self.pack_connection_update(id, oid, schema_id, name, connection, diff)
            }
        };

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
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        vec![BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_TABLES),
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::UInt64(schema_id.into()),
                Datum::String(name),
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
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        vec![BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_SOURCES),
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::UInt64(schema_id.into()),
                Datum::String(name),
                Datum::String(source_desc_name),
                Datum::from(connection_id.map(|id| id.to_string()).as_deref()),
                Datum::from(size),
                Datum::from(envelope),
                Datum::from(cluster_id),
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

    fn pack_connection_update(
        &self,
        id: GlobalId,
        oid: u32,
        schema_id: &SchemaSpecifier,
        name: &str,
        connection: &Connection,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        let mut updates = vec![BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_CONNECTIONS),
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::UInt64(schema_id.into()),
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
            .unwrap();
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
        view: &View,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        let create_sql = mz_sql::parse::parse(&view.create_sql)
            .expect("create_sql cannot be invalid")
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
                Datum::UInt64(schema_id.into()),
                Datum::String(name),
                Datum::String(&query_string),
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
        mview: &MaterializedView,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        let create_sql = mz_sql::parse::parse(&mview.create_sql)
            .expect("create_sql cannot be invalid")
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
                Datum::UInt64(schema_id.into()),
                Datum::String(name),
                Datum::String(&mview.cluster_id.to_string()),
                Datum::String(&query_string),
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
            updates.push(BuiltinTableUpdate {
                id: self.resolve_builtin_table(&MZ_SINKS),
                row: Row::pack_slice(&[
                    Datum::String(&id.to_string()),
                    Datum::UInt32(oid),
                    Datum::UInt64(schema_id.into()),
                    Datum::String(name),
                    Datum::String(connection.name()),
                    Datum::from(sink.connection_id().map(|id| id.to_string()).as_deref()),
                    Datum::from(self.get_storage_object_size(id)),
                    Datum::String(&sink.cluster_id.to_string()),
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
        index: &Index,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        let mut updates = vec![];

        let key_sqls = match mz_sql::parse::parse(&index.create_sql)
            .expect("create_sql cannot be invalid")
            .into_element()
        {
            Statement::CreateIndex(CreateIndexStatement { key_parts, .. }) => key_parts.unwrap(),
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
            ]),
            diff,
        });

        for (i, key) in index.keys.iter().enumerate() {
            let on_entry = self.get_entry(&index.on);
            let nullable = key
                .typ(
                    &on_entry
                        .desc(&self.resolve_full_name(on_entry.name(), on_entry.conn_id()))
                        .unwrap()
                        .typ()
                        .column_types,
                )
                .nullable;
            let seq_in_index = u64::cast_from(i + 1);
            let key_sql = key_sqls
                .get(i)
                .expect("missing sql information for index key")
                .to_string();
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
        typ: &Type,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        let generic_update = BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_TYPES),
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::UInt64(schema_id.into()),
                Datum::String(name),
                Datum::String(&TypeCategory::from_catalog_type(&typ.details.typ).to_string()),
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
                        length: func_impl_details.arg_typs.len(),
                    }],
                    arg_type_ids.iter().map(|id| Datum::String(id)),
                )
                .unwrap();
            let arg_type_ids = row.unpack_first();

            updates.push(BuiltinTableUpdate {
                id: self.resolve_builtin_table(&MZ_FUNCTIONS),
                row: Row::pack_slice(&[
                    Datum::String(&id.to_string()),
                    Datum::UInt32(func_impl_details.oid),
                    Datum::UInt64(schema_id.into()),
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
                ]),
                diff,
            });
        }
        updates
    }

    fn pack_secret_update(
        &self,
        id: GlobalId,
        schema_id: &SchemaSpecifier,
        name: &str,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        vec![BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_SECRETS),
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt64(schema_id.into()),
                Datum::String(name),
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
        let details = details.iter().next().unwrap();
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
        let table = self.resolve_builtin_table(&MZ_CLUSTER_REPLICA_HEARTBEATS);
        let row = Row::pack_slice(&[
            Datum::UInt64(id),
            Datum::TimestampTz(last_heartbeat.try_into().expect("must fit")),
        ]);
        BuiltinTableUpdate {
            id: table,
            row,
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
        let rows = updates.iter().enumerate().map(
            |(
                process_id,
                ServiceProcessMetrics {
                    cpu_nano_cores,
                    memory_bytes,
                },
            )| {
                Row::pack_slice(&[
                    replica_id.into(),
                    u64::cast_from(process_id).into(),
                    (*cpu_nano_cores).into(),
                    (*memory_bytes).into(),
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
                    ComputeReplicaAllocation {
                        memory_limit,
                        cpu_limit,
                        scale,
                        workers,
                    },
                )| {
                    // Just invent something when the limits are `None`,
                    // which only happens in non-prod environments (tests, process orchestrator, etc.)
                    let cpu_limit = cpu_limit.unwrap_or(CpuLimit::MAX);
                    let MemoryLimit(ByteSize(memory_bytes)) =
                        (*memory_limit).unwrap_or(MemoryLimit::MAX);
                    let row = Row::pack_slice(&[
                        size.as_str().into(),
                        u64::cast_from(scale.get()).into(),
                        u64::cast_from(workers.get()).into(),
                        cpu_limit.as_nanocpus().into(),
                        memory_bytes.into(),
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
        let rows = updates.into_iter().map(|(coll_id, time)| {
            Row::pack_slice(&[
                replica_id.into(),
                Datum::String(&coll_id.to_string()),
                Datum::MzTimestamp(*time),
            ])
        });
        let updates = rows
            .map(|row| BuiltinTableUpdate { id, row, diff })
            .collect();
        updates
    }
}
