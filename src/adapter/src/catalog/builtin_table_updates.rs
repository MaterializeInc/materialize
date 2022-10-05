// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use mz_audit_log::{EventDetails, EventType, ObjectType, VersionedEvent, VersionedStorageUsage};
use mz_compute_client::command::{ProcessId, ReplicaId};
use mz_compute_client::controller::{
    ComputeInstanceId, ComputeInstanceStatus, ComputeReplicaLocation,
};
use mz_expr::MirScalarExpr;
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_repr::adt::array::ArrayDimension;
use mz_repr::adt::jsonb::Jsonb;
use mz_repr::{Datum, Diff, GlobalId, Row};
use mz_sql::ast::{CreateIndexStatement, Statement};
use mz_sql::catalog::{CatalogDatabase, CatalogType, TypeCategory};
use mz_sql::names::{DatabaseId, ResolvedDatabaseSpecifier, SchemaId, SchemaSpecifier};
use mz_sql_parser::ast::display::AstDisplay;
use mz_storage::types::connections::KafkaConnection;
use mz_storage::types::hosts::StorageHostConfig;
use mz_storage::types::sinks::{KafkaSinkConnection, StorageSinkConnection};

use crate::catalog::builtin::{
    MZ_ARRAY_TYPES, MZ_AUDIT_EVENTS, MZ_BASE_TYPES, MZ_CLUSTERS, MZ_CLUSTER_REPLICAS_BASE,
    MZ_CLUSTER_REPLICA_HEARTBEATS, MZ_CLUSTER_REPLICA_STATUSES, MZ_COLUMNS, MZ_CONNECTIONS,
    MZ_DATABASES, MZ_FUNCTIONS, MZ_INDEXES, MZ_INDEX_COLUMNS, MZ_KAFKA_CONNECTIONS, MZ_KAFKA_SINKS,
    MZ_LIST_TYPES, MZ_MAP_TYPES, MZ_MATERIALIZED_VIEWS, MZ_PSEUDO_TYPES, MZ_ROLES, MZ_SCHEMAS,
    MZ_SECRETS, MZ_SINKS, MZ_SOURCES, MZ_SSH_TUNNEL_CONNECTIONS, MZ_STORAGE_USAGE, MZ_TABLES,
    MZ_TYPES, MZ_VIEWS,
};
use crate::catalog::{
    CatalogItem, CatalogState, Connection, Error, ErrorKind, Func, Index, MaterializedView, Sink,
    StorageSinkConnectionState, Type, View, SYSTEM_CONN_ID,
};
use crate::coord::ReplicaMetadata;

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
    pub(super) fn pack_database_update(&self, id: &DatabaseId, diff: Diff) -> BuiltinTableUpdate {
        let database = &self.database_by_id[id];
        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_DATABASES),
            row: Row::pack_slice(&[
                Datum::UInt64(id.0),
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

    pub(super) fn pack_role_update(&self, name: &str, diff: Diff) -> BuiltinTableUpdate {
        let role = &self.roles[name];
        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_ROLES),
            row: Row::pack_slice(&[
                Datum::String(&role.id.to_string()),
                Datum::UInt32(role.oid),
                Datum::String(name),
            ]),
            diff,
        }
    }

    pub(super) fn pack_compute_instance_update(
        &self,
        name: &str,
        diff: Diff,
    ) -> BuiltinTableUpdate {
        let id = self.compute_instances_by_name[name];
        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_CLUSTERS),
            row: Row::pack_slice(&[Datum::UInt64(id), Datum::String(name)]),
            diff,
        }
    }

    pub(super) fn pack_compute_replica_update(
        &self,
        compute_instance_id: ComputeInstanceId,
        name: &str,
        diff: Diff,
    ) -> BuiltinTableUpdate {
        let instance = &self.compute_instances_by_id[&compute_instance_id];
        let id = instance.replica_id_by_name[name];
        let replica = &instance.replicas_by_id[&id];

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
            id: self.resolve_builtin_table(&MZ_CLUSTER_REPLICAS_BASE),
            row: Row::pack_slice(&[
                Datum::UInt64(compute_instance_id),
                Datum::UInt64(id),
                Datum::String(name),
                Datum::from(size),
                Datum::from(az),
            ]),
            diff,
        }
    }

    pub(super) fn pack_compute_instance_status_update(
        &self,
        compute_instance_id: ComputeInstanceId,
        replica_id: ReplicaId,
        process_id: ProcessId,
        diff: Diff,
    ) -> BuiltinTableUpdate {
        let event = self
            .try_get_compute_instance_status(compute_instance_id, replica_id, process_id)
            .expect("status not known");
        let status = match event.status {
            ComputeInstanceStatus::Ready => "ready",
            ComputeInstanceStatus::NotReady => "not_ready",
            ComputeInstanceStatus::Unknown => "unknown",
        };

        BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_CLUSTER_REPLICA_STATUSES),
            row: Row::pack_slice(&[
                Datum::UInt64(replica_id),
                Datum::Int64(process_id),
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
            CatalogItem::Log(_) => {
                self.pack_source_update(id, oid, schema_id, name, "log", None, None, diff)
            }
            CatalogItem::Index(index) => self.pack_index_update(id, oid, name, index, diff),
            CatalogItem::Table(_) => self.pack_table_update(id, oid, schema_id, name, diff),
            CatalogItem::Source(source) => self.pack_source_update(
                id,
                oid,
                schema_id,
                name,
                source.source_desc.name(),
                source.connection_id,
                match &source.host_config {
                    StorageHostConfig::Remote { .. } => None,
                    StorageHostConfig::Managed { size, .. } => Some(size),
                },
                diff,
            ),
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
            CatalogItem::StorageCollection(_) => self.pack_source_update(
                id,
                oid,
                schema_id,
                name,
                "storage collection",
                None,
                None,
                diff,
            ),
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
                    mz_storage::types::connections::Connection::Kafka { .. } => "kafka",
                    mz_storage::types::connections::Connection::Csr { .. } => {
                        "confluent-schema-registry"
                    }
                    mz_storage::types::connections::Connection::Postgres { .. } => "postgres",
                    mz_storage::types::connections::Connection::Aws(..) => "aws",
                    mz_storage::types::connections::Connection::Ssh { .. } => "ssh-tunnel",
                }),
            ]),
            diff,
        }];
        match connection.connection {
            mz_storage::types::connections::Connection::Ssh(ref ssh) => {
                if let Some(public_keypair) = ssh.public_keys.as_ref() {
                    updates.extend(self.pack_ssh_tunnel_connection_update(
                        id,
                        name,
                        public_keypair,
                        diff,
                    ));
                } else {
                    tracing::error!("does this even happen?");
                }
            }
            mz_storage::types::connections::Connection::Kafka(ref kafka) => {
                updates.extend(self.pack_kafka_connection_update(id, kafka, diff));
            }
            mz_storage::types::connections::Connection::Csr(_)
            | mz_storage::types::connections::Connection::Postgres(_)
            | mz_storage::types::connections::Connection::Aws(_) => {}
        };
        updates
    }

    pub(crate) fn pack_ssh_tunnel_connection_update(
        &self,
        id: GlobalId,
        name: &str,
        (public_key_primary, public_key_secondary): &(String, String),
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        vec![BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_SSH_TUNNEL_CONNECTIONS),
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::String(name),
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
                kafka.brokers.iter().map(|id| Datum::String(id)),
            )
            .unwrap();
        let brokers = row.unpack_first();
        vec![BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_KAFKA_CONNECTIONS),
            row: Row::pack_slice(&[Datum::String(&id.to_string()), brokers, progress_topic]),
            diff,
        }]
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
                Datum::UInt64(mview.compute_instance),
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
            }
            updates.push(BuiltinTableUpdate {
                id: self.resolve_builtin_table(&MZ_SINKS),
                row: Row::pack_slice(&[
                    Datum::String(&id.to_string()),
                    Datum::UInt32(oid),
                    Datum::UInt64(schema_id.into()),
                    Datum::String(name),
                    Datum::String(connection.name()),
                    Datum::from(sink.connection_id.map(|id| id.to_string()).as_deref()),
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
                Datum::UInt64(index.compute_instance),
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
            let arg_ids = func_impl_details
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
                    arg_ids.iter().map(|id| Datum::String(id)),
                )
                .unwrap();
            let arg_ids = row.unpack_first();

            updates.push(BuiltinTableUpdate {
                id: self.resolve_builtin_table(&MZ_FUNCTIONS),
                row: Row::pack_slice(&[
                    Datum::String(&id.to_string()),
                    Datum::UInt32(func_impl_details.oid),
                    Datum::UInt64(schema_id.into()),
                    Datum::String(name),
                    arg_ids,
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
        let (event_type, object_type, event_details, user, occurred_at): (
            &EventType,
            &ObjectType,
            &EventDetails,
            &Option<String>,
            u64,
        ) = match event {
            VersionedEvent::V1(ev) => (
                &ev.event_type,
                &ev.object_type,
                &ev.event_details,
                &ev.user,
                ev.occurred_at,
            ),
        };
        let event_details = Jsonb::from_serde_json(event_details.as_json())
            .map_err(|e| {
                Error::new(ErrorKind::Unstructured(format!(
                    "could not pack audit log update: {}",
                    e
                )))
            })?
            .into_row();
        let event_details = event_details.iter().next().unwrap();
        let dt = mz_ore::now::to_datetime(occurred_at).naive_utc();
        let id = event.sortable_id();
        Ok(BuiltinTableUpdate {
            id: self.resolve_builtin_table(&MZ_AUDIT_EVENTS),
            row: Row::pack_slice(&[
                Datum::UInt64(id),
                Datum::String(&format!("{}", event_type)),
                Datum::String(&format!("{}", object_type)),
                event_details,
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
        md: ReplicaMetadata,
        diff: Diff,
    ) -> BuiltinTableUpdate {
        let ReplicaMetadata { last_heartbeat } = md;
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
        event: &VersionedStorageUsage,
    ) -> Result<BuiltinTableUpdate, Error> {
        let (id, object_id, size_bytes, collection_timestamp): (u64, &Option<String>, u64, u64) =
            match event {
                VersionedStorageUsage::V1(ev) => {
                    (ev.id, &ev.object_id, ev.size_bytes, ev.collection_timestamp)
                }
            };

        let table = self.resolve_builtin_table(&MZ_STORAGE_USAGE);
        let object_id_val = match object_id {
            Some(s) => Datum::String(s),
            None => Datum::Null,
        };
        let dt = mz_ore::now::to_datetime(collection_timestamp).naive_utc();

        let row = Row::pack_slice(&[
            Datum::UInt64(id),
            object_id_val,
            Datum::UInt64(size_bytes),
            Datum::TimestampTz(DateTime::from_utc(dt, Utc).try_into().expect("must fit")),
        ]);
        let diff = 1;
        Ok(BuiltinTableUpdate {
            id: table,
            row,
            diff,
        })
    }
}
