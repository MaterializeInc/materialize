// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::os::unix::ffi::OsStringExt;

use mz_dataflow_types::sinks::{AvroOcfSinkConnector, KafkaSinkConnector};
use mz_expr::{GlobalId, MirScalarExpr};
use mz_ore::collections::CollectionExt;
use mz_repr::adt::array::ArrayDimension;
use mz_repr::{Datum, Diff, Row};
use mz_sql::ast::{CreateIndexStatement, Statement};
use mz_sql::catalog::CatalogType;
use mz_sql::names::DatabaseSpecifier;
use mz_sql_parser::ast::display::AstDisplay;

use crate::catalog::builtin::{
    MZ_ARRAY_TYPES, MZ_AVRO_OCF_SINKS, MZ_BASE_TYPES, MZ_COLUMNS, MZ_DATABASES, MZ_FUNCTIONS,
    MZ_INDEXES, MZ_INDEX_COLUMNS, MZ_KAFKA_SINKS, MZ_LIST_TYPES, MZ_MAP_TYPES, MZ_PSEUDO_TYPES,
    MZ_ROLES, MZ_SCHEMAS, MZ_SINKS, MZ_SOURCES, MZ_TABLES, MZ_TYPES, MZ_VIEWS,
};
use crate::catalog::{
    CatalogItem, CatalogState, Func, Index, Sink, SinkConnector, SinkConnectorState, Source, Table,
    Type, SYSTEM_CONN_ID,
};

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
    pub(super) fn pack_database_update(&self, name: &str, diff: Diff) -> BuiltinTableUpdate {
        let database = &self.by_name[name];
        BuiltinTableUpdate {
            id: MZ_DATABASES.id,
            row: Row::pack_slice(&[
                Datum::Int64(database.id),
                Datum::UInt32(database.oid),
                Datum::String(&name),
            ]),
            diff,
        }
    }

    pub(super) fn pack_schema_update(
        &self,
        database_spec: &DatabaseSpecifier,
        schema_name: &str,
        diff: Diff,
    ) -> BuiltinTableUpdate {
        let (database_id, schema) = match database_spec {
            DatabaseSpecifier::Ambient => (None, &self.ambient_schemas[schema_name]),
            DatabaseSpecifier::Name(name) => {
                let db = &self.by_name[name];
                (Some(db.id), &db.schemas[schema_name])
            }
        };
        BuiltinTableUpdate {
            id: MZ_SCHEMAS.id,
            row: Row::pack_slice(&[
                Datum::Int64(schema.id),
                Datum::UInt32(schema.oid),
                Datum::from(database_id),
                Datum::String(schema_name),
            ]),
            diff,
        }
    }

    pub(super) fn pack_role_update(&self, name: &str, diff: Diff) -> BuiltinTableUpdate {
        let role = &self.roles[name];
        BuiltinTableUpdate {
            id: MZ_ROLES.id,
            row: Row::pack_slice(&[
                Datum::Int64(role.id),
                Datum::UInt32(role.oid),
                Datum::String(&name),
            ]),
            diff,
        }
    }

    pub(super) fn pack_item_update(&self, id: GlobalId, diff: Diff) -> Vec<BuiltinTableUpdate> {
        let entry = self.get_by_id(&id);
        let id = entry.id();
        let oid = entry.oid();
        let conn_id = entry.item().conn_id().unwrap_or(SYSTEM_CONN_ID);
        let schema_id = self
            .get_schema(&entry.name().database, &entry.name().schema, conn_id)
            .unwrap()
            .id;
        let name = &entry.name().item;
        let mut updates = match entry.item() {
            CatalogItem::Index(index) => self.pack_index_update(id, oid, name, index, diff),
            CatalogItem::Table(table) => {
                self.pack_table_update(id, oid, schema_id, name, table, diff)
            }
            CatalogItem::Source(source) => {
                self.pack_source_update(id, oid, schema_id, name, source, diff)
            }
            CatalogItem::View(_) => self.pack_view_update(id, oid, schema_id, name, diff),
            CatalogItem::Sink(sink) => self.pack_sink_update(id, oid, schema_id, name, sink, diff),
            CatalogItem::Type(ty) => self.pack_type_update(id, oid, schema_id, name, ty, diff),
            CatalogItem::Func(func) => self.pack_func_update(id, schema_id, name, func, diff),
        };

        if let Ok(desc) = entry.desc() {
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
                    id: MZ_COLUMNS.id,
                    row: Row::pack_slice(&[
                        Datum::String(&id.to_string()),
                        Datum::String(column_name.as_str()),
                        Datum::Int64(i as i64 + 1),
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
        schema_id: i64,
        name: &str,
        table: &Table,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        vec![BuiltinTableUpdate {
            id: MZ_TABLES.id,
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::Int64(schema_id),
                Datum::String(name),
                Datum::from(table.persist_name.as_deref()),
            ]),
            diff,
        }]
    }

    fn pack_source_update(
        &self,
        id: GlobalId,
        oid: u32,
        schema_id: i64,
        name: &str,
        source: &Source,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        let persist_name = source
            .persist_details
            .as_ref()
            .map(|persist| &*persist.primary_stream);
        vec![BuiltinTableUpdate {
            id: MZ_SOURCES.id,
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::Int64(schema_id),
                Datum::String(name),
                Datum::String(source.connector.name()),
                Datum::String(self.is_volatile(id).as_str()),
                Datum::from(persist_name),
            ]),
            diff,
        }]
    }

    fn pack_view_update(
        &self,
        id: GlobalId,
        oid: u32,
        schema_id: i64,
        name: &str,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        vec![BuiltinTableUpdate {
            id: MZ_VIEWS.id,
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::Int64(schema_id),
                Datum::String(name),
                Datum::String(self.is_volatile(id).as_str()),
            ]),
            diff,
        }]
    }

    fn pack_sink_update(
        &self,
        id: GlobalId,
        oid: u32,
        schema_id: i64,
        name: &str,
        sink: &Sink,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        let mut updates = vec![];
        if let Sink {
            connector: SinkConnectorState::Ready(connector),
            ..
        } = sink
        {
            match connector {
                SinkConnector::Kafka(KafkaSinkConnector {
                    topic, consistency, ..
                }) => {
                    let consistency_topic = if let Some(consistency) = consistency {
                        Datum::String(consistency.topic.as_str())
                    } else {
                        Datum::Null
                    };
                    updates.push(BuiltinTableUpdate {
                        id: MZ_KAFKA_SINKS.id,
                        row: Row::pack_slice(&[
                            Datum::String(&id.to_string()),
                            Datum::String(topic.as_str()),
                            consistency_topic,
                        ]),
                        diff,
                    });
                }
                SinkConnector::AvroOcf(AvroOcfSinkConnector { path, .. }) => {
                    updates.push(BuiltinTableUpdate {
                        id: MZ_AVRO_OCF_SINKS.id,
                        row: Row::pack_slice(&[
                            Datum::String(&id.to_string()),
                            Datum::Bytes(&path.clone().into_os_string().into_vec()),
                        ]),
                        diff,
                    });
                }
                _ => (),
            }
            updates.push(BuiltinTableUpdate {
                id: MZ_SINKS.id,
                row: Row::pack_slice(&[
                    Datum::String(&id.to_string()),
                    Datum::UInt32(oid),
                    Datum::Int64(schema_id),
                    Datum::String(name),
                    Datum::String(connector.name()),
                    Datum::String(self.is_volatile(id).as_str()),
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
            id: MZ_INDEXES.id,
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::String(name),
                Datum::String(&index.on.to_string()),
                Datum::String(self.is_volatile(id).as_str()),
                Datum::from(index.enabled),
            ]),
            diff,
        });

        for (i, key) in index.keys.iter().enumerate() {
            let nullable = key
                .typ(self.get_by_id(&index.on).desc().unwrap().typ())
                .nullable;
            let seq_in_index = i64::try_from(i + 1).expect("invalid index sequence number");
            let key_sql = key_sqls
                .get(i)
                .expect("missing sql information for index key")
                .to_string();
            let (field_number, expression) = match key {
                MirScalarExpr::Column(col) => (
                    Datum::Int64(i64::try_from(*col + 1).expect("invalid index column number")),
                    Datum::Null,
                ),
                _ => (Datum::Null, Datum::String(&key_sql)),
            };
            updates.push(BuiltinTableUpdate {
                id: MZ_INDEX_COLUMNS.id,
                row: Row::pack_slice(&[
                    Datum::String(&id.to_string()),
                    Datum::Int64(seq_in_index),
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
        schema_id: i64,
        name: &str,
        typ: &Type,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        let generic_update = BuiltinTableUpdate {
            id: MZ_TYPES.id,
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::UInt32(oid),
                Datum::Int64(schema_id),
                Datum::String(name),
            ]),
            diff,
        };

        let (index_id, update) = match typ.details.typ {
            CatalogType::Array { element_id } => (
                MZ_ARRAY_TYPES.id,
                vec![id.to_string(), element_id.to_string()],
            ),
            CatalogType::List { element_id } => (
                MZ_LIST_TYPES.id,
                vec![id.to_string(), element_id.to_string()],
            ),
            CatalogType::Map { key_id, value_id } => (
                MZ_MAP_TYPES.id,
                vec![id.to_string(), key_id.to_string(), value_id.to_string()],
            ),
            CatalogType::Pseudo => (MZ_PSEUDO_TYPES.id, vec![id.to_string()]),
            _ => (MZ_BASE_TYPES.id, vec![id.to_string()]),
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
        schema_id: i64,
        name: &str,
        func: &Func,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate> {
        let mut updates = vec![];
        for func_impl_details in func.inner.func_impls() {
            let arg_ids = func_impl_details
                .arg_oids
                .iter()
                .map(|oid| self.get_by_oid(oid).id().to_string())
                .collect::<Vec<_>>();
            let mut row = Row::default();
            row.packer()
                .push_array(
                    &[ArrayDimension {
                        lower_bound: 1,
                        length: arg_ids.len(),
                    }],
                    arg_ids.iter().map(|id| Datum::String(&id)),
                )
                .unwrap();
            let arg_ids = row.unpack_first();

            let variadic_id = func_impl_details
                .variadic_oid
                .map(|oid| self.get_by_oid(&oid).id().to_string());

            let ret_id = func_impl_details
                .return_oid
                .map(|oid| self.get_by_oid(&oid).id().to_string());

            updates.push(BuiltinTableUpdate {
                id: MZ_FUNCTIONS.id,
                row: Row::pack_slice(&[
                    Datum::String(&id.to_string()),
                    Datum::UInt32(func_impl_details.oid),
                    Datum::Int64(schema_id),
                    Datum::String(name),
                    arg_ids,
                    Datum::from(variadic_id.as_deref()),
                    Datum::from(ret_id.as_deref()),
                    func_impl_details.return_is_set.into(),
                ]),
                diff,
            });
        }
        updates
    }
}
