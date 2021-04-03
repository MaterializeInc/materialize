// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryFrom;
use std::os::unix::ffi::OsStringExt;

use dataflow_types::{AvroOcfSinkConnector, KafkaSinkConnector};
use expr::{GlobalId, MirScalarExpr};
use ore::collections::CollectionExt;
use repr::adt::array::ArrayDimension;
use repr::{Datum, Row};
use sql::ast::{CreateIndexStatement, Statement};
use sql::names::DatabaseSpecifier;

use crate::catalog::builtin::{
    MZ_ARRAY_TYPES, MZ_AVRO_OCF_SINKS, MZ_BASE_TYPES, MZ_COLUMNS, MZ_DATABASES, MZ_FUNCTIONS,
    MZ_INDEXES, MZ_INDEX_COLUMNS, MZ_KAFKA_SINKS, MZ_LIST_TYPES, MZ_MAP_TYPES, MZ_PSEUDO_TYPES,
    MZ_ROLES, MZ_SCHEMAS, MZ_SINKS, MZ_SOURCES, MZ_TABLES, MZ_TYPES, MZ_VIEWS,
};
use crate::catalog::{
    Catalog, CatalogItem, Func, Index, Sink, SinkConnector, SinkConnectorState, Type, TypeInner,
    SYSTEM_CONN_ID,
};

/// An update to a built-in table.
#[derive(Debug)]
pub struct BuiltinTableUpdate {
    /// The ID of the table to update.
    pub id: GlobalId,
    /// The data to put into the table.
    pub row: Row,
    /// The diff of the data.
    pub diff: isize,
}

impl Catalog {
    pub(super) fn pack_database_update(&self, name: &str, diff: isize) -> BuiltinTableUpdate {
        let database = &self.by_name[name];
        BuiltinTableUpdate {
            id: MZ_DATABASES.id,
            row: Row::pack_slice(&[
                Datum::Int64(database.id),
                Datum::Int32(database.oid as i32),
                Datum::String(&name),
            ]),
            diff,
        }
    }

    pub(super) fn pack_schema_update(
        &self,
        database_spec: &DatabaseSpecifier,
        schema_name: &str,
        diff: isize,
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
                Datum::Int32(schema.oid as i32),
                Datum::from(database_id),
                Datum::String(schema_name),
            ]),
            diff,
        }
    }

    pub(super) fn pack_role_update(&self, name: &str, diff: isize) -> BuiltinTableUpdate {
        let role = &self.roles[name];
        BuiltinTableUpdate {
            id: MZ_ROLES.id,
            row: Row::pack_slice(&[
                Datum::Int64(role.id),
                Datum::Int32(role.oid as i32),
                Datum::String(&name),
            ]),
            diff,
        }
    }

    pub(super) fn pack_item_update(&self, id: GlobalId, diff: isize) -> Vec<BuiltinTableUpdate> {
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
            CatalogItem::Table(_) => self.pack_table_update(id, oid, schema_id, name, diff),
            CatalogItem::Source(_) => self.pack_source_update(id, oid, schema_id, name, diff),
            CatalogItem::View(_) => self.pack_view_update(id, oid, schema_id, name, diff),
            CatalogItem::Sink(sink) => self.pack_sink_update(id, oid, schema_id, name, sink, diff),
            CatalogItem::Type(ty) => self.pack_type_update(id, oid, schema_id, name, ty, diff),
            CatalogItem::Func(func) => self.pack_func_update(id, schema_id, name, func, diff),
        };

        if let Ok(desc) = entry.desc() {
            for (i, (column_name, column_type)) in desc.iter().enumerate() {
                updates.push(BuiltinTableUpdate {
                    id: MZ_COLUMNS.id,
                    row: Row::pack_slice(&[
                        Datum::String(&id.to_string()),
                        Datum::String(
                            &column_name
                                .map(|n| n.to_string())
                                .unwrap_or_else(|| "?column?".to_owned()),
                        ),
                        Datum::Int64(i as i64 + 1),
                        Datum::from(column_type.nullable),
                        Datum::String(pgrepr::Type::from(&column_type.scalar_type).name()),
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
        diff: isize,
    ) -> Vec<BuiltinTableUpdate> {
        vec![BuiltinTableUpdate {
            id: MZ_TABLES.id,
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::Int32(oid as i32),
                Datum::Int64(schema_id),
                Datum::String(name),
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
        diff: isize,
    ) -> Vec<BuiltinTableUpdate> {
        vec![BuiltinTableUpdate {
            id: MZ_SOURCES.id,
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::Int32(oid as i32),
                Datum::Int64(schema_id),
                Datum::String(name),
                Datum::String(self.is_volatile(id).as_str()),
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
        diff: isize,
    ) -> Vec<BuiltinTableUpdate> {
        vec![BuiltinTableUpdate {
            id: MZ_VIEWS.id,
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::Int32(oid as i32),
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
        diff: isize,
    ) -> Vec<BuiltinTableUpdate> {
        let mut updates = vec![];
        if let Sink {
            connector: SinkConnectorState::Ready(connector),
            ..
        } = sink
        {
            match connector {
                SinkConnector::Kafka(KafkaSinkConnector { topic, .. }) => {
                    updates.push(BuiltinTableUpdate {
                        id: MZ_KAFKA_SINKS.id,
                        row: Row::pack_slice(&[
                            Datum::String(&id.to_string()),
                            Datum::String(topic.as_str()),
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
                    Datum::Int32(oid as i32),
                    Datum::Int64(schema_id),
                    Datum::String(name),
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
        diff: isize,
    ) -> Vec<BuiltinTableUpdate> {
        let mut updates = vec![];

        let key_sqls = match sql::parse::parse(&index.create_sql)
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
                Datum::Int32(oid as i32),
                Datum::String(name),
                Datum::String(&index.on.to_string()),
                Datum::String(self.is_volatile(id).as_str()),
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
        diff: isize,
    ) -> Vec<BuiltinTableUpdate> {
        let generic_update = BuiltinTableUpdate {
            id: MZ_TYPES.id,
            row: Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::Int32(oid as i32),
                Datum::Int64(schema_id),
                Datum::String(name),
            ]),
            diff,
        };

        let (index_id, update) = match typ.inner {
            TypeInner::Array { element_id } => (
                MZ_ARRAY_TYPES.id,
                vec![id.to_string(), element_id.to_string()],
            ),
            TypeInner::Base => (MZ_BASE_TYPES.id, vec![id.to_string()]),
            TypeInner::List { element_id } => (
                MZ_LIST_TYPES.id,
                vec![id.to_string(), element_id.to_string()],
            ),
            TypeInner::Map { key_id, value_id } => (
                MZ_MAP_TYPES.id,
                vec![id.to_string(), key_id.to_string(), value_id.to_string()],
            ),
            TypeInner::Pseudo => (MZ_PSEUDO_TYPES.id, vec![id.to_string()]),
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
        diff: isize,
    ) -> Vec<BuiltinTableUpdate> {
        let mut updates = vec![];
        for func_impl_details in func.inner.func_impls() {
            let arg_ids = func_impl_details
                .arg_oids
                .iter()
                .map(|oid| self.get_by_oid(oid).id().to_string())
                .collect::<Vec<_>>();
            let mut row = Row::default();
            row.push_array(
                &[ArrayDimension {
                    lower_bound: 1,
                    length: arg_ids.len(),
                }],
                arg_ids.iter().map(|id| Datum::String(&id)),
            )
            .unwrap();
            let arg_ids = row.unpack_first();

            let variadic_id = match func_impl_details.variadic_oid {
                Some(oid) => Some(self.get_by_oid(&oid).id().to_string()),
                None => None,
            };

            updates.push(BuiltinTableUpdate {
                id: MZ_FUNCTIONS.id,
                row: Row::pack_slice(&[
                    Datum::String(&id.to_string()),
                    Datum::Int32(func_impl_details.oid as i32),
                    Datum::Int64(schema_id),
                    Datum::String(name),
                    arg_ids,
                    Datum::from(variadic_id.as_deref()),
                ]),
                diff,
            });
        }
        updates
    }
}
