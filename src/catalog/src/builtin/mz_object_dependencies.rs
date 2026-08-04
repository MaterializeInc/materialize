// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The `mz_internal.mz_object_dependencies` materialized view and the
//! generated view it reads.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::LazyLock;

use itertools::Itertools;
use mz_catalog_protos::objects::CatalogItemType as ProtoCatalogItemType;
use mz_ore::collections::CollectionExt;
use mz_pgrepr::oid;
use mz_repr::namespaces::MZ_INTERNAL_SCHEMA;
use mz_repr::{RelationDesc, SemanticType, SqlScalarType};
use mz_sql::catalog::{CatalogType, NameReference};
use mz_sql_parser::ast::UnresolvedItemName;
use mz_sql_parser::ast::item_refs::collect_item_references;

use super::{
    Builtin, BuiltinMaterializedView, BuiltinView, LinkProperties, Ontology, OntologyLink,
    PUBLIC_SELECT, assert_safe_builtin_name,
};

pub(super) const MZ_OBJECT_DEPENDENCIES_RAW: &str = "mz_object_dependencies_raw";

/// The durable encoding of a `GidMapping` key's `object_type`, as it appears in
/// `mz_catalog_raw`'s JSON.
fn object_type_code(object_type: ProtoCatalogItemType) -> String {
    serde_json::to_string(&object_type).expect("CatalogItemType is serializable")
}

/// Row of a builtin
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct BuiltinEdgeRow {
    object_schema: String,
    object_name: String,
    object_type: String,
    ref_schema: String,
    ref_name: String,
    ref_kind: &'static str,
}

impl BuiltinEdgeRow {
    /// Renders the row as a SQL `VALUES` tuple.
    ///
    /// The literals are not escaped. `BuiltinEdgeCollector::collect` runs
    /// every name through `assert_safe_builtin_name` before building a row.
    fn to_sql_row(&self) -> String {
        let BuiltinEdgeRow {
            object_schema,
            object_name,
            object_type,
            ref_schema,
            ref_name,
            ref_kind,
        } = self;
        format!(
            "('{object_schema}', '{object_name}', '{object_type}', \
             '{ref_schema}', '{ref_name}', '{ref_kind}')"
        )
    }
}

/// Collects builtin dependency edges by parsing builtin SQL.
struct BuiltinEdgeCollector<'a> {
    /// Builtin name to its schema, for resolving unqualified references.
    schema_by_name: BTreeMap<&'a str, &'a str>,
    /// Element type name to the `(schema, name)` of its array type. e.g. `T[]`
    /// maps to the qualified Postgres compatible type `_T`.
    array_type_by_elem: BTreeMap<&'a str, (&'a str, &'a str)>,
    rows: BTreeSet<BuiltinEdgeRow>,
}

impl<'a> BuiltinEdgeCollector<'a> {
    fn new(builtins: &'a [Builtin<NameReference>]) -> Self {
        let mut schema_by_name: BTreeMap<&str, &str> = BTreeMap::new();
        for b in builtins {
            if let Some(prev) = schema_by_name.insert(b.name(), b.schema()) {
                assert_eq!(
                    prev,
                    b.schema(),
                    "builtin name {} appears in multiple schemas; unqualified references are ambiguous",
                    b.name()
                );
            }
        }

        let mut array_type_by_elem: BTreeMap<&str, (&str, &str)> = BTreeMap::new();
        for b in builtins {
            if let Builtin::Type(t) = b {
                if let CatalogType::Array { element_reference } = &t.details.typ {
                    array_type_by_elem.insert(element_reference, (t.schema, t.name));
                }
            }
        }

        BuiltinEdgeCollector {
            schema_by_name,
            array_type_by_elem,
            rows: BTreeSet::new(),
        }
    }

    fn resolve(&self, object: &str, name: &UnresolvedItemName) -> (String, String) {
        match &name.0[..] {
            [.., schema, item] => (schema.as_str().to_string(), item.as_str().to_string()),
            [item] => {
                let item = item.as_str();
                let schema = self.schema_by_name.get(item).unwrap_or_else(|| {
                    panic!(
                        "cannot resolve unqualified reference {item:?} in builtin {object}; \
                         qualify the reference or check that the referenced builtin exists"
                    )
                });
                (schema.to_string(), item.to_string())
            }
            [] => panic!("empty item reference in builtin {object}"),
        }
    }

    /// Parses `create_sql` and records one row per catalog item it references.
    fn collect(
        &mut self,
        object_schema: &str,
        object_name: &str,
        object_type: ProtoCatalogItemType,
        create_sql: &str,
    ) {
        assert_safe_builtin_name(object_schema, "object schema");
        assert_safe_builtin_name(object_name, "object");

        let stmt = mz_sql::parse::parse(create_sql)
            .unwrap_or_else(|e| panic!("invalid sql for builtin {object_name}: {e}"))
            .into_element()
            .ast;
        let refs = collect_item_references(&stmt);

        // Builtin SQL is name-based. An id reference here means somebody
        // hardcoded a system id, which is not stable across versions.
        assert!(
            refs.ids.is_empty(),
            "builtin {} references items by id: {:?}",
            object_name,
            refs.ids
        );

        let mut referenced: Vec<(String, String, &'static str)> = Vec::new();
        for item_name in &refs.named_relations {
            let (schema, name) = self.resolve(object_name, item_name);
            referenced.push((schema, name, "rel"));
        }
        for item_name in &refs.named_funcs {
            let (schema, name) = self.resolve(object_name, item_name);
            referenced.push((schema, name, "func"));
        }
        for item_name in &refs.named_types {
            let (schema, name) = self.resolve(object_name, item_name);
            referenced.push((schema, name, "type"));
        }
        // Map the array type `T[]` to its Postgres compatible type `_T`.
        for elem in &refs.named_array_elements {
            // `T[]` references the array type paired with `T`, not `T` itself.
            let (_, name) = self.resolve(object_name, elem);
            let (array_schema, array_name) = self
                .array_type_by_elem
                .get(name.as_str())
                .unwrap_or_else(|| {
                    panic!(
                        "builtin {object_name} uses {name}[], but no builtin array type has \
                         element {name}"
                    )
                });
            referenced.push((array_schema.to_string(), array_name.to_string(), "type"));
        }

        let object_type = object_type_code(object_type);
        for (ref_schema, ref_name, ref_kind) in referenced {
            assert_safe_builtin_name(&ref_schema, "referenced schema");
            assert_safe_builtin_name(&ref_name, "referenced object");
            assert!(
                !(object_schema == ref_schema && object_name == ref_name),
                "builtin {object_schema}.{object_name} references itself"
            );
            self.rows.insert(BuiltinEdgeRow {
                object_schema: object_schema.to_string(),
                object_name: object_name.to_string(),
                object_type: object_type.clone(),
                ref_schema,
                ref_name,
                ref_kind,
            });
        }
    }
}

/// Renders the `mz_object_dependencies_raw` view body with `rows` inlined as
/// the builtin edge table.
fn mz_object_dependencies_raw_sql(rows: &BTreeSet<BuiltinEdgeRow>) -> String {
    let builtin_ref_values = rows.iter().map(BuiltinEdgeRow::to_sql_row).join(",");

    let func_type = object_type_code(ProtoCatalogItemType::Func);
    let type_type = object_type_code(ProtoCatalogItemType::Type);
    let source_type = object_type_code(ProtoCatalogItemType::Source);
    // Every kind a name in relation position can denote.
    let relation_types = format!("NOT IN ('{type_type}', '{func_type}')");

    format!(
        "
WITH
    user_items AS (
        SELECT
            mz_internal.parse_catalog_id(data->'key'->'gid') AS id,
            mz_internal.parse_catalog_item_references(data->'value'->'definition'->'V1'->>'create_sql') AS refs
        FROM mz_internal.mz_catalog_raw
        WHERE
            data->>'kind' = 'Item' AND
            -- Exclude temporary objects
            data->'value'->>'ephemeral_owner_session' IS NULL
    ),
    gid_mappings AS (
        SELECT
            's' || (data->'value'->>'catalog_id') AS id,
            data->'key'->>'schema_name' AS schema_name,
            data->'key'->>'object_name' AS object_name,
            data->'key'->>'object_type' AS object_type
        FROM mz_internal.mz_catalog_raw
        WHERE data->>'kind' = 'GidMapping'
    ),
    user_id_edges AS (
        SELECT u.id AS object_id, r.ref AS referenced_object_id
        FROM user_items u
        CROSS JOIN LATERAL jsonb_array_elements_text(u.refs->'ids') AS r(ref)
    ),
    user_func_edges AS (
        SELECT u.id AS object_id, gm.id AS referenced_object_id
        FROM user_items u
        CROSS JOIN LATERAL jsonb_array_elements(u.refs->'named_funcs') AS f(func)
        JOIN gid_mappings gm ON
            gm.object_type = '{func_type}' AND
            gm.schema_name = f.func->>'schema' AND
            gm.object_name = f.func->>'name'
    ),
    user_type_edges AS (
        SELECT u.id AS object_id, gm.id AS referenced_object_id
        FROM user_items u
        CROSS JOIN LATERAL jsonb_array_elements(u.refs->'named_types') AS t(typ)
        JOIN gid_mappings gm ON
            gm.object_type = '{type_type}' AND
            gm.schema_name = t.typ->>'schema' AND
            gm.object_name = t.typ->>'name'
    ),
    user_relation_edges AS (
        SELECT u.id AS object_id, gm.id AS referenced_object_id
        FROM user_items u
        CROSS JOIN LATERAL jsonb_array_elements(u.refs->'named_relations') AS n(rel)
        JOIN gid_mappings gm ON
            gm.object_type {relation_types} AND
            gm.schema_name = n.rel->>'schema' AND
            gm.object_name = n.rel->>'name'
    ),
    builtin_edges AS (
        SELECT obj.id AS object_id, ref.id AS referenced_object_id
        FROM
            (VALUES {builtin_ref_values})
                AS bv(object_schema, object_name, object_type, ref_schema, ref_name, ref_kind)
            JOIN gid_mappings obj ON
                obj.schema_name = bv.object_schema AND
                obj.object_name = bv.object_name AND
                obj.object_type = bv.object_type
            JOIN gid_mappings ref ON
                ref.schema_name = bv.ref_schema AND
                ref.object_name = bv.ref_name AND
                CASE bv.ref_kind
                    WHEN 'func' THEN ref.object_type = '{func_type}'
                    WHEN 'type' THEN ref.object_type = '{type_type}'
                    ELSE ref.object_type {relation_types}
                END
    ),
    introspection_source_index_edges AS (
        SELECT
            'si' || (isi.data->'value'->>'catalog_id') AS object_id,
            's' || (gm.data->'value'->>'catalog_id') AS referenced_object_id
        FROM mz_internal.mz_catalog_raw AS isi
        JOIN mz_internal.mz_catalog_raw AS gm ON
            gm.data->>'kind' = 'GidMapping' AND
            gm.data->'key'->>'object_type' = '{source_type}' AND
            gm.data->'key'->>'schema_name' = 'mz_introspection' AND
            gm.data->'key'->>'object_name' = isi.data->'key'->>'name'
        WHERE isi.data->>'kind' = 'ClusterIntrospectionSourceIndex'
    )
SELECT object_id, referenced_object_id FROM user_id_edges
UNION ALL
SELECT object_id, referenced_object_id FROM user_func_edges
UNION ALL
SELECT object_id, referenced_object_id FROM user_type_edges
UNION ALL
SELECT object_id, referenced_object_id FROM user_relation_edges
UNION ALL
SELECT object_id, referenced_object_id FROM builtin_edges
UNION ALL
SELECT object_id, referenced_object_id FROM introspection_source_index_edges
"
    )
}

/// Generate the `mz_internal.mz_object_dependencies_raw` builtin view with
/// builtin dependency edges inlined as VALUES clauses.
///
/// The view unions these edge sources:
///
/// - User items: references extracted from stored `create_sql` via `parse_catalog_item_references`.
///   Id references are used directly. Function references print as plain qualified names in stored
///   SQL, so they are recovered by joining `(schema, name)` against `GidMapping` rows. Temporary
///   items are excluded.
/// - Builtin items: name references joined to `GidMapping` at query time.
/// - Introspection source indexes: `si<id> -> s<log id>` edges from
///   `ClusterIntrospectionSourceIndex` entries.
pub(super) fn make_mz_object_dependencies_raw(builtins: &[Builtin<NameReference>]) -> BuiltinView {
    let mut collector = BuiltinEdgeCollector::new(builtins);
    for b in builtins {
        let (object_type, create_sql) = match b {
            Builtin::View(v) => (ProtoCatalogItemType::View, v.create_sql()),
            Builtin::MaterializedView(mv) => {
                (ProtoCatalogItemType::MaterializedView, mv.create_sql())
            }
            Builtin::Index(i) => (ProtoCatalogItemType::Index, i.create_sql()),
            // The user_items branch already covers runtime alterable connections, thus we skip as
            // to not double-count.
            Builtin::Connection(c) if c.runtime_alterable => continue,
            Builtin::Connection(c) => (ProtoCatalogItemType::Connection, c.sql.to_string()),
            // Skip objects constructed without SQL.
            Builtin::Log(_)
            | Builtin::Table(_)
            | Builtin::Type(_)
            | Builtin::Func(_)
            | Builtin::Source(_) => continue,
        };
        collector.collect(b.schema(), b.name(), object_type, &create_sql);
    }

    // We include `mz_object_dependencies` own edges into `mz_object_dependencies`.
    let self_edges = |sql: &str| {
        let mut collector = BuiltinEdgeCollector::new(builtins);
        collector.collect(
            MZ_INTERNAL_SCHEMA,
            MZ_OBJECT_DEPENDENCIES_RAW,
            ProtoCatalogItemType::View,
            &format!("CREATE VIEW {MZ_INTERNAL_SCHEMA}.{MZ_OBJECT_DEPENDENCIES_RAW} AS {sql}"),
        );
        collector.rows
    };

    let self_rows = self_edges(&mz_object_dependencies_raw_sql(&collector.rows));
    let mut rows = collector.rows;
    rows.extend(self_rows.iter().cloned());
    let sql = mz_object_dependencies_raw_sql(&rows);

    BuiltinView {
        name: MZ_OBJECT_DEPENDENCIES_RAW,
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_MZ_OBJECT_DEPENDENCIES_RAW_OID,
        desc: RelationDesc::builder()
            .with_column("object_id", SqlScalarType::String.nullable(true))
            .with_column("referenced_object_id", SqlScalarType::String.nullable(true))
            .finish(),
        column_comments: BTreeMap::from_iter([
            (
                "object_id",
                "The ID of the dependent object. Corresponds to `mz_objects.id`.",
            ),
            (
                "referenced_object_id",
                "The ID of the referenced object. Corresponds to `mz_objects.id`.",
            ),
        ]),
        sql: Box::leak(sql.into_boxed_str()),
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
}

pub static MZ_OBJECT_DEPENDENCIES: LazyLock<BuiltinMaterializedView> =
    LazyLock::new(|| BuiltinMaterializedView {
        name: "mz_object_dependencies",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::MV_MZ_OBJECT_DEPENDENCIES_OID,
        desc: RelationDesc::builder()
            .with_column("object_id", SqlScalarType::String.nullable(false))
            .with_column(
                "referenced_object_id",
                SqlScalarType::String.nullable(false),
            )
            .finish(),
        column_comments: BTreeMap::from_iter([
            (
                "object_id",
                "The ID of the dependent object. Corresponds to `mz_objects.id`.",
            ),
            (
                "referenced_object_id",
                "The ID of the referenced object. Corresponds to `mz_objects.id`.",
            ),
        ]),
        sql: "
IN CLUSTER mz_catalog_server
WITH (
    ASSERT NOT NULL object_id,
    ASSERT NOT NULL referenced_object_id
) AS
SELECT object_id, referenced_object_id
FROM mz_internal.mz_object_dependencies_raw",
        is_retained_metrics_object: true,
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "object_dependency",
            description: "A dependency edge: one object depends on another",
            links: &const {
                [
                    OntologyLink {
                        name: "depends_on",
                        target: "object",
                        properties: LinkProperties::DependsOn {
                            source_column: "object_id",
                            target_column: "id",
                            source_id_type: Some(mz_repr::SemanticType::CatalogItemId),
                            requires_mapping: None,
                        },
                    },
                    OntologyLink {
                        name: "dependency_is",
                        target: "object",
                        properties: LinkProperties::DependsOn {
                            source_column: "referenced_object_id",
                            target_column: "id",
                            source_id_type: Some(mz_repr::SemanticType::CatalogItemId),
                            requires_mapping: None,
                        },
                    },
                ]
            },
            column_semantic_types: &const {
                [
                    ("object_id", SemanticType::CatalogItemId),
                    ("referenced_object_id", SemanticType::CatalogItemId),
                ]
            },
        }),
    });
