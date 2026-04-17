// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Catalog ontology views derived from existing builtin definitions.
//!
//! Enumerates builtins that have `ontology: Some(...)` and generates 4 views:
//! - entity_types: from ontology.description + RelationDesc::keys()
//! - properties: from mz_columns + mz_comments + semantic type inference
//! - semantic_types: small const reference data
//! - link_types: from ontology.links on each builtin

use std::collections::BTreeMap;

use mz_pgrepr::oid;
use mz_repr::namespaces::MZ_INTERNAL_SCHEMA;
use mz_repr::{RelationDesc, SqlScalarType};
use mz_sql::catalog::NameReference;

use super::{Builtin, BuiltinView, Ontology, PUBLIC_SELECT};

pub(super) fn generate_views(builtins: &[Builtin<NameReference>]) -> Vec<Builtin<NameReference>> {
    let infos: Vec<_> = builtins
        .iter()
        .filter_map(|b| {
            let (name, schema, desc, ontology) = match b {
                Builtin::Table(t) => (t.name, t.schema, &t.desc, t.ontology.as_ref()?),
                Builtin::View(v) => (v.name, v.schema, &v.desc, v.ontology.as_ref()?),
                Builtin::MaterializedView(mv) => {
                    (mv.name, mv.schema, &mv.desc, mv.ontology.as_ref()?)
                }
                Builtin::Source(s) => (s.name, s.schema, &s.desc, s.ontology.as_ref()?),
                _ => return None,
            };
            let entity_name = ontology.entity_name.to_string();
            Some(Info {
                table_name: name,
                schema_name: schema,
                entity_name,
                desc,
                ontology,
            })
        })
        .collect();

    vec![
        Builtin::View(leak(entity_types_view(&infos))),
        Builtin::View(leak(semantic_types_view())),
        Builtin::View(leak(properties_view(&infos))),
        Builtin::View(leak(link_types_view(&infos))),
    ]
}

/// Leak a `BuiltinView` to get a `&'static` reference. Called exactly 4 times
/// at startup (one per ontology view). These views live for the entire process
/// lifetime (same as `LazyLock<&'static BuiltinView>` used by other builtins),
/// so the leak is intentional and bounded.
fn leak(v: BuiltinView) -> &'static BuiltinView {
    Box::leak(Box::new(v))
}

struct Info<'a> {
    table_name: &'static str,
    schema_name: &'static str,
    entity_name: String,
    desc: &'a RelationDesc,
    ontology: &'a Ontology,
}

/// Escape single quotes for SQL string literals. Only safe for trusted
/// compile-time constants (entity names, descriptions, link JSON from
/// `Ontology` annotations) — never use with user-supplied input.
fn esc(s: &str) -> String {
    s.replace('\'', "''")
}

/// Build a simple ontology view from a name, OID, column defs, and SQL.
fn view(
    name: &'static str,
    o: u32,
    cols: &[(&'static str, SqlScalarType, bool)],
    keys: &[Vec<usize>],
    sql: String,
) -> BuiltinView {
    let mut b = RelationDesc::builder();
    for (n, ty, nullable) in cols {
        b = b.with_column(*n, ty.clone().nullable(*nullable));
    }
    let mut desc = b.finish();
    for key in keys {
        desc = desc.with_key(key.clone());
    }
    BuiltinView {
        name,
        schema: MZ_INTERNAL_SCHEMA,
        oid: o,
        desc,
        column_comments: BTreeMap::new(),
        sql: Box::leak(sql.into_boxed_str()),
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
}

/// Extract the first primary key from a `RelationDesc` and format it as a
/// JSON object, e.g. `{"primary_key": ["id", "schema_id"]}`. Returns `None`
/// if the relation has no keys defined.
fn pk_json(desc: &RelationDesc) -> Option<String> {
    let keys = desc.typ().keys.first()?;
    let cols: Vec<_> = keys
        .iter()
        .map(|&i| format!("\"{}\"", desc.get_name(i)))
        .collect();
    Some(format!("{{\"primary_key\": [{}]}}", cols.join(", ")))
}

// ── View builders ────────────────────────────────────────────

fn entity_types_view(infos: &[Info]) -> BuiltinView {
    let vals: Vec<_> = infos
        .iter()
        .map(|i| {
            let pk = pk_json(i.desc)
                .map_or_else(|| "NULL::jsonb".into(), |j| format!("'{}'::jsonb", esc(&j)));
            format!(
                "('{}','{}.{}',{},'{}')",
                esc(&i.entity_name),
                esc(i.schema_name),
                esc(i.table_name),
                pk,
                esc(i.ontology.description)
            )
        })
        .collect();
    view(
        "mz_ontology_entity_types",
        oid::VIEW_MZ_ONTOLOGY_ENTITY_TYPES_OID,
        &[
            ("name", SqlScalarType::String, false),
            ("relation", SqlScalarType::String, false),
            ("properties", SqlScalarType::Jsonb, true),
            ("description", SqlScalarType::String, false),
        ],
        &[vec![0], vec![1], vec![3]],
        format!(
            "SELECT name::text,relation::text,properties::jsonb,description::text FROM (VALUES {}) AS t(name,relation,properties,description)",
            vals.join(",")
        ),
    )
}

fn semantic_types_view() -> BuiltinView {
    let vals: Vec<_> = SEMANTIC_TYPE_DEFS
        .iter()
        .map(|(n, t, d)| format!("('{}','{}','{}')", esc(n), esc(t), esc(d)))
        .collect();
    view(
        "mz_ontology_semantic_types",
        oid::VIEW_MZ_ONTOLOGY_SEMANTIC_TYPES_OID,
        &[
            ("name", SqlScalarType::String, false),
            ("sql_type", SqlScalarType::String, false),
            ("description", SqlScalarType::String, false),
        ],
        &[vec![0], vec![2]],
        format!(
            "SELECT name::text,sql_type::text,description::text FROM (VALUES {}) AS t(name,sql_type,description)",
            vals.join(",")
        ),
    )
}

/// Build the `mz_ontology_properties` view: one row per column of every
/// annotated builtin relation.
///
/// The generated SQL works in two halves:
///
/// 1. **Column discovery** — An inline VALUES list (`ent`) maps each entity to
///    its (schema, table) pair. This is joined through `mz_schemas` →
///    `mz_objects` → `mz_columns` so the view always reflects the live catalog
///    (column additions/removals are picked up automatically).
///
/// 2. **Annotation enrichment** — A second VALUES list (`ann`) carries the
///    semantic-type annotations from `RelationDesc` (e.g. "CatalogItemId").
///    Column descriptions come from `mz_comments`. Both are LEFT JOINed so
///    columns without annotations or comments still appear (with NULLs).
fn properties_view(infos: &[Info]) -> BuiltinView {
    let mut ent = Vec::new();
    let mut ann = Vec::new();
    for i in infos {
        ent.push(format!(
            "('{}','{}','{}')",
            esc(i.schema_name),
            esc(i.table_name),
            esc(&i.entity_name)
        ));
        for (idx, col) in i.desc.iter_names().enumerate() {
            if let Some(sem) = i.desc.get_semantic_type(idx) {
                ann.push(format!(
                    "('{}','{}','{}')",
                    esc(&i.entity_name),
                    esc(col.as_str()),
                    sem
                ));
            }
        }
    }
    view(
        "mz_ontology_properties",
        oid::VIEW_MZ_ONTOLOGY_PROPERTIES_OID,
        &[
            ("entity_type", SqlScalarType::String, false),
            ("column_name", SqlScalarType::String, false),
            ("semantic_type", SqlScalarType::String, true),
            ("description", SqlScalarType::String, true),
        ],
        &[],
        format!(
            "SELECT ent.entity_name AS entity_type,col.name AS column_name,\
         ann.semantic_type::text AS semantic_type,cmt.comment AS description \
         FROM (VALUES {ent}) AS ent(schema_name,table_name,entity_name) \
         JOIN mz_catalog.mz_schemas s ON s.name=ent.schema_name \
         JOIN mz_catalog.mz_objects o ON o.schema_id=s.id AND o.name=ent.table_name \
         JOIN mz_catalog.mz_columns col ON col.id=o.id \
         LEFT JOIN mz_internal.mz_comments cmt ON cmt.id=o.id AND cmt.object_sub_id=col.position \
         LEFT JOIN (VALUES {ann}) AS ann(entity_name,column_name,semantic_type) \
         ON ann.entity_name=ent.entity_name AND ann.column_name=col.name",
            ent = ent.join(","),
            ann = ann.join(","),
        ),
    )
}

fn link_types_view(infos: &[Info]) -> BuiltinView {
    let vals: Vec<_> = infos
        .iter()
        .flat_map(|i| {
            i.ontology.links.iter().map(move |l| {
                format!(
                    "('{}','{}','{}','{}'::jsonb,NULL::text)",
                    esc(l.name),
                    esc(&i.entity_name),
                    esc(l.target),
                    esc(l.properties_json),
                )
            })
        })
        .collect();
    view(
        "mz_ontology_link_types",
        oid::VIEW_MZ_ONTOLOGY_LINK_TYPES_OID,
        &[
            ("name", SqlScalarType::String, false),
            ("source_entity", SqlScalarType::String, false),
            ("target_entity", SqlScalarType::String, false),
            ("properties", SqlScalarType::Jsonb, false),
            ("description", SqlScalarType::String, true),
        ],
        &[],
        format!(
            "SELECT name::text,source_entity::text,target_entity::text,properties::jsonb,description::text FROM (VALUES {}) AS t(name,source_entity,target_entity,properties,description)",
            vals.join(",")
        ),
    )
}

// ── Semantic type reference data ─────────────────────────────

const SEMANTIC_TYPE_DEFS: &[(&str, &str, &str)] = &[
    (
        "CatalogItemId",
        "text",
        "SQL-layer object ID. Format: s{n}/u{n}.",
    ),
    (
        "GlobalId",
        "text",
        "Runtime ID used by compute/storage. Format: s{n}/u{n}/si{n}.",
    ),
    ("ClusterId", "text", "Cluster ID. Format: s{n}/u{n}."),
    (
        "ReplicaId",
        "text",
        "Cluster replica ID. Format: s{n}/u{n}.",
    ),
    ("SchemaId", "text", "Schema ID. Format: s{n}/u{n}."),
    ("DatabaseId", "text", "Database ID. Format: s{n}/u{n}."),
    ("RoleId", "text", "Role ID. Format: s{n}/g{n}/u{n}/p."),
    (
        "NetworkPolicyId",
        "text",
        "Network policy ID. Format: s{n}/u{n}.",
    ),
    ("ShardId", "text", "Persist shard ID. Format: s{uuid}."),
    ("OID", "oid", "PostgreSQL-compatible object identifier."),
    ("ObjectType", "text", "Catalog object type discriminator."),
    ("ConnectionType", "text", "Connection type discriminator."),
    ("SourceType", "text", "Source type discriminator."),
    (
        "MzTimestamp",
        "mz_timestamp",
        "Internal logical timestamp (uint64).",
    ),
    (
        "WallclockTimestamp",
        "timestamp with time zone",
        "Wall clock timestamp.",
    ),
    ("ByteCount", "uint8", "A count of bytes."),
    ("RecordCount", "uint8", "A count of records/rows."),
    ("CreditRate", "numeric", "Credits consumed per hour."),
    ("SqlDefinition", "text", "A SQL CREATE statement."),
    (
        "RedactedSqlDefinition",
        "text",
        "A redacted SQL CREATE statement.",
    ),
];
