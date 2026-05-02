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
use mz_repr::{RelationDesc, SemanticType, SqlScalarType};
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

/// A single typed SQL literal for use inside a VALUES list.
enum Lit {
    /// A text string: rendered as `'escaped'`.
    Str(String),
    /// A JSONB value: rendered as `'escaped'::jsonb`.
    Json(String),
    /// SQL NULL.
    Null,
}

impl Lit {
    fn render(&self) -> String {
        match self {
            Lit::Str(s) => format!("'{}'", esc(s)),
            Lit::Json(s) => format!("'{}'::jsonb", esc(s)),
            Lit::Null => "NULL".to_string(),
        }
    }
}

/// Map a `SqlScalarType` to the SQL type name used in cast expressions.
fn sql_type_name(ty: &SqlScalarType) -> &'static str {
    match ty {
        SqlScalarType::String => "text",
        SqlScalarType::Jsonb => "jsonb",
        SqlScalarType::Oid => "oid",
        SqlScalarType::UInt64 => "uint8",
        SqlScalarType::Numeric { .. } => "numeric",
        SqlScalarType::MzTimestamp => "mz_timestamp",
        SqlScalarType::TimestampTz { .. } => "timestamp with time zone",
        other => panic!("unsupported SqlScalarType in ontology view: {other:?}"),
    }
}

/// Escape single quotes for SQL string literals. Only safe for trusted
/// compile-time constants (entity names, descriptions, link JSON from
/// `Ontology` annotations) — never use with user-supplied input.
fn esc(s: &str) -> String {
    s.replace('\'', "''")
}

/// Render rows into a SQL `VALUES (r1c1,r1c2,...),(r2c1,...)` fragment.
/// Used when a VALUES list appears as a subquery inside a larger SQL string
/// rather than as the top-level source of a `values_view`.
fn values_sql(rows: &[Vec<Lit>]) -> String {
    rows.iter()
        .map(|row| {
            let lits: Vec<String> = row.iter().map(Lit::render).collect();
            format!("({})", lits.join(","))
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Build an ontology view from a static VALUES list. Each row is a `Vec<Lit>`;
/// all escaping and type-casting is handled here so callers never touch SQL
/// string formatting directly.
fn values_view(
    name: &'static str,
    oid: u32,
    cols: &[(&'static str, SqlScalarType, bool)],
    keys: &[Vec<usize>],
    rows: Vec<Vec<Lit>>,
) -> BuiltinView {
    let col_names: Vec<&str> = cols.iter().map(|(n, _, _)| *n).collect();
    let cast_exprs: Vec<String> = cols
        .iter()
        .map(|(n, ty, _)| format!("{n}::{}", sql_type_name(ty)))
        .collect();

    let vals: Vec<String> = rows
        .iter()
        .map(|row| {
            let lits: Vec<String> = row.iter().map(Lit::render).collect();
            format!("({})", lits.join(","))
        })
        .collect();

    let sql = format!(
        "SELECT {casts} FROM (VALUES {vals}) AS t({cols})",
        casts = cast_exprs.join(","),
        vals = vals.join(","),
        cols = col_names.join(","),
    );

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
        oid,
        desc,
        column_comments: BTreeMap::new(),
        sql: Box::leak(sql.into_boxed_str()),
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
}

/// Extract all keys from a `RelationDesc` and return a `Lit::Json` with shape:
/// `{"primary_key": ["id"], "alternate_keys": [["oid"]]}`.
/// `primary_key` is the first declared key; `alternate_keys` contains any
/// additional unique keys. Returns `Lit::Null` if no keys are defined.
fn pk_lit(desc: &RelationDesc) -> Lit {
    let all_keys = &desc.typ().keys;
    let Some((first, rest)) = all_keys.split_first() else {
        return Lit::Null;
    };
    let fmt_key = |key: &Vec<usize>| -> String {
        let cols: Vec<_> = key
            .iter()
            .map(|&i| serde_json::to_string(desc.get_name(i).as_str()).expect("valid utf-8"))
            .collect();
        format!("[{}]", cols.join(", "))
    };
    let primary = fmt_key(first);
    let json = if rest.is_empty() {
        format!("{{\"primary_key\": {primary}}}")
    } else {
        let alts: Vec<_> = rest.iter().map(fmt_key).collect();
        format!(
            "{{\"primary_key\": {primary}, \"alternate_keys\": [{}]}}",
            alts.join(", ")
        )
    };
    Lit::Json(json)
}

// ── View builders ────────────────────────────────────────────

fn entity_types_view(infos: &[Info]) -> BuiltinView {
    let rows = infos
        .iter()
        .map(|i| {
            vec![
                Lit::Str(i.entity_name.clone()),
                Lit::Str(format!("{}.{}", i.schema_name, i.table_name)),
                pk_lit(i.desc),
                Lit::Str(i.ontology.description.to_string()),
            ]
        })
        .collect();
    values_view(
        "mz_ontology_entity_types",
        oid::VIEW_MZ_ONTOLOGY_ENTITY_TYPES_OID,
        &[
            ("name", SqlScalarType::String, false),
            ("relation", SqlScalarType::String, false),
            ("properties", SqlScalarType::Jsonb, true),
            ("description", SqlScalarType::String, false),
        ],
        &[vec![0], vec![1], vec![3]],
        rows,
    )
}

fn semantic_types_view() -> BuiltinView {
    let rows = SEMANTIC_TYPE_DEFS
        .iter()
        .map(|(n, t, d)| {
            vec![
                Lit::Str(n.to_string()),
                Lit::Str(t.to_string()),
                Lit::Str(d.to_string()),
            ]
        })
        .collect();
    values_view(
        "mz_ontology_semantic_types",
        oid::VIEW_MZ_ONTOLOGY_SEMANTIC_TYPES_OID,
        &[
            ("name", SqlScalarType::String, false),
            ("sql_type", SqlScalarType::String, false),
            ("description", SqlScalarType::String, false),
        ],
        &[vec![0], vec![2]],
        rows,
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
///    semantic-type annotations from `Ontology::column_semantic_types`.
///    Column descriptions come from `mz_comments`. Both are LEFT JOINed so
///    columns without annotations or comments still appear (with NULLs).
fn properties_view(infos: &[Info]) -> BuiltinView {
    let mut ent: Vec<Vec<Lit>> = Vec::new();
    let mut ann: Vec<Vec<Lit>> = Vec::new();
    for i in infos {
        ent.push(vec![
            Lit::Str(i.schema_name.to_string()),
            Lit::Str(i.table_name.to_string()),
            Lit::Str(i.entity_name.clone()),
        ]);
        for (col_name, sem) in i.ontology.column_semantic_types {
            ann.push(vec![
                Lit::Str(i.entity_name.clone()),
                Lit::Str(col_name.to_string()),
                Lit::Str(sem.to_string()),
            ]);
        }
    }
    let sql = format!(
        "SELECT ent.entity_name AS entity_type,col.name AS column_name,\
         ann.semantic_type::text AS semantic_type,cmt.comment AS description \
         FROM (VALUES {ent}) AS ent(schema_name,table_name,entity_name) \
         JOIN mz_catalog.mz_schemas s ON s.name=ent.schema_name \
         JOIN mz_catalog.mz_objects o ON o.schema_id=s.id AND o.name=ent.table_name \
         JOIN mz_catalog.mz_columns col ON col.id=o.id \
         LEFT JOIN mz_internal.mz_comments cmt ON cmt.id=o.id AND cmt.object_sub_id=col.position \
         LEFT JOIN (VALUES {ann}) AS ann(entity_name,column_name,semantic_type) \
         ON ann.entity_name=ent.entity_name AND ann.column_name=col.name",
        ent = values_sql(&ent),
        ann = values_sql(&ann),
    );

    let mut b = RelationDesc::builder();
    for (n, ty, nullable) in &[
        ("entity_type", SqlScalarType::String, false),
        ("column_name", SqlScalarType::String, false),
        ("semantic_type", SqlScalarType::String, true),
        ("description", SqlScalarType::String, true),
    ] {
        b = b.with_column(*n, ty.clone().nullable(*nullable));
    }
    BuiltinView {
        name: "mz_ontology_properties",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_MZ_ONTOLOGY_PROPERTIES_OID,
        desc: b.finish(),
        column_comments: BTreeMap::new(),
        sql: Box::leak(sql.into_boxed_str()),
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
}

fn link_types_view(infos: &[Info]) -> BuiltinView {
    let rows = infos
        .iter()
        .flat_map(|i| {
            i.ontology.links.iter().map(move |l| {
                vec![
                    Lit::Str(l.name.to_string()),
                    Lit::Str(i.entity_name.clone()),
                    Lit::Str(l.target.to_string()),
                    Lit::Json(
                        serde_json::to_string(&l.properties)
                            .expect("LinkProperties is serializable"),
                    ),
                    Lit::Null,
                ]
            })
        })
        .collect();
    values_view(
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
        rows,
    )
}

// ── Semantic type reference data ─────────────────────────────

pub(super) const SEMANTIC_TYPE_DEFS: &[(SemanticType, &str, &str)] = &[
    (
        SemanticType::CatalogItemId,
        "text",
        "SQL-layer object ID. Format: s{n}/u{n}.",
    ),
    (
        SemanticType::GlobalId,
        "text",
        "Runtime ID used by compute/storage. Format: s{n}/u{n}/si{n}.",
    ),
    (
        SemanticType::ClusterId,
        "text",
        "Cluster ID. Format: s{n}/u{n}.",
    ),
    (
        SemanticType::ReplicaId,
        "text",
        "Cluster replica ID. Format: s{n}/u{n}.",
    ),
    (
        SemanticType::SchemaId,
        "text",
        "Schema ID. Format: s{n}/u{n}.",
    ),
    (
        SemanticType::DatabaseId,
        "text",
        "Database ID. Format: s{n}/u{n}.",
    ),
    (
        SemanticType::RoleId,
        "text",
        "Role ID. Format: s{n}/g{n}/u{n}/p.",
    ),
    (
        SemanticType::NetworkPolicyId,
        "text",
        "Network policy ID. Format: s{n}/u{n}.",
    ),
    (
        SemanticType::ShardId,
        "text",
        "Persist shard ID. Format: s{uuid}.",
    ),
    (
        SemanticType::OID,
        "oid",
        "PostgreSQL-compatible object identifier.",
    ),
    (
        SemanticType::ObjectType,
        "text",
        "Catalog object type discriminator (e.g., table, view, source, sink, index, materialized-view).",
    ),
    (
        SemanticType::ConnectionType,
        "text",
        "Connection type discriminator (e.g., kafka, postgres, mysql, ssh-tunnel).",
    ),
    (
        SemanticType::SourceType,
        "text",
        "Source type discriminator (e.g., kafka, postgres, mysql, webhook).",
    ),
    (
        SemanticType::MzTimestamp,
        "mz_timestamp",
        "Internal logical timestamp (8-byte unsigned integer).",
    ),
    (
        SemanticType::WallclockTimestamp,
        "timestamp with time zone",
        "Wall clock timestamp.",
    ),
    (SemanticType::ByteCount, "uint8", "A count of bytes."),
    (
        SemanticType::RecordCount,
        "uint8",
        "A count of records/rows.",
    ),
    (
        SemanticType::CreditRate,
        "numeric",
        "Credits consumed per hour.",
    ),
    (
        SemanticType::SqlDefinition,
        "text",
        "A SQL CREATE statement.",
    ),
    (
        SemanticType::RedactedSqlDefinition,
        "text",
        "A redacted SQL CREATE statement.",
    ),
];
